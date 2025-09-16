using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;
using DynamicDbQueryApi.Entities.Query;
using DynamicDbQueryApi.Interfaces;

namespace DynamicDbQueryApi.Services
{
    public class QueryParserService : IQueryParserService
    {
        // Ana parse metodu
        public QueryModel Parse(string queryString)
        {
            if (!CheckIfParanthesesBalanced(queryString))
            {
                throw new Exception("Query string has unbalanced parentheses.");
            }

            var queryModel = new QueryModel();

            // FROM Part
            queryModel.Table = ParseFromTable(queryString);

            // FETCH Part
            queryModel.Columns = ParseFetchColumns(queryString, queryModel.Table);
            List<string> aliasList = queryModel.Columns.Where(c => !string.IsNullOrWhiteSpace(c.Alias)).Select(c => c.Alias!).ToList();

            // DISTINCT Part
            queryModel.Distinct = queryString.Contains("FETCHD", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCHDISTINCT", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCH DISTINCT", StringComparison.OrdinalIgnoreCase);

            // INCLUDE Part
            queryModel.Includes = ParseIncludes(queryString, queryModel.Table);

            // FILTER Part
            queryModel.Filters = ParseFilters(queryString, queryModel.Table, aliasList);

            // GROUP BY Part
            queryModel.GroupBy = ParseGroupBy(queryString, queryModel.Table, aliasList);

            // HAVING Part
            queryModel.Having = ParseHaving(queryString, queryModel.Table, aliasList);

            // ORDER BY Part
            queryModel.OrderBy = ParseOrderBy(queryString, queryModel.Table, aliasList);

            // LIMIT Part
            queryModel.Limit = ParseLimit(queryString);

            // OFFSET Part
            queryModel.Offset = ParseOffset(queryString);

            return queryModel;
        }

        // FROM kısmını parse etme
        private string ParseFromTable(string queryString)
        {
            // FROM(...) veya FROM ... ifadesinin içinden tablo ismini al
            var m = Regex.Match(queryString, @"\bFROM\s*\(?\s*(?<table>[\w\d\._\-]+)\s*\)?", RegexOptions.IgnoreCase);
            if (!m.Success)
            {
                throw new Exception("Could not find table name in FROM clause.");
            }

            var table = m.Groups["table"].Value.Trim();

            return table;
        }

        // FETCH(...) kısmını parse etme
        private List<QueryColumnModel> ParseFetchColumns(string queryString, string tableName)
        {
            // "FETCH(", "FETCHD(", "FETCHDISTINCT(" veya "FETCH DISTINCT(" ile başlayan ve ")" ile biten kısmı al
            var m = Regex.Match(queryString, @"\bFETCH(?:\s+DISTINCT|DISTINCT|D)?\s*\(", RegexOptions.IgnoreCase);
            if (!m.Success)
            {
                return new List<QueryColumnModel> { new QueryColumnModel { Expression = "*", Alias = null } };
            }
            var fetchStart = m.Index + m.Length;

            // FETCH ifadesinin kapanış parantezini bul
            int fetchEnd = FindClosingParenthesis(queryString, m.Index + m.Length - 1);
            if (fetchEnd == -1)
            {
                throw new Exception("Could not find closing parenthesis for FETCH clause.");
            }

            var columnsPart = queryString.Substring(fetchStart, fetchEnd - fetchStart);

            // Eğer FETCH() içi boşsa tüm sütunları al
            if (string.IsNullOrWhiteSpace(columnsPart))
            {
                return new List<QueryColumnModel> { new QueryColumnModel { Expression = "*", Alias = null } };
            }

            var columns = columnsPart.Split(',').Select(c => c.Trim()).ToList();

            List<QueryColumnModel> columnModels = new List<QueryColumnModel>();

            for (int i = 0; i < columns.Count; i++)
            {
                // Alias için " AS " ile ayır
                var parts = Regex.Split(columns[i], @"\s+AS\s+", RegexOptions.IgnoreCase);
                var expression = AddTablePrefixToColumn(parts[0].Trim(), tableName);
                string alias;
                if (parts.Length == 2)
                {
                    alias = parts[1].Trim();
                }
                else
                {
                    alias = expression.ToLowerInvariant().Replace('.', '_').Replace('(', '_').Replace(')', '_').Replace('*', 'a').Replace(',', '_').Replace(' ', '_').Replace('-', '_');

                }
                columnModels.Add(new QueryColumnModel
                {
                    Expression = expression,
                    Alias = alias
                });
            }
            return columnModels;
        }

        // INCLUDE(...) kısmını parse etme
        private List<IncludeModel> ParseIncludes(string queryString, string table)
        {
            var includes = new List<IncludeModel>();
            var m = Regex.Match(queryString, @"\bINCLUDE\s*\(\s*(.*?)\s*\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success) return includes;

            var body = m.Groups[1].Value;

            var includeTables = body.Split(',').Select(c => c.Trim()).Where(c => !string.IsNullOrWhiteSpace(c)).ToList();

            foreach (var include in includeTables)
            {
                // Join type için boşlukla ayır
                var partsWithJoin = include.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var includeTable = partsWithJoin[0];
                var joinType = "LEFT";

                // Eğer bir join tipi belirtilmişse al
                if (partsWithJoin.Length > 1)
                {
                    joinType = partsWithJoin[1].ToUpperInvariant();
                }

                // Eğer birden fazla tablo varsa hepsini ayır (orders.items.details)
                var parts = includeTable.Split('.');

                includes.Add(new IncludeModel
                {
                    Table = table,
                    TableKey = "Key", // Varsayılan anahtar
                    IncludeTable = parts[0],
                    IncludeKey = "Key", // Varsayılan yabancı anahtar
                    JoinType = joinType
                });

                // Ara tabloları ilişkilendir
                if (parts.Length > 1)
                {
                    for (int i = 1; i < parts.Length; i++)
                    {
                        var parentTable = parts[i - 1];
                        var childTable = parts[i];

                        includes.Add(new IncludeModel
                        {
                            Table = parentTable,
                            TableKey = "Key", // Varsayılan anahtar
                            IncludeTable = childTable,
                            IncludeKey = "Key", // Varsayılan yabancı anahtar
                            JoinType = joinType
                        });
                    }
                }
            }

            return includes;
        }

        // FILTER(...) kısmını parse etme
        private FilterModel? ParseFilters(string queryString, string tableName, List<string> aliasList)
        {
            var start = Regex.Match(queryString, @"\bFILTER\s*\(", RegexOptions.IgnoreCase);
            if (!start.Success) return null;

            // Parantez başlangıç indeksini ve kapanış indeksini bul
            int openIdx = start.Index + start.Length - 1;
            int bodyStart = openIdx + 1;
            int closeIdx = FindClosingParenthesis(queryString, openIdx);
            if (closeIdx == -1) return null;

            // FILTER(...) içindeki metni al
            var body = queryString.Substring(bodyStart, closeIdx - bodyStart).Trim();
            if (string.IsNullOrWhiteSpace(body)) return null;

            // FILTER ifadesini FilterModel yapısına dönüştür
            var filterModel = BuildFilterModel(body, tableName, aliasList);


            return filterModel;
        }

        // GROUPBY(...) kısmını parse etme
        private List<string> ParseGroupBy(string queryString, string tableName, List<string> aliasList)
        {
            var m = Regex.Match(queryString, @"\bGROUPBY\s*\(\s*(.*?)\s*\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success) return new List<string>();

            var body = m.Groups[1].Value;

            var groupByColumns = body.Split(',').Select(c => c.Trim()).Where(c => !string.IsNullOrWhiteSpace(c)).ToList();

            // Her column için tablo ismi ekle
            for (int i = 0; i < groupByColumns.Count; i++)
            {
                groupByColumns[i] = AddTablePrefixToColumn(groupByColumns[i], tableName, aliasList);
            }

            return groupByColumns;
        }

        // HAVING(...) kısmını parse etme
        private FilterModel? ParseHaving(string queryString, string tableName, List<string> aliasList)
        {
            var start = Regex.Match(queryString, @"\bHAVING\s*\(", RegexOptions.IgnoreCase);
            if (!start.Success) return null;

            // Parantez başlangıç indeksini ve kapanış indeksini bul
            int openIdx = start.Index + start.Length - 1;
            int bodyStart = openIdx + 1;
            int closeIdx = FindClosingParenthesis(queryString, openIdx);
            if (closeIdx == -1) return null;

            // HAVING(...) içindeki metni al
            var body = queryString.Substring(bodyStart, closeIdx - bodyStart).Trim();
            if (string.IsNullOrWhiteSpace(body)) return null;

            // HAVING ifadesini FilterModel yapısına dönüştür
            var filterModel = BuildFilterModel(body, tableName, aliasList);

            return filterModel;
        }

        // ORDERBY(...) kısmını parse etme
        private List<OrderByModel> ParseOrderBy(string queryString, string tableName, List<string> aliasList)
        {
            var start = Regex.Match(queryString, @"\bORDERBY\s*\(", RegexOptions.IgnoreCase);
            if (!start.Success) return new List<OrderByModel>();

            // Parantez başlangıç indeksini ve kapanış indeksini bul
            int openIdx = start.Index + start.Length - 1;
            int bodyStart = openIdx + 1;
            int closeIdx = FindClosingParenthesis(queryString, openIdx);
            if (closeIdx == -1) return new List<OrderByModel>();

            var body = queryString.Substring(bodyStart, closeIdx - bodyStart).Trim();

            var orderByColumns = new List<OrderByModel>();
            var columns = body.Split(',').Where(c => !string.IsNullOrWhiteSpace(c)).Select(c => c.Trim()).ToList();

            // Her column için tablo ismi ekle ve DESC kontrolü yap
            foreach (var column in columns)
            {
                if (!string.IsNullOrWhiteSpace(column))
                {
                    var parts = column.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 0)
                    {
                        var columnWithPrefix = AddTablePrefixToColumn(parts[0], tableName, aliasList);
                        var orderByModel = new OrderByModel
                        {
                            Column = columnWithPrefix,
                            Desc = parts.Length > 1 && parts[1].Equals("DESC", StringComparison.OrdinalIgnoreCase)
                        };
                        orderByColumns.Add(orderByModel);
                    }
                }
            }

            return orderByColumns;
        }

        // TAKE(int) veya TAKE int veya LIMIT(int) veya LIMIT int kısmını parse etme
        private int ParseLimit(string queryString)
        {
            var m = Regex.Match(queryString, @"\bTAKE\s*\(\s*(\d+)\s*\)", RegexOptions.IgnoreCase);
            if (!m.Success)
            {
                m = Regex.Match(queryString, @"\bLIMIT\s*\(\s*(\d+)\s*\)", RegexOptions.IgnoreCase);
                if (!m.Success) return 0;
            }

            return int.TryParse(m.Groups[1].Value, out var limit) ? limit : 0;
        }

        // SKIP(int) veya SKIP int veya OFFSET(int) veya OFFSET int kısmını parse etme
        private int ParseOffset(string queryString)
        {
            var m = Regex.Match(queryString, @"\bSKIP\s*\(\s*(\d+)\s*\)", RegexOptions.IgnoreCase);
            if (!m.Success)
            {
                m = Regex.Match(queryString, @"\bOFFSET\s*\(\s*(\d+)\s*\)", RegexOptions.IgnoreCase);
                if (!m.Success) return 0;
            }

            return int.TryParse(m.Groups[1].Value, out var offset) ? offset : 0;
        }

        // Parantezlerin dengeli olup olmadığını kontrol etme
        public bool CheckIfParanthesesBalanced(string input)
        {
            int balance = 0;
            foreach (char c in input)
            {
                if (c == '(') balance++;
                else if (c == ')') balance--;

                // Eğer kapanış parantezi açılış parantezinden önce gelirse
                if (balance < 0) return false;
            }
            return balance == 0;
        }

        private string ParseFunction(string functionString, string inner, string tableName, List<string>? aliasList = null)
        {

            // Aggregate fonksiyonlarını yakala (COUNT, SUM, AVG, MIN, MAX)
            var aggregateFuncs = new[] { "COUNT", "SUM", "AVG", "MIN", "MAX" };
            if (aggregateFuncs.Contains(functionString))
            {
                var innerParts = inner.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(p => p.Trim()).ToList();

                if (innerParts.Count == 1)
                {
                    // eğer sayı ise tablo ismi ekleme
                    if (int.TryParse(innerParts[0], out _))
                    {
                        return $"{functionString}({innerParts[0]})";
                    }
                    var col = AddTablePrefixToColumn(inner, tableName, aliasList);
                    return $"{functionString}({col})";
                }
                else
                {
                    throw new Exception($"Invalid usage of aggregate function {functionString} with no parameters.");
                }
            }

            // Sayısal fonksiyonları yakala (ABS, CEIL, FLOOR, ROUND, SQRT, POWER, MOD, EXP, LOG, LOG10)
            var numericFuncs = new[] { "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "SQRT", "POWER", "MOD", "EXP", "LOG", "LOG10" };

            // Tek parametreli numeric fonksiyonları kontrol et
            if (numericFuncs.Contains(functionString) && !inner.Contains(","))
            {
                // eğer sayı ise tablo ismi ekleme
                if (double.TryParse(inner, out _))
                {
                    return $"{functionString}({inner})";
                }
                var col = AddTablePrefixToColumn(inner, tableName, aliasList);
                return $"{functionString}({col})";
            }
            // İki parametreli numeric fonksiyonları kontrol et (POWER, MOD, ROUND, LOG)
            if (numericFuncs.Contains(functionString) && inner.Contains(","))
            {
                var innerParts = inner.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(p => p.Trim()).ToList();

                if (innerParts.Count == 2)
                {
                    var col1 = innerParts[0];
                    if (!double.TryParse(innerParts[0], out _))
                    {
                        col1 = AddTablePrefixToColumn(innerParts[0], tableName, aliasList);
                    }
                    var col2 = innerParts[1];
                    if (!double.TryParse(innerParts[1], out _))
                    {
                        col2 = AddTablePrefixToColumn(innerParts[1], tableName, aliasList);
                    }
                    return $"{functionString}({col1}, {col2})";
                }
                else
                {
                    throw new Exception($"Invalid usage of numeric function {functionString} with multiple parameters.");
                }
            }

            // Metin fonksiyonlarını yakala (LENGTH, LEN, SUBSTRING, SUBSTR, CONCAT, LOWER, UPPER, TRIM, LTRIM, RTRIM, INDEXOF, REPLACE, REVERSE)
            var stringFuncs = new[] { "LENGTH", "LEN", "SUBSTRING", "SUBSTR", "CONCAT", "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM", "INDEXOF", "REPLACE", "REVERSE" };
            if (stringFuncs.Contains(functionString))
            {
                var innerParts = inner.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(p => p.Trim()).ToList();

                if (functionString == "LENGTH" || functionString == "LEN")
                {
                    string function = "LENGTH";
                    if (innerParts.Count != 1)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with multiple parameters.");
                    }
                    // Eğer string ise tablo ismi ekleme
                    if (innerParts[0].StartsWith("'") && innerParts[0].EndsWith("'"))
                    {
                        return $"{function}({innerParts[0]})";
                    }
                    var col = AddTablePrefixToColumn(innerParts[0], tableName, aliasList);
                    return $"{function}({col})";
                }

                if (functionString == "SUBSTRING" || functionString == "SUBSTR")
                {
                    string function = "SUBSTRING";
                    if (innerParts.Count != 3)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with incorrect number of parameters.");
                    }
                    var col = innerParts[0];
                    if (!(col.StartsWith("'") && col.EndsWith("'")))
                    {
                        col = AddTablePrefixToColumn(col, tableName, aliasList);
                    }
                    // innerParts[1] ve innerParts[2] sayı olmalı
                    if (!int.TryParse(innerParts[1], out var start) || !int.TryParse(innerParts[2], out var length))
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with non-numeric start or length parameters.");
                    }
                    return $"{function}({col}, {start}, {length})";
                }

                if (functionString == "CONCAT")
                {
                    if (innerParts.Count < 2)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with less than two parameters.");
                    }
                    var cols = new List<string>();
                    foreach (var part in innerParts)
                    {
                        var p = part;
                        // eğer string ise tablo ismi ekleme
                        if (!((p.StartsWith("'") && p.EndsWith("'")) || (p.StartsWith("\"") && p.EndsWith("\""))))
                        {
                            p = AddTablePrefixToColumn(part, tableName, aliasList);
                        }
                        cols.Add(p);
                    }
                    return $"{functionString}({string.Join(", ", cols)})";
                }

                if (functionString == "UPPER" || functionString == "LOWER" || functionString == "LTRIM" || functionString == "RTRIM" || functionString == "TRIM" || functionString == "REVERSE")
                {
                    if (innerParts.Count != 1)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with multiple parameters.");
                    }
                    var col = innerParts[0];
                    // eğer string ise tablo ismi ekleme
                    if (!(col.StartsWith("'") && col.EndsWith("'")))
                    {
                        col = AddTablePrefixToColumn(col, tableName, aliasList);
                    }
                    return $"{functionString}({col})";
                }

                if (functionString == "INDEXOF")
                {
                    if (innerParts.Count != 2)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with incorrect number of parameters.");
                    }
                    var col = innerParts[0];
                    if (!(col.StartsWith("'") && col.EndsWith("'")))
                    {
                        col = AddTablePrefixToColumn(col, tableName, aliasList);
                    }
                    var search = innerParts[1];
                    // eğer string ise tablo ismi ekleme
                    if (!(search.StartsWith("'") && search.EndsWith("'")))
                    {
                        search = AddTablePrefixToColumn(search, tableName, aliasList);
                    }
                    return $"{functionString}({col}, {search})";
                }

                if (functionString == "REPLACE")
                {
                    if (innerParts.Count != 3)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with incorrect number of parameters.");
                    }
                    var col = innerParts[0];
                    if (!(col.StartsWith("'") && col.EndsWith("'")))
                    {
                        col = AddTablePrefixToColumn(col, tableName, aliasList);
                    }
                    var search = innerParts[1];
                    // eğer string ise tablo ismi ekleme
                    if (!(search.StartsWith("'") && search.EndsWith("'")))
                    {
                        search = AddTablePrefixToColumn(search, tableName, aliasList);
                    }
                    var replace = innerParts[2];
                    // eğer string ise tablo ismi ekleme
                    if (!(replace.StartsWith("'") && replace.EndsWith("'")))
                    {
                        replace = AddTablePrefixToColumn(replace, tableName, aliasList);
                    }
                    return $"{functionString}({col}, {search}, {replace})";
                }
            }

            // Null fonksiyonlarını yakala (COALESCE, IFNULL, ISNULL, NVL)
            var nullFuncs = new[] { "COALESCE", "IFNULL", "ISNULL", "NVL" };
            if (nullFuncs.Contains(functionString))
            {
                string function = "COALESCE";
                var innerParts = inner.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(p => p.Trim()).ToList();

                if (innerParts.Count >= 2)
                {
                    var cols = new List<string>();
                    foreach (var part in innerParts)
                    {
                        var p = part;
                        // eğer string ise tablo ismi ekleme
                        if (!(p.StartsWith("'") && p.EndsWith("'")))
                        {
                            p = AddTablePrefixToColumn(part, tableName, aliasList);
                        }
                        cols.Add(p);
                    }
                    return $"{function}({string.Join(", ", cols)})";
                }
                else
                {
                    throw new Exception($"Invalid usage of null function {functionString} with less than two parameters.");
                }
            }

            // Tarih fonksiyonlarını yakala (NOW, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, DATEADD, DATEDIFF, DATENAME, DATEPART, DAY, MONTH, YEAR)
            // var dateFuncs = new[] { "NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATEADD", "DATEDIFF", "DATENAME", "DATEPART", "DAY", "MONTH", "YEAR" };


            // Eğer tanımlanamayan bir fonksiyon ise hata fırlat
            throw new Exception($"Unknown function: {functionString}");

        }

        // Columnlara table ismi ekleme
        private string AddTablePrefixToColumn(string column, string tableName, List<string>? aliasList = null)
        {
            if (string.IsNullOrWhiteSpace(column)) return column;

            // Eğer tek tırnak içinde string ise tablo ismi ekleme
            if (column.StartsWith("'") && column.EndsWith("'"))
            {
                return column;
            }

            column = column.Trim();

            // COUNT(*) veya SUM(*) gibi durumlar için kontrol et
            if (column == "*") return column;

            // Regex: func ( ... ) şeklinde olanları yakala -> ...
            var pattern = $@"^(?<func>[A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(?<inner>.*?)\s*\)$";
            var funcMatch = Regex.Match(column, pattern, RegexOptions.IgnoreCase);

            if (funcMatch.Success)
            {
                var funcName = funcMatch.Groups["func"].Value.ToUpperInvariant();
                var inner = funcMatch.Groups["inner"].Value.Trim();

                // Fonksiyonu parse et
                return ParseFunction(funcName, inner, tableName, aliasList);
            }

            // Eğer aliasList verilmişse ve column bu listede varsa tablo ismi ekleme
            if (aliasList != null && aliasList.Contains(column, StringComparer.OrdinalIgnoreCase))
            {
                return column;
            }

            var parts = column.Split('.');
            // Eğer tablo ismi yoksa ekle
            if (parts.Length == 1)
            {
                return $"{tableName}.{parts[0]}";
            }
            else if (parts.Length > 1)
            {
                return $"{parts[parts.Length - 2]}.{parts[parts.Length - 1]}";
            }
            return column;
        }

        // En dıştaki parantezin kapanışını bulma
        private int FindClosingParenthesis(string str, int openIdx)
        {
            int depth = 0;
            for (int i = openIdx; i < str.Length; i++)
            {
                if (str[i] == '(')
                {
                    depth++;
                }
                else if (str[i] == ')')
                {
                    depth--;
                    if (depth == 0) return i;
                }
            }
            return -1; // Not found
        }

        // En dıştaki parantez içi operandları çıkarma ve yerlerine $0, $1, ... koyma
        public static List<string> ExtractTopLevelOperands(ref string body, out string replacedBody)
        {
            var expressions = new List<string>();
            var sb = new StringBuilder();
            int depth = 0;
            int start = -1;
            int counter = 0;

            for (int i = 0; i < body.Length; i++)
            {
                char c = body[i];

                if (c == '(')
                {
                    if (depth == 0)
                    {
                        // Parantez başlangıcı
                        start = i + 1;
                    }
                    depth++;
                }
                else if (c == ')')
                {
                    depth--;

                    if (depth == 0 && start >= 0)
                    {
                        string inner = body.Substring(start, i - start);
                        string placeholder = $"${counter++}";
                        expressions.Add(inner.Trim());
                        sb.Append(placeholder);
                        start = -1;
                    }
                }
                else
                {
                    if (depth == 0)
                    {
                        sb.Append(c);
                    }
                }
            }
            replacedBody = sb.ToString().Trim();
            return expressions;
        }

        // FILTER ifadesini FilterModel yapısına dönüştürme
        private FilterModel? BuildFilterModel(string body, string tableName, List<string> aliasList)
        {
            if (IsSingleConditionFilter(body))
            {
                var cond = ParseConditionFilter(body, tableName, aliasList);
                return cond;
            }

            // ExpressionSplitter kullanarak en dıştaki operandları çıkar ve yerlerine $0, $1, ... koy
            var exprList = ExpressionSplitter2.SimplerExtractTopLevelOperands(ref body, out var replacedBody);

            var finalExprList = exprList;
            var finalReplacedBody = replacedBody;

            // Eğer exprList 2'den büyükse, replacedBody'yi operatör önceliğine göre parantezleyip tekrar parçala
            if (exprList.Count > 2)
            {
                // Operatör önceliğine göre parantezleme
                var str = ParenthesizeByPrecedence(finalReplacedBody);

                // Baştaki ve sondaki parantezleri kaldır
                if (str.StartsWith("(") && str.EndsWith(")"))
                {
                    str = str.Substring(1, str.Length - 2).Trim();
                }

                // finalReplacedBody deki $ ile başlayan yerler exprListteki karşılığı ile değiştir
                for (int i = 0; i < finalExprList.Count; i++)
                {
                    var repl = finalExprList[i].Trim();
                    if (!IsSingleConditionFilter(repl))
                    {
                        // Eğer tekil condition filter değilse işlem önceliğini kaybetmemek için parantez ekle
                        if (!(repl.StartsWith("(") && repl.EndsWith(")")))
                            repl = "(" + repl + ")";
                    }
                    str = str.Replace($"${i}", repl);
                }

                // ExpressionSplitter kullanarak yeniden en dıştaki operandları çıkar ve yerlerine $0, $1, ... koy
                var multiExprList = ExpressionSplitter2.SimplerExtractTopLevelOperands(ref str, out var multiReplacedBody);

                finalExprList = multiExprList;
                finalReplacedBody = multiReplacedBody;
            }

            // finalReplacedBody tek bir logical expression olmalı ($0 AND $1)
            var tokens = finalReplacedBody.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            // finalExprListteki $0, $1, ... yerlerini tekrar orijinal ifadeleri ile değiştir
            for (int i = 0; i < finalExprList.Count; i++)
            {
                tokens = tokens.Select(t => t == $"${i}" ? finalExprList[i] : t).ToArray();
            }

            var filter = new LogicalFilterModel();

            // Token uzunluğu 3 olmalı: [expr1, AND/OR, expr2]
            if (tokens.Length == 3 && (tokens[1].Equals("AND", StringComparison.OrdinalIgnoreCase) || tokens[1].Equals("OR", StringComparison.OrdinalIgnoreCase)))
            {
                filter.Operator = tokens[1].Equals("AND", StringComparison.OrdinalIgnoreCase) ? LogicalOperator.And : LogicalOperator.Or;

                // Recursive olarak sol ve sağ ifadeleri işle
                filter.Left = BuildFilterModel(tokens[0], tableName, aliasList)!;
                filter.Right = BuildFilterModel(tokens[2], tableName, aliasList)!;

                return filter;
            }
            else
            {
                throw new Exception($"Could not parse logical filter expression: {body} - Tokens: {JsonSerializer.Serialize(tokens)}");
            }
        }

        // Tek bir condition filter ifadesi mi kontrolü
        private bool IsSingleConditionFilter(string expr)
        {
            var tokens = expr.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            foreach (var token in tokens)
            {
                if (token.Equals("AND", StringComparison.OrdinalIgnoreCase) ||
                    token.Equals("OR", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }
            return true;
        }

        // Operatör önceliğine göre parantezleme (AND > OR)
        public static string ParenthesizeByPrecedence(string input)
        {
            var tokens = input.Split(' ', StringSplitOptions.RemoveEmptyEntries).ToList();

            // 1. Önce AND işlemleri
            for (int i = 0; i < tokens.Count; i++)
            {
                if (tokens[i].Equals("AND", StringComparison.OrdinalIgnoreCase))
                {
                    string left = tokens[i - 1];
                    string right = tokens[i + 1];

                    tokens[i - 1] = $"({left} AND {right})";
                    tokens.RemoveAt(i); // AND
                    tokens.RemoveAt(i); // right
                    i--; // index kaydır
                }
            }

            // 2. Kalan OR işlemleri
            while (tokens.Count > 1)
            {
                if (!tokens[1].Equals("OR", StringComparison.OrdinalIgnoreCase))
                {
                    throw new Exception("Unexpected operator sequence while processing OR operations.");
                }

                string left = tokens[0];
                string op = tokens[1]; // OR
                string right = tokens[2];

                string combined = $"({left} {op} {right})";
                tokens[0] = combined;
                tokens.RemoveAt(1);
                tokens.RemoveAt(1);
            }

            return tokens[0];
        }

        // Tekil condition filter ifadesini parse etme
        private ConditionFilterModel? ParseConditionFilter(string expr, string tableName, List<string> aliasList)
        {
            if (string.IsNullOrWhiteSpace(expr)) return null;

            var s = expr.Trim();

            // Genel operatorler (>=, <=, !=, <>, =, >, <, CONTAINS, BEGINSWITH, ENDSWITH, LIKE)
            var m = Regex.Match(s,
                @"^\s*(?<col>[\w\.\-\(\)\*\s,']+?)\s*(?<op>>=|<=|!=|<>|=|>|<|LIKE|CONTAINS|BEGINSWITH|ENDSWITH)\s*(?<rhs>.+?)\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            if (!m.Success)
            {
                throw new Exception($"Could not parse condition filter expression: {expr}");
            }

            var col = m.Groups["col"].Value;
            var op = m.Groups["op"].Value.ToUpperInvariant();
            var rhs = m.Groups["rhs"].Value.Trim();

            // string Unquote(string v)
            // {
            //     v = v.Trim();
            //     if (v.Length >= 2 && v[0] == '\'' && v[^1] == '\'')
            //         return v.Substring(1, v.Length - 2).Replace("''", "'");
            //     return v;
            // }

            // Value tek tırnaklıysa tırnakları kaldır
            // string rhsValue = rhsRaw;
            // if (rhsRaw.StartsWith("'") && rhsRaw.EndsWith("'"))
            // {
            //     rhsValue = Unquote(rhsRaw);
            // }

            // Operator mapping kısmı
            ComparisonOperator comp;
            switch (op)
            {
                case "=": comp = ComparisonOperator.Eq; break;
                case "!=":
                case "<>": comp = ComparisonOperator.Neq; break;
                case ">": comp = ComparisonOperator.Gt; break;
                case "<": comp = ComparisonOperator.Lt; break;
                case ">=": comp = ComparisonOperator.Gte; break;
                case "<=": comp = ComparisonOperator.Lte; break;
                case "CONTAINS": comp = ComparisonOperator.Contains; break;
                case "BEGINSWITH": comp = ComparisonOperator.BeginsWith; break;
                case "ENDSWITH": comp = ComparisonOperator.EndsWith; break;
                case "LIKE": comp = ComparisonOperator.Like; break;
                default: return null;
            }

            // Eğer comp != ve rhs == NULL ise IS NOT NULL yap
            if (comp == ComparisonOperator.Neq && rhs.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            {
                comp = ComparisonOperator.IsNotNull;
            }
            else if (comp == ComparisonOperator.Eq && rhs.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            {
                comp = ComparisonOperator.IsNull;
            }

            // Eğer col sayı veya string değil ise tablo ismi ekle
            if (!(double.TryParse(col, out _) || !(col.StartsWith("'") && col.EndsWith("'"))))
            {
                col = AddTablePrefixToColumn(col, tableName, aliasList);
            }

            // Eğer rhs sayı veya string değil ise tablo ismi ekle
            if (!(double.TryParse(rhs, out _) || !(rhs.StartsWith("'") && rhs.EndsWith("'"))))
            {
                rhs = AddTablePrefixToColumn(rhs, tableName, aliasList);
            }

            return new ConditionFilterModel
            {
                Column = col,
                Operator = comp,
                Value = rhs
            };
        }
    }
}