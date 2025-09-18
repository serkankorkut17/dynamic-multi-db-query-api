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

            List<string> columns = SplitByCommas(columnsPart);

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
                    IncludeKey = "Key", // Varsayılan include table anahtarı
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
                            IncludeKey = "Key", // Varsayılan include table anahtarı
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

            if (int.TryParse(m.Groups[1].Value, out var limit))
            {
                return limit;
            }
            else
            {
                throw new Exception("Could not parse LIMIT/TAKE value.");
            }
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

            if (int.TryParse(m.Groups[1].Value, out var offset))
            {
                return offset;
            }
            else
            {
                throw new Exception("Could not parse OFFSET/SKIP value.");
            }
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

        // String eğer fonksiyon ise fonksiyon adını ve içini çıkarma
        private (string function, string inner)? ExtractFunction(string s)
        {
            var start = Regex.Match(s, @"^(?<func>[A-Za-z_][A-Za-z0-9_]*)\s*\(", RegexOptions.IgnoreCase);
            if (start.Success)
            {
                // Parantez başlangıç indeksini ve kapanış indeksini bul
                int openIdx = start.Index + start.Length - 1;
                int bodyStart = openIdx + 1;
                int closeIdx = FindClosingParenthesis(s, openIdx);
                if (closeIdx == -1) throw new Exception("Could not find closing parenthesis for function in column.");

                var body = s.Substring(bodyStart, closeIdx - bodyStart).Trim();

                var funcName = start.Groups["func"].Value.ToUpperInvariant();
                // Fonksiyonu parse et
                return (funcName, body);
            }
            return null;
        }

        // Fonksiyonları parse etme
        private string ParseFunction(string functionString, string inner, string tableName, List<string>? aliasList = null)
        {
            // Eğer inner boşsa hata fırlat
            if (string.IsNullOrWhiteSpace(inner))
            {
                throw new Exception($"Function {functionString} requires parameters.");
            }

            // Fonksiyon içindeki parametreleri ayır
            var innerParams = SplitByCommas(inner);

            if (innerParams.Count != 0)
            {
                for (int i = 0; i < innerParams.Count; i++)
                {
                    innerParams[i] = AddTablePrefixToColumn(innerParams[i], tableName, aliasList);
                }
            }

            // Aggregate fonksiyonlarını yakala (COUNT, SUM, AVG, MIN, MAX)
            var aggregateFuncs = new[] { "COUNT", "SUM", "AVG", "MIN", "MAX" };
            if (aggregateFuncs.Contains(functionString))
            {
                if (innerParams.Count == 1)
                {
                    return $"{functionString}({innerParams[0]})";
                }
                else
                {
                    throw new Exception($"Invalid usage of aggregate function {functionString} with no parameters.");
                }
            }

            // Sayısal fonksiyonları yakala (ABS, CEIL, FLOOR, ROUND, SQRT, POWER, MOD, EXP, LOG, LOG10)
            var numericFuncs = new[] { "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "SQRT", "POWER", "MOD", "EXP", "LOG", "LN", "LOG10" };
            // LOG(x, base), LOG(x))

            // Tek parametreli numeric fonksiyonları kontrol et
            if (numericFuncs.Contains(functionString))
            {
                if (innerParams.Count == 1)
                {
                    return $"{functionString}({innerParams[0]})";
                }
                else if (innerParams.Count == 2)
                {
                    return $"{functionString}({innerParams[0]}, {innerParams[1]})";
                }
                else
                {
                    throw new Exception($"Invalid usage of numeric function {functionString} with incorrect number of parameters.");
                }
            }


            // Metin fonksiyonlarını yakala (LENGTH, LEN, SUBSTRING, SUBSTR, CONCAT, LOWER, UPPER, TRIM, LTRIM, RTRIM, INDEXOF, REPLACE, REVERSE)
            var stringFuncs = new[] { "LENGTH", "LEN", "SUBSTRING", "SUBSTR", "CONCAT", "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM", "INDEXOF", "REPLACE", "REVERSE" };
            if (stringFuncs.Contains(functionString))
            {
                if (functionString == "LENGTH" || functionString == "LEN")
                {
                    string function = "LENGTH";
                    if (innerParams.Count != 1)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with multiple parameters.");
                    }
                    return $"{function}({innerParams[0]})";
                }

                // SUBSTRING(string, start, length) veya SUBSTR(string, start)
                if (functionString == "SUBSTRING" || functionString == "SUBSTR")
                {
                    string function = "SUBSTRING";
                    if (innerParams.Count != 2 && innerParams.Count != 3)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with incorrect number of parameters.");
                    }
                    var col = innerParams[0];
                    if (innerParams.Count == 2)
                    {
                        // SUBSTRING(string, start)
                        if (!int.TryParse(innerParams[1], out var start))
                        {
                            throw new Exception($"Invalid usage of string function {functionString} with non-numeric start parameter.");
                        }
                        return $"{function}({col}, {start})";
                    }
                    else if (innerParams.Count == 3)
                    {
                        // SUBSTRING(string, start, length)
                        if (!int.TryParse(innerParams[1], out var start) || !int.TryParse(innerParams[2], out var length))
                        {
                            throw new Exception($"Invalid usage of string function {functionString} with non-numeric start or length parameters.");
                        }
                        return $"{function}({col}, {start}, {length})";
                    }
                }

                if (functionString == "CONCAT")
                {
                    if (innerParams.Count < 2)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with less than two parameters.");
                    }
                    var cols = new List<string>();
                    foreach (var part in innerParams)
                    {
                        var p = part;
                        cols.Add(p);
                    }
                    return $"{functionString}({string.Join(", ", cols)})";
                }

                if (functionString == "UPPER" || functionString == "LOWER" || functionString == "LTRIM" || functionString == "RTRIM" || functionString == "TRIM" || functionString == "REVERSE")
                {
                    if (innerParams.Count != 1)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with multiple parameters.");
                    }
                    var col = innerParams[0];
                    return $"{functionString}({col})";
                }

                // INDEXOF(string, search) veya INDEXOF(string, search, startIndex)
                if (functionString == "INDEXOF")
                {
                    if (innerParams.Count != 2)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with incorrect number of parameters.");
                    }
                    var col = innerParams[0];
                    var search = innerParams[1];
                    return $"{functionString}({col}, {search})";
                }

                if (functionString == "REPLACE")
                {
                    if (innerParams.Count != 3)
                    {
                        throw new Exception($"Invalid usage of string function {functionString} with incorrect number of parameters.");
                    }
                    var col = innerParams[0];
                    var search = innerParams[1];
                    var replace = innerParams[2];

                    return $"{functionString}({col}, {search}, {replace})";
                }
            }

            // Null fonksiyonlarını yakala (COALESCE, IFNULL, ISNULL, NVL)
            var nullFuncs = new[] { "COALESCE", "IFNULL", "ISNULL", "NVL" };
            if (nullFuncs.Contains(functionString))
            {
                string function = "COALESCE";

                if (innerParams.Count >= 2)
                {
                    var cols = new List<string>();
                    foreach (var part in innerParams)
                    {
                        var p = part;
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
            var dateFuncs = new[] { "NOW", "GETDATE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "DATEADD", "DATEDIFF", "DATENAME", "TO_CHAR", "DAY", "MONTH", "YEAR" };
            if (dateFuncs.Contains(functionString))
            {
                if (functionString == "NOW" || functionString == "GETDATE" || functionString == "CURRENT_TIMESTAMP" || functionString == "CURRENT_DATE" || functionString == "CURRENT_TIME")
                {
                    if (innerParams.Count != 0)
                    {
                        throw new Exception($"Invalid usage of date function {functionString} with parameters.");
                    }
                    return $"{functionString}()";
                }

                if (functionString == "DAY" || functionString == "MONTH" || functionString == "YEAR")
                {
                    if (innerParams.Count != 1)
                    {
                        throw new Exception($"Invalid usage of date function {functionString} with incorrect number of parameters.");
                    }
                    return $"{functionString}({innerParams[0]})";
                }

                if (functionString == "DATEADD" || functionString == "DATEDIFF")
                {
                    if (innerParams.Count != 3)
                    {
                        throw new Exception($"Invalid usage of date function {functionString} with incorrect number of parameters.");
                    }
                    var col = innerParams[0];
                    var interval = innerParams[1];
                    var number = innerParams[2];

                    return $"{functionString}({col}, {interval}, {number})";
                }

                if (functionString == "DATENAME" || functionString == "TO_CHAR")
                {
                    if (innerParams.Count != 2)
                    {
                        throw new Exception($"Invalid usage of date function {functionString} with incorrect number of parameters.");
                    }
                    var part = innerParams[0];
                    var col = innerParams[1];

                    return $"{functionString}({part}, {col})";
                }
            }



            // Eğer tanımlanamayan bir fonksiyon ise hata fırlat
            throw new Exception($"Unknown function: {functionString}");

        }

        // Columnlara table ismi ekleme
        private string AddTablePrefixToColumn(string column, string tableName, List<string>? aliasList = null)
        {
            if (string.IsNullOrWhiteSpace(column)) return column;

            // Eğer sayı ise tablo ismi ekleme
            if (int.TryParse(column, out _))
            {
                return column;
            }

            // Eğer tek tırnak içinde string ise tablo ismi ekleme
            if (column.StartsWith("'") && column.EndsWith("'"))
            {
                return column;
            }

            column = column.Trim();

            // COUNT(*) veya SUM(*) gibi durumlar için kontrol et
            if (column == "*") return column;

            // Eğer fonksiyon ise fonksiyon adını ve içini çıkar ve parse et
            var func = ExtractFunction(column);
            if (func != null)
            {
                var (funcName, inner) = func.Value;
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

        // Tek tırnaklı string sonunu bulma
        private int FindClosingQuote(string str, int startIdx)
        {
            for (int i = startIdx + 1; i < str.Length; i++)
            {
                if (str[i] == '\'')
                {
                    // Eğer öncesinde \ yoksa kapatma tırnağıdır
                    if (i == 0 || str[i - 1] != '\\')
                    {
                        return i;
                    }
                }
            }
            return -1; // Not found
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

            // Genel operatorler (>=, <=, !=, <>, ==, =, >, <, CONTAINS, BEGINSWITH, ENDSWITH, LIKE)
            var m = Regex.Match(s,
                @"^\s*(?<col>[\w\.\-\(\)\*\s,']+?)\s*(?<op>>=|<=|!=|<>|==|=|>|<|LIKE|CONTAINS|BEGINSWITH|ENDSWITH)\s*(?<rhs>.+?)\s*$",
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
                case "==": comp = ComparisonOperator.Eq; break;
                case "=": comp = ComparisonOperator.Eq; break;
                case "!=": comp = ComparisonOperator.Neq; break;
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

        // , lerden bölme (parantez içi ve string içi değilse)
        private List<string> SplitByCommas(string input)
        {
            List<string> result = new List<string>();
            for (int i = 0; i < input.Length; i++)
            {
                if (input[i] == '(')
                {
                    // Parantez içindeki virgülleri yok say
                    int closeIdx = FindClosingParenthesis(input, i);
                    if (closeIdx == -1)
                    {
                        throw new Exception("Could not find closing parenthesis in FETCH columns.");
                    }
                    i = closeIdx;
                }

                else if (input[i] == '\'')
                {
                    // String içindeki virgülleri yok say
                    int closeIdx = FindClosingQuote(input, i);
                    if (closeIdx == -1)
                    {
                        throw new Exception("Could not find closing quote in FETCH columns.");
                    }
                    i = closeIdx;
                }

                else if (input[i] == ',')
                {
                    // Virgül bulundu, böl
                    result.Add(input.Substring(0, i).Trim());
                    input = input.Substring(i + 1).Trim();
                    i = -1;
                }
            }
            // Son parçayı ekle
            if (!string.IsNullOrWhiteSpace(input))
            {
                result.Add(input.Trim());
            }

            return result;
        }
    }
}