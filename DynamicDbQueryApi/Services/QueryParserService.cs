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
using DynamicDbQueryApi.Helpers;

namespace DynamicDbQueryApi.Services
{
    public class QueryParserService : IQueryParserService
    {
        // Ana parse metodu
        public QueryModel Parse(string queryString)
        {
            if (!StringHelpers.CheckIfParanthesesBalanced(queryString))
            {
                throw new Exception("Query string has unbalanced parentheses.");
            }

            var queryModel = new QueryModel();

            // FROM Part
            queryModel.Table = ParseFromTable(queryString);

            // FETCH Part
            queryModel.Columns = ParseFetchColumns(queryString, queryModel.Table);
            List<string> aliasList = queryModel.Columns.Where(c => !string.IsNullOrWhiteSpace(c.Alias)).Select(c => c.Alias!).ToList();

            // Alias - Column Dictionary
            var aliasColumnDict = queryModel.Columns
                .Where(c => !string.IsNullOrWhiteSpace(c.Alias))
                .ToDictionary(c => c.Alias!, c => c.Expression);

            // DISTINCT Part
            queryModel.Distinct = queryString.Contains("FETCHD", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCHDISTINCT", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCH DISTINCT", StringComparison.OrdinalIgnoreCase);

            // INCLUDE Part
            queryModel.Includes = ParseIncludes(queryString, queryModel.Table);

            // FILTER Part
            queryModel.Filters = ParseFilters(queryString, queryModel.Table, aliasColumnDict);

            // GROUP BY Part
            queryModel.GroupBy = ParseGroupBy(queryString, queryModel.Table, aliasColumnDict);

            // HAVING Part
            queryModel.Having = ParseHaving(queryString, queryModel.Table, aliasColumnDict);

            // ORDER BY Part
            queryModel.OrderBy = ParseOrderBy(queryString, queryModel.Table, aliasColumnDict);

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
            int fetchEnd = StringHelpers.FindClosingParenthesis(queryString, m.Index + m.Length - 1);
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

            List<string> columns = StringHelpers.SplitByCommas(columnsPart);

            List<QueryColumnModel> columnModels = new List<QueryColumnModel>();

            for (int i = 0; i < columns.Count; i++)
            {
                // Eğer * ise direkt ekle ve alias null bırak
                if (columns[i].Trim() == "*")
                {
                    columnModels.Add(new QueryColumnModel
                    {
                        Expression = "*",
                        Alias = null
                    });
                    continue;
                }
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
                    var func = ExtractFunction(parts[0].Trim());
                    if (StringHelpers.IsDateOrTimestamp(expression))
                    {
                        alias = "date_" + i;
                    }
                    else if (func != null)
                    {
                        alias = func.Value.function.ToLowerInvariant();
                    }
                    else
                    {
                        // Diğer karakterleri kaldır ve sadece harf, rakam, _ bırak
                        alias = Regex.Replace(parts[0].Trim(), @"[^\w]", "_").ToLowerInvariant();

                        // harflerle başlamıyorsa başına _ ekle
                        if (!string.IsNullOrWhiteSpace(alias) && !char.IsLetter(alias[0]) && alias[0] != '_')
                        {
                            alias = "_" + alias;
                        }
                    }

                    // Eğer alias sayı ile başlıyorsa başına _ ekle
                    if (char.IsDigit(alias[0]))
                    {
                        alias = "_" + alias;
                    }
                }
                columnModels.Add(new QueryColumnModel
                {
                    Expression = expression,
                    Alias = alias
                });
            }

            // Aynı alias varsa sonuna _1, _2 ekle
            var aliasCount = new Dictionary<string, int>();
            foreach (var column in columnModels)
            {
                if (!string.IsNullOrEmpty(column.Alias))
                {
                    if (aliasCount.ContainsKey(column.Alias))
                    {
                        aliasCount[column.Alias]++;
                        column.Alias += "_" + aliasCount[column.Alias];
                    }
                    else
                    {
                        aliasCount[column.Alias] = 1;
                    }
                }
            }
            return columnModels;
        }

        // INCLUDE(...) kısmını parse etme
        private List<IncludeModel> ParseIncludes(string queryString, string table)
        {
            var includes = new List<IncludeModel>();
            var start = Regex.Match(queryString, @"\bINCLUDE\s*\(", RegexOptions.IgnoreCase);
            if (!start.Success) return includes;

            // Parantez başlangıç indeksini ve kapanış indeksini bul
            int openIdx = start.Index + start.Length - 1;
            int bodyStart = openIdx + 1;
            int closeIdx = StringHelpers.FindClosingParenthesis(queryString, openIdx);
            if (closeIdx == -1) return includes;

            // INCLUDE(...) içindeki metni al
            var body = queryString.Substring(bodyStart, closeIdx - bodyStart).Trim();
            if (string.IsNullOrWhiteSpace(body)) return includes;

            var includeTables = StringHelpers.SplitByCommas(body);

            foreach (var include in includeTables)
            {
                // Join type için boşlukla ayır
                var partsWithJoin = StringHelpers.SplitByWhitespaces(include);
                var includeTable = partsWithJoin[0];
                var joinType = "LEFT";

                // Eğer bir join tipi belirtilmişse al
                if (partsWithJoin.Count > 1)
                {
                    joinType = partsWithJoin[1].ToUpperInvariant();
                }

                // Eğer birden fazla tablo varsa hepsini ayır (orders.items.details)
                var parts = includeTable.Split('.');

                // ilk tabloyu ana tablo ile ilişkilendir
                var firstInclude = GetIncludeModel(table, parts[0], joinType);
                includes.Add(firstInclude);

                // Ara tabloları ilişkilendir
                if (parts.Length > 1)
                {
                    for (int i = 1; i < parts.Length; i++)
                    {
                        var parentTable = parts[i - 1];
                        var childTable = parts[i];

                        var includeModel = GetIncludeModel(parentTable, childTable, joinType);
                        includes.Add(includeModel);
                    }
                }
            }

            return includes;
        }

        public IncludeModel GetIncludeModel(string fromTable, string include, string joinType = "LEFT")
        {
            // table (column1, column2) şeklinde include varsa tablo ismini ve kolonları ayır
            if (include.Contains('(') && include.Contains(')'))
            {
                var tablePart = include;
                var tableName = tablePart.Substring(0, tablePart.IndexOf('(')).Trim();
                var columnsPart = tablePart.Substring(tablePart.IndexOf('(') + 1, tablePart.IndexOf(')') - tablePart.IndexOf('(') - 1);
                var columns = StringHelpers.SplitByCommas(columnsPart);

                // Eğer kolonlar 2 tane değilse hata fırlat
                if (columns.Count != 2)
                {
                    throw new Exception("Include with columns must have exactly two columns: table(column1, column2)");
                }

                string col1 = columns[0].Trim();
                string col2 = columns[1].Trim();
                // kolonlar table.column şeklindeyse sadece column kısmını al
                if (columns[0].Contains('.'))
                {
                    col1 = columns[0].Substring(columns[0].IndexOf('.') + 1).Trim();
                }
                if (columns[1].Contains('.'))
                {
                    col2 = columns[1].Substring(columns[1].IndexOf('.') + 1).Trim();
                }

                return new IncludeModel
                {
                    Table = fromTable,
                    TableKey = col1, // Ana tablonun anahtarı
                    IncludeTable = tableName,
                    IncludeKey = col2, // Include tablonun anahtarı
                    JoinType = joinType
                };
            }
            else
            {
                // Normal include ise tablo ismi olarak al
                return new IncludeModel
                {
                    Table = fromTable,
                    TableKey = null, // Varsayılan anahtar
                    IncludeTable = include,
                    IncludeKey = null, // Varsayılan include table anahtarı
                    JoinType = joinType
                };
            }
        }

        // FILTER(...) kısmını parse etme
        private FilterModel? ParseFilters(string queryString, string tableName, Dictionary<string, string> aliasColumnDict)
        {
            var start = Regex.Match(queryString, @"\bFILTER\s*\(", RegexOptions.IgnoreCase);
            if (!start.Success) return null;

            // Parantez başlangıç indeksini ve kapanış indeksini bul
            int openIdx = start.Index + start.Length - 1;
            int bodyStart = openIdx + 1;
            int closeIdx = StringHelpers.FindClosingParenthesis(queryString, openIdx);
            if (closeIdx == -1) return null;

            // FILTER(...) içindeki metni al
            var body = queryString.Substring(bodyStart, closeIdx - bodyStart).Trim();
            if (string.IsNullOrWhiteSpace(body)) return null;

            // FILTER ifadesini FilterModel yapısına dönüştür
            var filterModel = BuildFilterModel(body, tableName, aliasColumnDict);


            return filterModel;
        }

        // GROUPBY(...) kısmını parse etme
        private List<string> ParseGroupBy(string queryString, string tableName, Dictionary<string, string> aliasColumnDict)
        {
            var m = Regex.Match(queryString, @"\bGROUPBY\s*\(\s*(.*?)\s*\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success) return new List<string>();

            var body = m.Groups[1].Value;

            var groupByColumns = StringHelpers.SplitByCommas(body);

            // Her column için tablo ismi ekle
            for (int i = 0; i < groupByColumns.Count; i++)
            {
                groupByColumns[i] = AddTablePrefixToColumn(groupByColumns[i], tableName, aliasColumnDict);
            }

            return groupByColumns;
        }

        // HAVING(...) kısmını parse etme
        private FilterModel? ParseHaving(string queryString, string tableName, Dictionary<string, string> aliasColumnDict)
        {
            var start = Regex.Match(queryString, @"\bHAVING\s*\(", RegexOptions.IgnoreCase);
            if (!start.Success) return null;

            // Parantez başlangıç indeksini ve kapanış indeksini bul
            int openIdx = start.Index + start.Length - 1;
            int bodyStart = openIdx + 1;
            int closeIdx = StringHelpers.FindClosingParenthesis(queryString, openIdx);
            if (closeIdx == -1) return null;

            // HAVING(...) içindeki metni al
            var body = queryString.Substring(bodyStart, closeIdx - bodyStart).Trim();
            if (string.IsNullOrWhiteSpace(body)) return null;

            // HAVING ifadesini FilterModel yapısına dönüştür
            var filterModel = BuildFilterModel(body, tableName, aliasColumnDict);

            return filterModel;
        }

        // ORDERBY(...) kısmını parse etme
        private List<OrderByModel> ParseOrderBy(string queryString, string tableName, Dictionary<string, string> aliasColumnDict)
        {
            var start = Regex.Match(queryString, @"\bORDERBY\s*\(", RegexOptions.IgnoreCase);
            if (!start.Success) return new List<OrderByModel>();

            // Parantez başlangıç indeksini ve kapanış indeksini bul
            int openIdx = start.Index + start.Length - 1;
            int bodyStart = openIdx + 1;
            int closeIdx = StringHelpers.FindClosingParenthesis(queryString, openIdx);
            if (closeIdx == -1) return new List<OrderByModel>();

            var body = queryString.Substring(bodyStart, closeIdx - bodyStart).Trim();

            var orderByColumns = new List<OrderByModel>();
            var columns = StringHelpers.SplitByCommas(body);

            // Her column için tablo ismi ekle ve DESC kontrolü yap
            foreach (var column in columns)
            {
                if (!string.IsNullOrWhiteSpace(column))
                {
                    var parts = StringHelpers.SplitByWhitespaces(column);
                    if (parts.Count > 0)
                    {
                        var columnWithPrefix = AddTablePrefixToColumn(parts[0], tableName, aliasColumnDict);
                        var orderByModel = new OrderByModel
                        {
                            Column = columnWithPrefix,
                            Desc = parts.Count > 1 && parts[1].Equals("DESC", StringComparison.OrdinalIgnoreCase)
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

            var limit = m.Groups[1].Value;

            if (int.TryParse(limit, out var limitValue))
            {
                return limitValue;
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

            var offset = m.Groups[1].Value;

            if (int.TryParse(offset, out var offsetValue))
            {
                return offsetValue;
            }
            else
            {
                throw new Exception("Could not parse OFFSET/SKIP value.");
            }
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
                int closeIdx = StringHelpers.FindClosingParenthesis(s, openIdx);
                if (closeIdx == -1) throw new Exception("Could not find closing parenthesis for function in column.");

                var body = s.Substring(bodyStart, closeIdx - bodyStart).Trim();

                var funcName = start.Groups["func"].Value.ToUpperInvariant();
                // Fonksiyonu parse et
                return (funcName, body);
            }
            return null;
        }

        // Fonksiyonları parse etme
        private string ParseFunction(string functionString, string inner, string tableName, Dictionary<string, string>? aliasColumnDict = null)
        {
            // Fonksiyon içindeki parametreleri ayır
            List<string> innerParams;
            if (string.IsNullOrWhiteSpace(inner))
            {
                innerParams = new List<string>();
            }
            else
            {
                innerParams = StringHelpers.SplitByCommas(inner);
            }

            if (innerParams.Count != 0)
            {
                for (int i = 0; i < innerParams.Count; i++)
                {
                    if (functionString.Equals("IF", StringComparison.OrdinalIgnoreCase) && i == 0)
                    {
                        // IF fonksiyonunun ilk parametresi bir condition olduğu için burada işlem yapma
                        continue;
                    }
                    if (functionString.Equals("CASE", StringComparison.OrdinalIgnoreCase) || functionString.Equals("IFS", StringComparison.OrdinalIgnoreCase))
                    {
                        // CASE/IFS fonksiyonunun çift parametreleri condition olduğu için burada işlem yapma
                        if (i % 2 == 0) continue;
                    }
                    innerParams[i] = AddTablePrefixToColumn(innerParams[i], tableName, aliasColumnDict);
                }
            }

            // IF fonksiyonunu yakala (IF(condition, trueValue, falseValue))
            if (functionString.Equals("IF", StringComparison.OrdinalIgnoreCase))
            {
                if (innerParams.Count == 3)
                {
                    var condition = innerParams[0];
                    var filterModel = BuildFilterModel(condition, tableName, aliasColumnDict!);
                    var trueValue = AddTablePrefixToColumn(innerParams[1], tableName, aliasColumnDict);
                    var falseValue = AddTablePrefixToColumn(innerParams[2], tableName, aliasColumnDict);
                    var options = new JsonSerializerOptions
                    {
                        WriteIndented = true
                    };
                    string conditionString = JsonSerializer.Serialize<FilterModel>(filterModel!, options);

                    return $"IF({conditionString}, {trueValue}, {falseValue})";
                }
                else
                {
                    throw new Exception($"Invalid usage of IF function with incorrect number of parameters.");
                }
            }

            // CASE/IFS: (cond1, val1, cond2, val2, ..., elseVal)
            if (functionString.Equals("CASE", StringComparison.OrdinalIgnoreCase) ||
                functionString.Equals("IFS", StringComparison.OrdinalIgnoreCase))
            {
                if (innerParams.Count < 3 || innerParams.Count % 2 == 0)
                    throw new Exception("CASE/IFS requires (cond1, val1, ..., elseVal) with odd arg count.");

                for (int i = 0; i < innerParams.Count - 1; i += 2)
                {
                    var condRaw = innerParams[i].Trim();
                    var condModel = BuildFilterModel(condRaw, tableName, aliasColumnDict!);
                    var options = new JsonSerializerOptions
                    {
                        WriteIndented = true
                    };
                    string conditionString = JsonSerializer.Serialize<FilterModel>(condModel!, options);
                    innerParams[i] = conditionString;
                }

                return $"IFS({string.Join(", ", innerParams)})";
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
            var dateFuncs = new[] { "NOW", "GETDATE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "TODAY", "CURRENT_TIME", "TIME", "DATEADD", "DATEDIFF", "DATENAME", "TO_CHAR", "DAY", "MONTH", "YEAR" };
            if (dateFuncs.Contains(functionString))
            {
                if (functionString == "NOW" || functionString == "GETDATE" || functionString == "CURRENT_TIMESTAMP" || functionString == "CURRENT_DATE" || functionString == "TODAY" || functionString == "CURRENT_TIME" || functionString == "TIME")
                {
                    if (innerParams.Count > 1)
                    {
                        throw new Exception($"Invalid usage of date function {functionString} with parameters.");
                    }
                    else if (innerParams.Count == 1 && !string.IsNullOrWhiteSpace(innerParams[0]))
                    {
                        return $"{functionString}({innerParams[0]})";
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
        private string AddTablePrefixToColumn(string column, string tableName, Dictionary<string, string>? aliasColumnDict = null)
        {
            if (string.IsNullOrWhiteSpace(column)) return column;

            // Eğer TRUE, FALSE, NULL ise tablo ismi ekleme
            if (column.Equals("TRUE", StringComparison.OrdinalIgnoreCase) || column.Equals("FALSE", StringComparison.OrdinalIgnoreCase) || column.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            {
                return column.ToUpperInvariant();
            }

            //  Eğer SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR ise tablo ismi ekleme
            var dateParts = new[] { "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "YEAR" };
            if (dateParts.Contains(column.ToUpperInvariant()))
            {
                return column.ToUpperInvariant();
            }

            // Eğer tek tırnak içinde string ise tablo ismi ekleme
            if (column.StartsWith("'") && column.EndsWith("'"))
            {
                return column;
            }

            // Eğer x+y, x-y, x*y, x/y işlemi ise tablo ismi ekleme
            if (Regex.IsMatch(column, @"^[\w\d\._\-]+\s*[\+\-\*/]\s*[\w\d\._\-]+$"))
            {
                var partsMath = Regex.Split(column, @"\s*([\+\-\*/])\s*");
                if (partsMath.Length == 3)
                {
                    var left = AddTablePrefixToColumn(partsMath[0], tableName, aliasColumnDict);
                    var op = partsMath[1];
                    var right = AddTablePrefixToColumn(partsMath[2], tableName, aliasColumnDict);
                    return $"{left} {op} {right}";
                }
            }

            // Eğer 3.14, -42 gibi sayı ise tablo ismi ekleme
            if (double.TryParse(column, out _))
            {
                return column;
            }

            // Eğer sayı ise tablo ismi ekleme
            if (int.TryParse(column, out _))
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
                return ParseFunction(funcName, inner, tableName, aliasColumnDict);
            }

            // Eğer aliasColumnDict verilmişse ve column bu dictte varsa karşılığı ile değiştir
            if (aliasColumnDict != null && aliasColumnDict.TryGetValue(column, out var actualColumn))
            {
                return actualColumn;
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

        // FILTER ifadesini FilterModel yapısına dönüştürme
        private FilterModel? BuildFilterModel(string body, string tableName, Dictionary<string, string> aliasColumnDict)
        {
            if (IsSingleConditionFilter(body))
            {
                var cond = ParseConditionFilter(body, tableName, aliasColumnDict);
                return cond;
            }

            // ExpressionSplitter kullanarak en dıştaki operandları çıkar ve yerlerine $0, $1, ... koy
            var exprList = StringHelpers.ExtractTopLevelOperands(ref body, out var replacedBody);

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
                var multiExprList = StringHelpers.ExtractTopLevelOperands(ref str, out var multiReplacedBody);

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
                filter.Left = BuildFilterModel(tokens[0], tableName, aliasColumnDict)!;
                filter.Right = BuildFilterModel(tokens[2], tableName, aliasColumnDict)!;

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
        private ConditionFilterModel? ParseConditionFilter(string expr, string tableName, Dictionary<string, string> aliasColumnDict)
        {
            if (string.IsNullOrWhiteSpace(expr)) return null;

            var s = expr.Trim();

            var exprParts = StringHelpers.SplitByWhitespaces(s);
            string col = exprParts[0];

            s = exprParts.Count > 1 ? string.Join(' ', exprParts.Skip(1)) : "";

            var pattern = @"^\s*
                    (?<op>
                        >=|<=|!=|<>|==|=|>|<
                        |NOT\s+IN|IN
                        |NOT\s+BETWEEN|BETWEEN
                        |NOT\s+LIKE|LIKE
                        |NOT\s+ILIKE|ILIKE
                        |NOT\s+CONTAINS|CONTAINS
                        |NOT\s+ICONTAINS|ICONTAINS
                        |NOT\s+STARTSWITH|STARTSWITH
                        |NOT\s+ISTARTSWITH|ISTARTSWITH
                        |NOT\s+BEGINSWITH|BEGINSWITH
                        |NOT\s+IBEGINSWITH|IBEGINSWITH
                        |NOT\s+ENDSWITH|ENDSWITH
                        |NOT\s+IENDSWITH|IENDSWITH
                        |IS\s+NOT|IS
                    )
                    \s*
                    (?:\(\s*(?<rhs>.*?)\s*\)|(?<rhs>.+?))?
                    \s*$";

            var m = Regex.Match(s, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.IgnorePatternWhitespace);

            if (!m.Success)
                throw new Exception($"Could not parse condition filter expression: {expr}");

            string op = m.Groups["op"].Value.ToUpperInvariant();
            string rhs = m.Groups["rhs"].Value.Trim();

            Console.WriteLine($"Condition: {s}, Col: {col}, Op: {op}, Rhs: {rhs}");
            

            // Genel operatorler (>=, <=, !=, <>, ==, =, >, <, CONTAINS, STARTSWITH, BEGINSWITH, ENDSWITH, LIKE)
            // var m = Regex.Match(s,
            //     @"^\s*(?<col>[\w\.\-\(\)\*\s,']+?)\s*(?<op1>>=|<=|!=|<>|==|=|>|<|LIKE|CONTAINS|STARTSWITH|BEGINSWITH|ENDSWITH|IN|BETWEEN)\s*(?<rhs>.+?)\s*$",
            //     RegexOptions.IgnoreCase | RegexOptions.Singleline);

            // var m = Regex.Match(s,
            //     @"^\s*(?<col>[\w\.\-\(\)\*']+)\s*(?<op1>>=|<=|!=|<>|==|=|>|<|\bBETWEEN\b|\bSTARTSWITH\b|\bBEGINSWITH\b|\bENDSWITH\b|\bCONTAINS\b|\bLIKE\b|\bIS\b|\bIN\b)\s*(?<rhs>.+?)\s*$",
            //     RegexOptions.IgnoreCase | RegexOptions.Singleline);

            // Eğer rhs parantez ile başlıyor ve bitiyorsa parantezleri kaldır
            if (rhs.StartsWith("(") && rhs.EndsWith(")"))
            {
                rhs = rhs.Substring(1, rhs.Length - 2).Trim();
            }

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
                case "LIKE": comp = ComparisonOperator.Like; break;
                case "ILIKE": comp = ComparisonOperator.ILike; break;
                case "NOT LIKE": comp = ComparisonOperator.NotLike; break;
                case "NOT ILIKE": comp = ComparisonOperator.NotILike; break;
                case "CONTAINS": comp = ComparisonOperator.Contains; break;
                case "ICONTAINS": comp = ComparisonOperator.IContains; break;
                case "NOT CONTAINS": comp = ComparisonOperator.NotContains; break;
                case "NOT ICONTAINS": comp = ComparisonOperator.NotIContains; break;
                case "STARTSWITH": comp = ComparisonOperator.BeginsWith; break;
                case "BEGINSWITH": comp = ComparisonOperator.BeginsWith; break;
                case "ISTARTSWITH": comp = ComparisonOperator.IBeginsWith; break;
                case "IBEGINSWITH": comp = ComparisonOperator.IBeginsWith; break;
                case "NOT STARTSWITH": comp = ComparisonOperator.NotBeginsWith; break;
                case "NOT BEGINSWITH": comp = ComparisonOperator.NotBeginsWith; break;
                case "NOT ISTARTSWITH": comp = ComparisonOperator.NotIBeginsWith; break;
                case "NOT IBEGINSWITH": comp = ComparisonOperator.NotIBeginsWith; break;
                case "ENDSWITH": comp = ComparisonOperator.EndsWith; break;
                case "IENDSWITH": comp = ComparisonOperator.IEndsWith; break;
                case "NOT ENDSWITH": comp = ComparisonOperator.NotEndsWith; break;
                case "NOT IENDSWITH": comp = ComparisonOperator.NotIEndsWith; break;
                case "IS": comp = ComparisonOperator.IsNull; break;
                case "IS NOT": comp = ComparisonOperator.IsNotNull; break;
                case "IN": comp = ComparisonOperator.In; break;
                case "NOT IN": comp = ComparisonOperator.NotIn; break;
                case "BETWEEN": comp = ComparisonOperator.Between; break;
                case "NOT BETWEEN": comp = ComparisonOperator.NotBetween; break;
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

            col = AddTablePrefixToColumn(col, tableName, aliasColumnDict);
            // RHS eğer liste ise her bir elemanına tablo ismi ekle
            if (comp == ComparisonOperator.In || comp == ComparisonOperator.NotIn)
            {
                var items = StringHelpers.SplitByCommas(rhs);
                for (int i = 0; i < items.Count; i++)
                {
                    items[i] = AddTablePrefixToColumn(items[i], tableName, aliasColumnDict);
                }
                rhs = string.Join(", ", items);
            }
            // RHS eğer BETWEEN ise iki tarafına tablo ismi ekle
            else if (comp == ComparisonOperator.Between || comp == ComparisonOperator.NotBetween)
            {
                var parts = StringHelpers.SplitByCommas(rhs);
                if (parts.Count != 2)
                {
                    throw new Exception("BETWEEN operator requires two values separated by a comma.");
                }
                var left = AddTablePrefixToColumn(parts[0], tableName, aliasColumnDict);
                var right = AddTablePrefixToColumn(parts[1], tableName, aliasColumnDict);
                rhs = $"{left}, {right}";
            }
            else
            {
                rhs = AddTablePrefixToColumn(rhs, tableName, aliasColumnDict);
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