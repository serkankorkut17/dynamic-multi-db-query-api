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

            // DISTINCT Part
            queryModel.Distinct = queryString.Contains("FETCHD", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCHDISTINCT", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCH DISTINCT", StringComparison.OrdinalIgnoreCase);

            // INCLUDE Part
            queryModel.Includes = ParseIncludes(queryString, queryModel.Table);

            // FILTER Part
            queryModel.Filters = ParseFilters(queryString, queryModel.Table);

            // GROUP BY Part
            queryModel.GroupBy = ParseGroupBy(queryString, queryModel.Table);

            // HAVING Part
            queryModel.Having = ParseHaving(queryString, queryModel.Table);

            // ORDER BY Part
            queryModel.OrderBy = ParseOrderBy(queryString, queryModel.Table);

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
        private List<string> ParseFetchColumns(string queryString, string tableName)
        {
            // "FETCH(", "FETCHD(", "FETCHDISTINCT(" veya "FETCH DISTINCT(" ile başlayan ve ")" ile biten kısmı al
            var m = Regex.Match(queryString, @"\bFETCH(?:\s+DISTINCT|DISTINCT|D)?\s*\(", RegexOptions.IgnoreCase);
            if (!m.Success)
            {
                return new List<string> { "*" };
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
                return new List<string> { "*" };
            }

            var columns = columnsPart.Split(',').Select(c => c.Trim()).ToList();

            for (int i = 0; i < columns.Count; i++)
            {
                // Her column için tablo ismi ekle
                columns[i] = AddTablePrefixToColumn(columns[i], tableName);
            }
            return columns;
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
        private FilterModel? ParseFilters(string queryString, string tableName)
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
            var filterModel = BuildFilterModel(body, tableName);


            return filterModel;
        }

        // GROUPBY(...) kısmını parse etme
        private List<string> ParseGroupBy(string queryString, string tableName)
        {
            var m = Regex.Match(queryString, @"\bGROUPBY\s*\(\s*(.*?)\s*\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success) return new List<string>();

            var body = m.Groups[1].Value;

            var groupByColumns = body.Split(',').Select(c => c.Trim()).Where(c => !string.IsNullOrWhiteSpace(c)).ToList();

            // Her column için tablo ismi ekle
            for (int i = 0; i < groupByColumns.Count; i++)
            {
                groupByColumns[i] = AddTablePrefixToColumn(groupByColumns[i], tableName);
            }

            return groupByColumns;
        }

        // HAVING(...) kısmını parse etme
        private FilterModel? ParseHaving(string queryString, string tableName)
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
            var filterModel = BuildFilterModel(body, tableName);

            return filterModel;
        }

        // ORDERBY(...) kısmını parse etme
        private List<OrderByModel> ParseOrderBy(string queryString, string tableName)
        {
            var m = Regex.Match(queryString, @"\bORDERBY\s*\(\s*(.*?)\s*\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success) return new List<OrderByModel>();

            var body = m.Groups[1].Value;

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
                        var columnWithPrefix = AddTablePrefixToColumn(parts[0], tableName);
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

        // Columnlara table ismi ekleme
        private string AddTablePrefixToColumn(string column, string tableName)
        {
            if (string.IsNullOrWhiteSpace(column)) return column;

            column = column.Trim();

            // COUNT(*) veya SUM(*) gibi durumlar için kontrol et
            if (column == "*") return column;

            // Aggregate fonksiyonlarını yakala (COUNT, SUM, AVG, MIN, MAX)
            var aggregateFuncs = new[] { "COUNT", "SUM", "AVG", "MIN", "MAX" };

            foreach (var func in aggregateFuncs)
            {
                // Regex: func ( ... )
                var pattern = $@"^{func}\s*\(\s*(?<inner>.+?)\s*\)$";
                var match = Regex.Match(column, pattern, RegexOptions.IgnoreCase);

                if (match.Success)
                {
                    var inner = match.Groups["inner"].Value.Trim();

                    // Eğer inner '*' ise dokunma
                    if (inner == "*")
                    {
                        return $"{func.ToUpper()}(*)";
                    }

                    // Recursive olarak prefix ekle
                    inner = AddTablePrefixToColumn(inner, tableName);
                    return $"{func.ToUpper()}({inner})";
                }
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
        private FilterModel? BuildFilterModel(string body, string tableName)
        {
            if (IsSingleConditionFilter(body))
            {
                var cond = ParseConditionFilter(body, tableName);
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
                filter.Left = BuildFilterModel(tokens[0], tableName)!;
                filter.Right = BuildFilterModel(tokens[2], tableName)!;

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
        private ConditionFilterModel? ParseConditionFilter(string expr, string tableName)
        {
            if (string.IsNullOrWhiteSpace(expr)) return null;

            string Unquote(string v)
            {
                v = v.Trim();
                if (v.Length >= 2 && v[0] == '\'' && v[^1] == '\'')
                    return v.Substring(1, v.Length - 2).Replace("''", "'");
                return v;
            }

            var s = expr.Trim();

            //!!! IS [NOT] NULL
            var mIs = Regex.Match(
                s,
                @"^\s*(?<col>(?:\$\d+|[\w\.\-]+|'[^']+'|""[^""]+""|\[[^\]]+\]))\s+IS\s+(?<not>NOT)?\s+NULL\s*$",
                RegexOptions.IgnoreCase);
            if (mIs.Success)
            {
                return new ConditionFilterModel
                {
                    Column = mIs.Groups["col"].Value,
                    Operator = mIs.Groups["not"].Success ? ComparisonOperator.IsNotNull : ComparisonOperator.IsNull,
                    Value = null
                };
            }

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
            var rhsRaw = m.Groups["rhs"].Value.Trim();

            // Value tek tırnaklıysa tırnakları kaldır
            string rhsValue = rhsRaw;
            if (rhsRaw.StartsWith("'") && rhsRaw.EndsWith("'"))
            {
                rhsValue = Unquote(rhsRaw);
            }

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

            // Her column için tablo ismi ekle
            var columnWithPrefix = AddTablePrefixToColumn(col, tableName);

            return new ConditionFilterModel
            {
                Column = columnWithPrefix,
                Operator = comp,
                Value = rhsValue
            };
        }
    }
}