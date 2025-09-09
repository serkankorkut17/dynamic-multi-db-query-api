using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Interfaces;
using ZstdSharp.Unsafe;

namespace DynamicDbQueryApi.Services
{
    public class QueryParser : IQueryParser
    {
        private readonly IDbConnection _connection;
        private readonly string _dbType;
        private string? _table;
        public QueryParser(IDbConnection connection, string dbType)
        {
            _connection = connection;
            _dbType = dbType;
        }

        public QueryModel Parse(string queryString)
        {
            var queryModel = new QueryModel();

            // FROM Part
            queryModel.Table = ParseFromTable(queryString);
            _table = queryModel.Table;

            // FETCH Part
            queryModel.Columns = ParseFetchColumns(queryString);

            // DISTINCT Part
            queryModel.Distinct = queryString.Contains("FETCHD", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCHDISTINCT", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCH DISTINCT", StringComparison.OrdinalIgnoreCase);

            // INCLUDE Part
            queryModel.Includes = ParseIncludes(queryModel.Table, queryString);

            // FILTER Part
            queryModel.Filters = ParseFilters(queryString);

            // GROUP BY Part
            queryModel.GroupBy = ParseGroupBy(queryString);

            // ORDER BY Part
            queryModel.OrderBy = ParseOrderBy(queryString);

            // LIMIT Part
            queryModel.Limit = ParseLimit(queryString);

            // OFFSET Part
            queryModel.Offset = ParseOffset(queryString);

            return queryModel;
        }

        private string AddTablePrefixToColumn(string column)
        {
            if (string.IsNullOrWhiteSpace(column) || column == "*") return column;

            var parts = column.Split('.');
            // Eğer tablo ismi yoksa ekle
            if (parts.Length == 1)
            {
                return $"{_table}.{parts[0]}";
            }
            else if (parts.Length > 1)
            {
                return $"{parts[parts.Length - 2]}.{parts[parts.Length - 1]}";
            }
            return column;
        }

        private List<string> ParseFetchColumns(string queryString)
        {
            // "FETCH(", "FETCHD(", "FETCHDISTINCT(" veya "FETCH DISTINCT(" ile başlayan ve ")" ile biten kısmı al
            var m = Regex.Match(queryString, @"\bFETCH(?:\s+DISTINCT|DISTINCT|D)?\s*\(", RegexOptions.IgnoreCase);
            if (!m.Success)
            {
                return new List<string> { "*" };
            }
            var fetchStart = m.Index + m.Length;
            var fetchEnd = queryString.IndexOf(")", fetchStart);
            var columnsPart = queryString.Substring(fetchStart, fetchEnd - fetchStart);

            var columns = columnsPart.Split(',').Select(c => c.Trim()).ToList();
            // Console.WriteLine($"Text: {JsonSerializer.Serialize(columns)}");

            for (int i = 0; i < columns.Count; i++)
            {
                // split sonra her parçayı trimle
                columns[i] = AddTablePrefixToColumn(columns[i]);
            }

            return columns;
        }

        private string ParseFromTable(string queryString)
        {
            var m = Regex.Match(queryString, @"\bFROM\s*\(?\s*(?<table>[\w\d\._\-]+)\s*\)?", RegexOptions.IgnoreCase);
            if (!m.Success)
            {
                throw new Exception("Could not find FROM clause with table name.");
            }

            var table = m.Groups["table"].Value.Trim();

            return table;
        }

        private ForeignKeyPair? GetIncludeKeys(string table, string includeTable)
        {
            var includeSql = GetIncludeQuery(table, includeTable);

            var includeResult = _connection.Query(includeSql).FirstOrDefault();

            if (includeResult != null)
            {
                var pair = new ForeignKeyPair();
                IDictionary<string, object>? dict = includeResult as IDictionary<string, object>;

                if (dict != null)
                {
                    pair.ForeignKey = dict["fk_column"]?.ToString() ?? "";
                    pair.ReferencedKey = dict["referenced_column"]?.ToString() ?? "";
                }
                else
                {
                    pair.ForeignKey = "";
                    pair.ReferencedKey = "";
                }

                return pair;
            }

            return null;
        }

        private List<IncludeModel> ParseIncludes(string table, string queryString)
        {
            var includes = new List<IncludeModel>();
            var m = Regex.Match(queryString, @"\bINCLUDE\s*\(\s*(.*?)\s*\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success) return includes;

            var body = m.Groups[1].Value;

            var includeTables = body.Split(',').Select(c => c.Trim()).Where(c => !string.IsNullOrWhiteSpace(c)).ToList();

            foreach (var include in includeTables)
            {
                // first get join type if exists
                var partsWithJoin = include.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var includeTable = partsWithJoin[0];
                var joinType = "LEFT";

                if (partsWithJoin.Length > 1)
                {
                    joinType = partsWithJoin[1].ToUpper();
                }

                var parts = includeTable.Split('.');

                var pairs = GetIncludeKeys(table, parts[0]);

                if (pairs != null)
                {
                    includes.Add(new IncludeModel
                    {
                        Table = table,
                        TableKey = pairs.ForeignKey,
                        IncludeTable = parts[0],
                        IncludeKey = pairs.ReferencedKey,
                        JoinType = joinType
                    });
                }
                else
                {
                    pairs = GetIncludeKeys(parts[0], table);

                    if (pairs != null)
                    {
                        includes.Add(new IncludeModel
                        {
                            Table = parts[0],
                            TableKey = pairs.ForeignKey,
                            IncludeTable = table,
                            IncludeKey = pairs.ReferencedKey,
                            JoinType = joinType
                        });
                    }
                    else
                    {
                        throw new Exception($"Could not find foreign key relationship between {table} and {include}");
                    }
                }


                if (parts.Length > 1)
                {
                    for (int i = 1; i < parts.Length; i++)
                    {
                        var parentTable = parts[i - 1];
                        var childTable = parts[i];

                        pairs = GetIncludeKeys(parentTable, childTable);

                        if (pairs != null)
                        {
                            includes.Add(new IncludeModel
                            {
                                Table = parentTable,
                                TableKey = pairs.ForeignKey,
                                IncludeTable = childTable,
                                IncludeKey = pairs.ReferencedKey,
                                JoinType = "LEFT"
                            });
                        }
                        else
                        {
                            pairs = GetIncludeKeys(childTable, parentTable);

                            if (pairs != null)
                            {
                                includes.Add(new IncludeModel
                                {
                                    Table = childTable,
                                    TableKey = pairs.ForeignKey,
                                    IncludeTable = parentTable,
                                    IncludeKey = pairs.ReferencedKey,
                                    JoinType = "LEFT"
                                });
                            }
                            else
                            {
                                throw new Exception($"Could not find foreign key relationship between {parentTable} and {childTable}");
                            }
                        }
                    }
                }

            }

            return includes;
        }

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

        private FilterModel? ParseFilters(string queryString)
        {
            var start = Regex.Match(queryString, @"\bFILTER\s*\(", RegexOptions.IgnoreCase);
            if (!start.Success) return null;

            int openIdx = start.Index + start.Length - 1; // index of '('
            int bodyStart = openIdx + 1;
            int closeIdx = FindClosingParenthesis(queryString, openIdx);
            if (closeIdx == -1) return null; // or throw

            var body = queryString.Substring(bodyStart, closeIdx - bodyStart).Trim();
            if (string.IsNullOrWhiteSpace(body)) return null;
            // Console.WriteLine($"Text: {body}");

            var filterModel = BuildFilterModel(body);
            // Console.WriteLine($"Built FilterModel: {JsonSerializer.Serialize(filterModel)}");

            return filterModel;
        }

        private FilterModel? BuildFilterModel(string body)
        {
            // Console.WriteLine($"Building FilterModel for body: {body}");

            if (IsSingleConditionFilter(body))
            {
                var cond = ParseConditionFilter(body);
                // Console.WriteLine($"Text after parsing single condition filter: {JsonSerializer.Serialize(cond)}");
                return cond;
            }

            var exprList = ExpressionSplitter.ExtractTopLevelOperands(ref body, out var replacedBody);
            // Console.WriteLine($"Text after parsing: {replacedBody} with {exprList.Count} expressions");

            var finalExprList = exprList;
            var finalReplacedBody = replacedBody;
            if (exprList.Count > 2)
            {

                var str = ParenthesizeByPrecedence(finalReplacedBody);
                // Console.WriteLine($"Text after parenthesizing by precedence: {str}");
                // baştaki ve sondaki parantezleri kaldır
                if (str.StartsWith("(") && str.EndsWith(")"))
                {
                    str = str.Substring(1, str.Length - 2).Trim();
                }

                // strdeki $ ile başlayan yerler exprListteki karşılığı ile değiştirilecek
                for (int i = 0; i < finalExprList.Count; i++)
                {
                    var repl = finalExprList[i].Trim();
                    if (!IsSingleConditionFilter(repl))
                    {
                        // Zaten dıştan tam saran parantez yoksa ekle
                        if (!(repl.StartsWith("(") && repl.EndsWith(")")))
                            repl = "(" + repl + ")";
                    }
                    Console.WriteLine($"Replacing ${i} with: {repl}");
                    str = str.Replace($"${i}", repl);
                }
                // for (int i = 0; i < exprList.Count; i++)
                // {
                //     str = str.Replace($"${i}", exprList[i]);
                // }

                Console.WriteLine($"Text after replacing exprList in parenthesized string: {str}");
                var multiExprList = ExpressionSplitter.ExtractTopLevelOperands(ref str, out var multiReplacedBody);

                Console.WriteLine($"Text after final parsing: {multiReplacedBody} with {multiExprList.Count} expressions");
                Console.WriteLine($"Text after final expressions: {JsonSerializer.Serialize(multiExprList)}");
                

                finalExprList = multiExprList;
                finalReplacedBody = multiReplacedBody;
            }

            Console.WriteLine($"Body to parse: {body}");

            Console.WriteLine($"Text after final parsing: {finalReplacedBody} with {finalExprList.Count} expressions");
            Console.WriteLine($"Text after final expressions: {JsonSerializer.Serialize(finalExprList)}");
            


            // if finalReplacedBody is like $0 AND $1 OR $2
            var tokens = finalReplacedBody.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            // finalReplacedBody deki $ ile başlayan yerler exprListteki karşılığı ile değiştirilecek
            for (int i = 0; i < finalExprList.Count; i++)
            {
                tokens = tokens.Select(t => t == $"${i}" ? finalExprList[i] : t).ToArray();
            }

            // print tokens
            Console.WriteLine($"Tokens: {JsonSerializer.Serialize(tokens)}");

            var filter = new LogicalFilterModel();
            // tokens[1] should be AND or OR
            if (tokens.Length == 3 && (tokens[1].Equals("AND", StringComparison.OrdinalIgnoreCase) || tokens[1].Equals("OR", StringComparison.OrdinalIgnoreCase)))
            {
                filter.Operator = tokens[1].Equals("AND", StringComparison.OrdinalIgnoreCase) ? LogicalOperator.And : LogicalOperator.Or;

                filter.Left = BuildFilterModel(tokens[0])!;
                // Console.WriteLine($"Building filter for: {tokens[0]} - Text after building filter: {JsonSerializer.Serialize(filter.Left)}");

                filter.Right = BuildFilterModel(tokens[2])!;
                // Console.WriteLine($"Building filter for: {tokens[2]} - Text after building filter: {JsonSerializer.Serialize(filter.Right)}");

                // Console.WriteLine($"Text after building filter: {JsonSerializer.Serialize(filter)}");

                return filter;
            }
            else
            {
                throw new Exception($"Could not parse logical filter expression: {body} - Tokens: {JsonSerializer.Serialize(tokens)}");
            }
        }

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

            // Console.WriteLine($"Text after AND processing: {string.Join(" ", tokens)}");

            // 2. Şimdi kalan OR işlemlerini sırayla binary bağla
            while (tokens.Count > 1)
            {
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


        private ConditionFilterModel? ParseConditionFilter(string expr)
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

            // IS [NOT] NULL
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


            // genel operatorler (>=, <=, !=, <>, =, >, <, CONTAINS, BEGINSWITH, ENDSWITH, LIKE)
            var m = Regex.Match(s,
                @"^\s*(?<col>[\w\.\-]+)\s*(?<op>>=|<=|!=|<>|=|>|<|LIKE|CONTAINS|BEGINSWITH|ENDSWITH)\s*(?<rhs>.+?)\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success)
            {
                throw new Exception($"Could not parse condition filter expression: {expr}");
            }

            var col = m.Groups["col"].Value;
            var op = m.Groups["op"].Value.ToUpperInvariant();
            var rhsRaw = m.Groups["rhs"].Value.Trim();

            // Sağ taraf tek tırnaklıysa çıkar
            string rhsValue = rhsRaw;
            if (rhsRaw.StartsWith("'") && rhsRaw.EndsWith("'"))
            {
                rhsValue = Unquote(rhsRaw);
            }

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
                case "LIKE": comp = ComparisonOperator.Contains; break; // map LIKE -> Contains (handle in SQL builder)
                default: return null;
            }

            var columnWithPrefix = AddTablePrefixToColumn(col);

            return new ConditionFilterModel
            {
                Column = columnWithPrefix,
                Operator = comp,
                Value = rhsValue
            };
        }

        private List<string> ParseGroupBy(string queryString)
        {
            var m = Regex.Match(queryString, @"\bGROUPBY\s*\(\s*(.*?)\s*\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success) return new List<string>();

            var body = m.Groups[1].Value;

            var groupByColumns = body.Split(',').Select(c => c.Trim()).Where(c => !string.IsNullOrWhiteSpace(c)).ToList();

            for (int i = 0; i < groupByColumns.Count; i++)
            {
                groupByColumns[i] = AddTablePrefixToColumn(groupByColumns[i]);
            }

            return groupByColumns;
        }

        private List<OrderByModel> ParseOrderBy(string queryString)
        {
            var m = Regex.Match(queryString, @"\bORDERBY\s*\(\s*(.*?)\s*\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success) return new List<OrderByModel>();

            var body = m.Groups[1].Value;

            var orderByColumns = new List<OrderByModel>();
            var columns = body.Split(',').Where(c => !string.IsNullOrWhiteSpace(c)).Select(c => c.Trim()).ToList();

            foreach (var column in columns)
            {
                if (!string.IsNullOrWhiteSpace(column))
                {
                    var parts = column.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 0)
                    {
                        var columnWithPrefix = AddTablePrefixToColumn(parts[0]);
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

        private int ParseLimit(string queryString)
        {
            var m = Regex.Match(queryString, @"\bTAKE\s*\(\s*(\d+)\s*\)", RegexOptions.IgnoreCase);
            if (!m.Success) return 0;

            return int.TryParse(m.Groups[1].Value, out var limit) ? limit : 0;
        }

        private int ParseOffset(string queryString)
        {
            var m = Regex.Match(queryString, @"\bSKIP\s*\(\s*(\d+)\s*\)", RegexOptions.IgnoreCase);
            if (!m.Success) return 0;

            return int.TryParse(m.Groups[1].Value, out var offset) ? offset : 0;
        }

        private string GetIncludeQuery(string fromTable, string includeTable)
        {
            var type = _dbType.ToLower();
            if (type == "postgresql" || type == "postgres")
            {
                return $@"
                    SELECT kcu.column_name AS fk_column, ccu.column_name AS referenced_column
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = '{fromTable}'
                    AND ccu.table_name = '{includeTable}'";
            }
            else if (type == "mssql" || type == "sqlserver")
            {
                return $@"
                    SELECT cp.name AS fk_column, cr.name AS referenced_column
                    FROM sys.foreign_keys AS fk
                    INNER JOIN sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
                    INNER JOIN sys.tables AS tp ON fkc.parent_object_id = tp.object_id
                    INNER JOIN sys.columns AS cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
                    INNER JOIN sys.tables AS tr ON fkc.referenced_object_id = tr.object_id
                    INNER JOIN sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
                    WHERE tp.name = '{fromTable}' AND tr.name = '{includeTable}'";
            }
            else if (type == "mysql")
            {
                return $@"
                    SELECT k.COLUMN_NAME AS fk_column, k.REFERENCED_COLUMN_NAME AS referenced_column
                    FROM information_schema.KEY_COLUMN_USAGE k
                    WHERE k.TABLE_NAME = '{fromTable}'
                    AND k.REFERENCED_TABLE_NAME = '{includeTable}'
                    AND k.REFERENCED_COLUMN_NAME IS NOT NULL";
            }
            else if (type == "oracle")
            {
                return $@"
                    SELECT a.column_name AS fk_column, c_pk.column_name AS referenced_column
                    FROM user_cons_columns a
                    JOIN user_constraints c ON a.constraint_name = c.constraint_name
                    JOIN user_cons_columns c_pk ON c.r_constraint_name = c_pk.constraint_name
                    WHERE c.constraint_type = 'R'
                    AND a.table_name = '{fromTable.ToUpper()}'
                    AND c_pk.table_name = '{includeTable.ToUpper()}'";
            }
            else
            {
                throw new NotSupportedException($"Database type is not supported for INCLUDE queries.");
            }

        }
    }
}