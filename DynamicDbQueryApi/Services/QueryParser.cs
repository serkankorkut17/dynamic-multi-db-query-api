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

namespace DynamicDbQueryApi.Services
{
    public class QueryParser : IQueryParser
    {
        private readonly IDbConnection _connection;
        private readonly string _dbType;
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

            // FETCH Part
            queryModel.Columns = ParseFetchColumns(queryModel.Table, queryString);

            // DISTINCT Part
            queryModel.Distinct = queryString.Contains("FETCHD", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCHDISTINCT", StringComparison.OrdinalIgnoreCase) || queryString.Contains("FETCH DISTINCT", StringComparison.OrdinalIgnoreCase);

            // INCLUDE Part
            queryModel.Includes = ParseIncludes(queryModel.Table, queryString);

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

        public List<string> ParseFetchColumns(string table, string queryString)
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
            Console.WriteLine($"Text: {JsonSerializer.Serialize(columns)}");

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                // split sonra her parçayı trimle
                if (!string.IsNullOrWhiteSpace(column) && column != "*")
                {
                    var parts = column.Split('.');
                    // Eğer tablo ismi yoksa ekle
                    if (parts.Length == 1)
                    {
                        var qualifiedColumn = $"{table}.{parts[0]}";
                        columns[i] = qualifiedColumn;
                    }
                    // eğer birden fazla . varsa sadece son kısmı al
                    else if (parts.Length > 1)
                    {
                        var qualifiedColumn = $"{parts[parts.Length - 2]}.{parts[parts.Length - 1]}";
                        columns[i] = qualifiedColumn;
                    }
                }
            }

            return columns;
        }

        public string ParseFromTable(string queryString)
        {
            var m = Regex.Match(queryString, @"\bFROM\s*\(?\s*(?<table>[\w\d\._\-]+)\s*\)?", RegexOptions.IgnoreCase);
            if (!m.Success) return string.Empty;

            var table = m.Groups["table"].Value.Trim();

            return table;
        }

        public ForeignKeyPair? GetIncludeKeys(string table, string includeTable)
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

        public List<IncludeModel> ParseIncludes(string table, string queryString)
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

        public FilterModel ParseFilters(string queryString)
        {
            var filtersStart = queryString.IndexOf("FILTER(") + 7;
            var filtersEnd = queryString.IndexOf(")", filtersStart);
            var filtersPart = queryString.Substring(filtersStart, filtersEnd - filtersStart).Trim();

            return new LogicalFilterModel
            {
                LogicalOperator = "AND",
                Filters = new List<FilterModel>()
            };

            // foreach (var column in columns)
            // {
            //     if (!string.IsNullOrWhiteSpace(column))
            //     {
            //         // Eğer !=, = <, >, <=, >= varsa ondan öncesini al
            //         var operators = new[] { "==", "!=", ">=", "<=", "=", "<", ">" };
            //         var op = operators.FirstOrDefault(o => column.Contains(o));

            //         if (op != null)
            //         {
            //             // == to = for SQL compatibility
            //             var normalizedOp = op == "==" ? "=" : op;
            //             var parts = column.Split(new[] { op }, StringSplitOptions.None);
            //             if (parts.Length == 2)
            //             {
            //                 var filter = new FilterModel
            //                 {
            //                     Column = parts[0].Trim(),
            //                     Operator = op,
            //                     Value = parts[1].Trim().Trim('\'')
            //                 };
            //                 queryModel.Columns.Add(filter.Column);
            //                 queryModel.Filters.Add(filter);
            //             }
            //         }
            //         else
            //         {
            //             queryModel.Columns.Add(column);
            //         }

            //     }
            // }

        }

        public List<string> ParseGroupBy(string queryString)
        {
            var m = Regex.Match(queryString, @"\bGROUPBY\s*\(\s*(.*?)\s*\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!m.Success) return new List<string>();

            var body = m.Groups[1].Value;

            var groupByColumns = body.Split(',').Select(c => c.Trim()).Where(c => !string.IsNullOrWhiteSpace(c)).ToList();

            return groupByColumns;
        }

        public List<OrderByModel> ParseOrderBy(string queryString)
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
                        var orderByModel = new OrderByModel
                        {
                            Column = parts[0],
                            Desc = parts.Length > 1 && parts[1].Equals("DESC", StringComparison.OrdinalIgnoreCase)
                        };
                        orderByColumns.Add(orderByModel);
                    }
                }
            }

            return orderByColumns;
        }

        public int ParseLimit(string queryString)
        {
            var m = Regex.Match(queryString, @"\bTAKE\s*\(\s*(\d+)\s*\)", RegexOptions.IgnoreCase);
            if (!m.Success) return 0;

            return int.TryParse(m.Groups[1].Value, out var limit) ? limit : 0;
        }

        public int ParseOffset(string queryString)
        {
            var m = Regex.Match(queryString, @"\bSKIP\s*\(\s*(\d+)\s*\)", RegexOptions.IgnoreCase);
            if (!m.Success) return 0;

            return int.TryParse(m.Groups[1].Value, out var offset) ? offset : 0;
        }

        public string GetIncludeQuery(string fromTable, string includeTable)
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