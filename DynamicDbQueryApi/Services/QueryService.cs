using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Dapper;
using DynamicDbQueryApi.Data;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Interfaces;
using Microsoft.VisualStudio.TextTemplating;

namespace DynamicDbQueryApi.Services
{
    public class QueryService : IQueryService
    {
        public async Task<IEnumerable<dynamic>> QueryAsync(QueryRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var connection = await context.GetOpenConnectionAsync();

            var model = ParseQueryString(connection, request.DbType, request.Query);
            // Console.WriteLine("Generated Model: ", JsonSerializer.Serialize(model));

            var sql = BuildSqlQuery(model);
            Console.WriteLine($"Generated SQL: {sql}");

            // return new List<dynamic> { sql };

            return await connection.QueryAsync(sql);
        }

        public QueryModel ParseQueryString(IDbConnection connection, string dbType, string queryString)
        {
            var queryModel = new QueryModel();
            if (queryString.StartsWith("FETCH"))
            {
                // FETCH Part
                var fetchStart = queryString.IndexOf("FETCH(") + 6;
                var fetchEnd = queryString.IndexOf(")", fetchStart);
                var columnsPart = queryString.Substring(fetchStart, fetchEnd - fetchStart);

                var columns = columnsPart.Split(',').Select(c => c.Trim()).ToList();

                foreach (var column in columns)
                {
                    if (!string.IsNullOrWhiteSpace(column))
                    {
                        // Eğer !=, = <, >, <=, >= varsa ondan öncesini al
                        var operators = new[] { "==", "!=", ">=", "<=", "=", "<", ">" };
                        var op = operators.FirstOrDefault(o => column.Contains(o));

                        if (op != null)
                        {
                            // == to = for SQL compatibility
                            var normalizedOp = op == "==" ? "=" : op;
                            var parts = column.Split(new[] { op }, StringSplitOptions.None);
                            if (parts.Length == 2)
                            {
                                var filter = new FilterModel
                                {
                                    Column = parts[0].Trim(),
                                    Operator = op,
                                    Value = parts[1].Trim().Trim('\'')
                                };
                                queryModel.Columns.Add(filter.Column);
                                queryModel.Filters.Add(filter);
                            }
                        }
                        else
                        {
                            queryModel.Columns.Add(column);
                        }

                    }
                }

                // FROM Part
                var fromStart = queryString.IndexOf("FROM ") + 5;
                var fromEnd = queryString.IndexOf(" ", fromStart);
                if (fromEnd == -1) fromEnd = queryString.Length;
                queryModel.Table = queryString.Substring(fromStart, fromEnd - fromStart).Trim();

                // INCLUDE Part
                var includeIndex = queryString.IndexOf("INCLUDE ");
                if (includeIndex != -1)
                {
                    var includeStart = includeIndex + 8;
                    var includeEnd = queryString.IndexOf(" ", includeStart);
                    if (includeEnd == -1) includeEnd = queryString.Length;
                    queryModel.Include.Table = queryString.Substring(includeStart, includeEnd - includeStart).Trim();

                    var includeSql = GetIncludeQuery(dbType, queryModel.Table, queryModel.Include.Table);

                    var includeResult = connection.Query(includeSql).FirstOrDefault();


                    if (includeResult != null)
                    {
                        IDictionary<string, object>? dict = includeResult as IDictionary<string, object>;

                        queryModel.Include.ForeignKey = dict["fk_column"]?.ToString() ?? "";
                        queryModel.Include.ReferencedKey = dict["referenced_column"]?.ToString() ?? "";
                    }

                    if (!string.IsNullOrWhiteSpace(queryModel.Include.Table) &&
                        (string.IsNullOrWhiteSpace(queryModel.Include.ForeignKey) ||
                        string.IsNullOrWhiteSpace(queryModel.Include.ReferencedKey)))
                    {
                        throw new Exception($"Could not find foreign key relationship between {queryModel.Table} and {queryModel.Include.Table}");
                    }
                }

                // GROUP BY Part
                var groupByIndex = queryString.IndexOf("GROUP ");
                if (groupByIndex != -1)
                {
                    var groupByStart = groupByIndex + 6;
                    var groupByEnd = queryString.IndexOf(" ", groupByStart);
                    if (groupByEnd == -1) groupByEnd = queryString.Length;
                    queryModel.GroupBy = queryString.Substring(groupByStart, groupByEnd - groupByStart).Trim();
                }
            }
            return queryModel;
        }
        public string BuildSqlQuery(QueryModel queryModel)
        {
            string columns = queryModel.Columns.Any() ? string.Join(", ", queryModel.Columns) : "*";
            string table = queryModel.Table;
            string sql = $"SELECT {columns} FROM {table}";

            // INCLUDE Part
            if (!string.IsNullOrWhiteSpace(queryModel.Include.Table) &&
                !string.IsNullOrWhiteSpace(queryModel.Include.ForeignKey) &&
                !string.IsNullOrWhiteSpace(queryModel.Include.ReferencedKey))
            {
                sql += $" LEFT JOIN {queryModel.Include.Table} ON {queryModel.Table}.{queryModel.Include.ForeignKey} = {queryModel.Include.Table}.{queryModel.Include.ReferencedKey}";
            }

            // FILTERS Part
            if (queryModel.Filters.Any())
            {
                List<string> filters = new List<string>();
                foreach (var filter in queryModel.Filters)
                {
                    string value = filter.Value;
                    if (!double.TryParse(value, out _))
                    {
                        value = $"'{value}'";
                    }
                    filters.Add($"{filter.Column} {filter.Operator} {value}");
                }
                sql += " WHERE " + string.Join(" AND ", filters);
            }

            return sql;
        }

        public string GetIncludeQuery(string dbType, string fromTable, string includeTable)
        {
            var type = dbType.ToLower();
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

        public async Task<IEnumerable<dynamic>> InspectDatabaseAsync(QueryRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var connection = await context.GetOpenConnectionAsync();

            var type = request.DbType.ToLower();
            string sql = type switch
            {
                "postgresql" or "postgres" => @"
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE';",
                "mssql" or "sqlserver" => @"
                    SELECT TABLE_NAME AS table_name
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE';",
                "mysql" => @"
                    SELECT TABLE_NAME AS table_name
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_SCHEMA = DATABASE();",
                "oracle" => @"
                    SELECT table_name
                    FROM user_tables",
                _ => throw new NotSupportedException($"Database type {request.DbType} is not supported for inspection.")
            };

            return await connection.QueryAsync(sql);
        }
    }
}