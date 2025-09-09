using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using DynamicDbQueryApi.Data;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Interfaces;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.VisualStudio.TextTemplating;

namespace DynamicDbQueryApi.Services
{
    public class QueryService : IQueryService
    {
        public async Task<IEnumerable<dynamic>> SQLQueryAsync(QueryRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var connection = await context.GetOpenConnectionAsync();

            return await connection.QueryAsync(request.Query);
        }

        public async Task<IEnumerable<dynamic>> MyQueryAsync(QueryRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var dbType = request.DbType.ToLower();

            var connection = await context.GetOpenConnectionAsync();

            // var model = ParseQueryString(connection, request.DbType, request.Query);
            // Console.WriteLine("Generated Model: ", JsonSerializer.Serialize(model));

            var parser = new QueryParser(connection, dbType);
            var model = parser.Parse(request.Query);

            var sql = BuildSqlQuery(dbType, model);
            Console.WriteLine($"Generated SQL: {sql}");

            // return new List<dynamic> { model };

            return await connection.QueryAsync(sql);
        }

        public string ConvertFilterToSql(FilterModel filter)
        {
            if (filter == null) return "1=1"; // Filter yoksa her zaman true döner

            if (filter is ConditionFilterModel condition)
            {
                if (condition.Operator == ComparisonOperator.IsNull)
                {
                    return $"{condition.Column} IS NULL";
                }
                else if (condition.Operator == ComparisonOperator.IsNotNull)
                {
                    return $"{condition.Column} IS NOT NULL";
                }
                else if (condition.Value == null)
                {
                    throw new ArgumentException($"Value cannot be null for operator {condition.Operator}");
                }
                else if (condition.Operator == ComparisonOperator.Like ||
                         condition.Operator == ComparisonOperator.Contains ||
                         condition.Operator == ComparisonOperator.BeginsWith ||
                         condition.Operator == ComparisonOperator.EndsWith)
                {
                    string pattern = condition.Operator switch
                    {
                        ComparisonOperator.Like => $"%{condition.Value}%",
                        ComparisonOperator.Contains => $"%{condition.Value}%",
                        ComparisonOperator.BeginsWith => $"{condition.Value}%",
                        ComparisonOperator.EndsWith => $"%{condition.Value}",
                        _ => throw new NotSupportedException($"Unsupported operator {condition.Operator}")
                    };
                    return $"{condition.Column} LIKE '{pattern}'";
                }
                else
                {
                    string sqlOperator = condition.Operator switch
                    {
                        ComparisonOperator.Eq => "=",
                        ComparisonOperator.Neq => "!=",
                        ComparisonOperator.Lt => "<",
                        ComparisonOperator.Lte => "<=",
                        ComparisonOperator.Gt => ">",
                        ComparisonOperator.Gte => ">=",
                        _ => throw new NotSupportedException($"Unsupported operator {condition.Operator}")
                    };

                    // değer sayısal ise tırnak kullanma
                    if (double.TryParse(condition.Value, out _))
                    {
                        return $"{condition.Column} {sqlOperator} {condition.Value}";
                    }
                    else
                    {
                        return $"{condition.Column} {sqlOperator} '{condition.Value}'";
                    }
                }
            }
            else if (filter is LogicalFilterModel logical)
            {
                string left = ConvertFilterToSql(logical.Left);
                string right = ConvertFilterToSql(logical.Right);
                string op = logical.Operator.ToString().ToUpperInvariant();

                return $"({left} {op} {right})";
            }
            else
            {
                throw new NotSupportedException($"Filter type {filter.GetType()} is not supported.");
            }
        }


        public string BuildSqlQuery(string dbType, QueryModel queryModel)
        {
            // SELEFCT and FROM Part
            string columns = queryModel.Columns.Any() ? string.Join(", ", queryModel.Columns) : "*";
            string table = queryModel.Table;

            string sql = "";

            // DISTINCT Part
            if (queryModel.Distinct)
            {
                sql += "SELECT DISTINCT ";
            }
            else
            {
                sql += "SELECT ";
            }
            sql = $"{sql}{columns} FROM {table}";

            // JOINS Part
            if (queryModel.Includes.Any())
            {
                foreach (var include in queryModel.Includes)
                {
                    sql += $" {include.JoinType.ToUpper()} JOIN {include.IncludeTable} ON {include.Table}.{include.TableKey} = {include.IncludeTable}.{include.IncludeKey}";
                }
            }

            var text = FilterPrinter.Dump(queryModel.Filters);
            Console.WriteLine(text);

            // FILTERS Part
            if (queryModel.Filters != null)
            {
                var whereClause = ConvertFilterToSql(queryModel.Filters);
                sql += $" WHERE {whereClause}";
            }


            // GROUP BY Part
            if (queryModel.GroupBy.Any())
            {
                sql += " GROUP BY " + string.Join(", ", queryModel.GroupBy);
            }

            // ORDER BY Part
            if (queryModel.OrderBy.Any())
            {
                sql += " ORDER BY " + string.Join(", ", queryModel.OrderBy.Select(o => $"{o.Column} {(o.Desc ? "DESC" : "ASC")}"));
            }

            // LIMIT and OFFSET Part
            if (queryModel.Limit.HasValue && queryModel.Limit.Value > 0)
            {
                if (dbType == "postgresql" || dbType == "postgres")
                {
                    sql += $" LIMIT {queryModel.Limit.Value} OFFSET {(queryModel.Offset.HasValue ? queryModel.Offset.Value : 0)}";
                }
                else if (dbType == "mysql")
                {
                    sql += $" LIMIT {queryModel.Limit.Value} OFFSET {(queryModel.Offset.HasValue ? queryModel.Offset.Value : 0)}";
                }
                else if ((dbType == "mssql" || dbType == "sqlserver") && !queryModel.OrderBy.Any())
                {
                    sql += $" OFFSET {(queryModel.Offset.HasValue ? queryModel.Offset.Value : 0)} ROWS FETCH NEXT {queryModel.Limit.Value} ROWS ONLY";
                }
                else if ((dbType == "oracle") && queryModel.OrderBy.Any())
                {
                    sql += $" OFFSET {(queryModel.Offset.HasValue ? queryModel.Offset.Value : 0)} ROWS FETCH NEXT {queryModel.Limit.Value} ROWS ONLY";
                }
            }

            return sql;
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