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
        public async Task<IEnumerable<dynamic>> QueryAsync(QueryRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var connection = await context.GetOpenConnectionAsync();

            // var model = ParseQueryString(connection, request.DbType, request.Query);
            // Console.WriteLine("Generated Model: ", JsonSerializer.Serialize(model));

            var parser = new QueryParser(connection, request.DbType);
            var model = parser.Parse(request.Query);

            // var sql = BuildSqlQuery(model);
            // Console.WriteLine($"Generated SQL: {sql}");

            return new List<dynamic> { model };

            // return await connection.QueryAsync(sql);
        }

        public string BuildSqlQuery(QueryModel queryModel)
        {
            string columns = queryModel.Columns.Any() ? string.Join(", ", queryModel.Columns) : "*";
            string table = queryModel.Table;
            string sql = $"SELECT {columns} FROM {table}";

            // INCLUDE Part
            // if (!string.IsNullOrWhiteSpace(queryModel.Include.Table) &&
            //     !string.IsNullOrWhiteSpace(queryModel.Include.ForeignKey) &&
            //     !string.IsNullOrWhiteSpace(queryModel.Include.ReferencedKey))
            // {
            //     sql += $" LEFT JOIN {queryModel.Include.Table} ON {queryModel.Table}.{queryModel.Include.ForeignKey} = {queryModel.Include.Table}.{queryModel.Include.ReferencedKey}";
            // }

            // FILTERS Part
            // if (queryModel.Filters.Any())
            // {
            //     List<string> filters = new List<string>();
            //     foreach (var filter in queryModel.Filters)
            //     {
            //         string value = filter.Value;
            //         if (!double.TryParse(value, out _))
            //         {
            //             value = $"'{value}'";
            //         }
            //         filters.Add($"{filter.Column} {filter.Operator} {value}");
            //     }
            //     sql += " WHERE " + string.Join(" AND ", filters);
            // }

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