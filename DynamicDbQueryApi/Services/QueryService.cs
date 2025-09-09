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
            // 1) Tabloları al
            string tablesSql = type switch
            {
                "postgresql" or "postgres" => @"
                    SELECT table_name AS TableName
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'",
                "mssql" or "sqlserver" => @"
                    SELECT TABLE_NAME AS TableName
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'",
                "mysql" => @"
                    SELECT TABLE_NAME AS TableName
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_SCHEMA = DATABASE()",
                "oracle" => @"
                    SELECT table_name AS TableName
                    FROM user_tables",
                _ => throw new NotSupportedException($"Database type {request.DbType} is not supported for inspection.")
            };

            var tableNames = (await connection.QueryAsync<string>(tablesSql)).ToList();

            // 2) Her tablo için kolonları çek
            // Ortak dönen yapı: { table: string, columns: [ { name, dataType, isNullable } ] }
            var result = new List<object>();

            foreach (var table in tableNames)
            {
                string columnsSql = type switch
                {
                    "postgresql" or "postgres" => @"
                        SELECT column_name   AS Name,
                               data_type     AS DataType,
                               is_nullable   AS IsNullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = @Table", 
                    "mssql" or "sqlserver" => @"
                        SELECT COLUMN_NAME   AS Name,
                               DATA_TYPE     AS DataType,
                               IS_NULLABLE   AS IsNullable
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = @Table", 
                    "mysql" => @"
                        SELECT COLUMN_NAME   AS Name,
                               DATA_TYPE     AS DataType,
                               IS_NULLABLE   AS IsNullable
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @Table", 
                    "oracle" => @"
                        SELECT column_name   AS Name,
                               data_type     AS DataType,
                               nullable      AS IsNullable
                        FROM user_tab_columns
                        WHERE table_name = :Table", 
                    _ => throw new NotSupportedException($"Database type {request.DbType} is not supported for inspection.")
                };

                IEnumerable<dynamic> columns;
                if (type == "oracle")
                {
                    // Oracle'da parametre :Table (büyük harf) tablo isimleri genelde uppercase tutulur
                    columns = await connection.QueryAsync(columnsSql, new { Table = table.ToUpperInvariant() });
                }
                else
                {
                    columns = await connection.QueryAsync(columnsSql, new { Table = table });
                }

                result.Add(new
                {
                    table = table,
                    columns = columns
                });
            }
            // 3) Tablolar arası ilişkiler (Foreign Key) -> relations list
            string relationsSql = type switch
            {
                "postgresql" or "postgres" => @"
                    SELECT 
                        tc.constraint_name AS constraint_name,
                        kcu.table_name     AS fk_table,
                        kcu.column_name    AS fk_column,
                        ccu.table_name     AS pk_table,
                        ccu.column_name    AS pk_column
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name = tc.constraint_name
                     AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_schema = 'public'", 
                "mssql" or "sqlserver" => @"
                    SELECT 
                        fk.name              AS constraint_name,
                        tf.name              AS fk_table,
                        cf.name              AS fk_column,
                        tp.name              AS pk_table,
                        cp.name              AS pk_column
                    FROM sys.foreign_keys fk
                    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                    INNER JOIN sys.tables tf ON fkc.parent_object_id = tf.object_id
                    INNER JOIN sys.columns cf ON fkc.parent_object_id = cf.object_id AND fkc.parent_column_id = cf.column_id
                    INNER JOIN sys.tables tp ON fkc.referenced_object_id = tp.object_id
                    INNER JOIN sys.columns cp ON fkc.referenced_object_id = cp.object_id AND fkc.referenced_column_id = cp.column_id", 
                "mysql" => @"
                    SELECT 
                        rc.CONSTRAINT_NAME      AS constraint_name,
                        kcu.TABLE_NAME          AS fk_table,
                        kcu.COLUMN_NAME         AS fk_column,
                        kcu.REFERENCED_TABLE_NAME AS pk_table,
                        kcu.REFERENCED_COLUMN_NAME AS pk_column
                    FROM information_schema.KEY_COLUMN_USAGE kcu
                    JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                      ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                     AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
                    WHERE kcu.CONSTRAINT_SCHEMA = DATABASE()
                      AND kcu.REFERENCED_TABLE_NAME IS NOT NULL", 
                "oracle" => @"
                    SELECT 
                        ac.constraint_name        AS constraint_name,
                        aco.table_name            AS fk_table,
                        aco.column_name           AS fk_column,
                        ac_r.table_name           AS pk_table,
                        acc.column_name           AS pk_column
                    FROM user_constraints ac
                    JOIN user_cons_columns aco ON ac.constraint_name = aco.constraint_name
                    JOIN user_constraints ac_r ON ac.r_constraint_name = ac_r.constraint_name
                    JOIN user_cons_columns acc ON ac_r.constraint_name = acc.constraint_name AND acc.position = aco.position
                    WHERE ac.constraint_type = 'R'", 
                _ => null!
            };

            var relationsRaw = string.IsNullOrWhiteSpace(relationsSql)
                ? new List<dynamic>()
                : (await connection.QueryAsync(relationsSql)).ToList();

            // Çok kolonlu FK'ler için constraint_name + fk_table + pk_table ile grupla
            var relations = relationsRaw
                .GroupBy(r => new { constraint = (string)r.constraint_name, fk = (string)r.fk_table, pk = (string)r.pk_table })
                .Select(g => new
                {
                    constraint = g.Key.constraint,
                    foreignTable = g.Key.fk,
                    primaryTable = g.Key.pk,
                    foreignColumns = g.Select(x => (string)x.fk_column).Distinct().ToList(),
                    primaryColumns = g.Select(x => (string)x.pk_column).Distinct().ToList()
                })
                .OrderBy(x => x.foreignTable)
                .ThenBy(x => x.constraint)
                .ToList();

            // İstersen her tabloya inbound/outbound eklemek için hızlı index oluştur
            var byTable = tableNames.ToDictionary(t => t, t => new { inbound = new List<object>(), outbound = new List<object>() });
            foreach (var rel in relations)
            {
                if (byTable.TryGetValue(rel.foreignTable, out var fkSide))
                {
                    fkSide.outbound.Add(rel);
                }
                if (byTable.TryGetValue(rel.primaryTable, out var pkSide))
                {
                    pkSide.inbound.Add(rel);
                }
            }

            // Tablo listesinde kolonlara ek olarak inbound/outbound relation özetleri
            var enrichedTables = result.Select(t =>
            {
                var tableProp = t.GetType().GetProperty("table");
                var colsProp = t.GetType().GetProperty("columns");
                var tableName = tableProp?.GetValue(t) as string ?? string.Empty;
                var colsVal = colsProp?.GetValue(t);
                if (!byTable.TryGetValue(tableName, out var relInfo))
                {
                    relInfo = new { inbound = new List<object>(), outbound = new List<object>() };
                }
                return new
                {
                    table = tableName,
                    columns = colsVal,
                    inboundRelations = relInfo.inbound,
                    outboundRelations = relInfo.outbound
                };
            }).ToList();

            // Geriye tek bir obje döndürüyoruz (interface IEnumerable olduğu için liste sarmalaması)
            var payload = new List<dynamic>
            {
                new {
                    tables = enrichedTables,
                    relations = relations
                }
            };

            return payload;
        }
    }
}