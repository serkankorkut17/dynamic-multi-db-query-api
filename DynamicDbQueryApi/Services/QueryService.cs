using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dapper;
using DynamicDbQueryApi.Data;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;
using DynamicDbQueryApi.Interfaces;
using Humanizer;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.VisualStudio.TextTemplating;
using Serilog;

namespace DynamicDbQueryApi.Services
{
    public class QueryService : IQueryService
    {
        private readonly ILogger<QueryService> _logger;
        private readonly IQueryParserService _queryParserService;
        private readonly ISqlBuilderService _sqlBuilderService;
        private readonly IQueryRepository _queryRepo;
        private readonly ISchemaSqlProvider _schemaSqlProvider;

        public QueryService(ILogger<QueryService> logger, IQueryParserService queryParserService, ISqlBuilderService sqlBuilderService, IQueryRepository queryRepo, ISchemaSqlProvider schemaSqlProvider)
        {
            _logger = logger;
            _queryParserService = queryParserService;
            _sqlBuilderService = sqlBuilderService;
            _queryRepo = queryRepo;
            _schemaSqlProvider = schemaSqlProvider;
        }

        // Düz SQL sorgusu çalıştırır
        public async Task<IEnumerable<dynamic>> SQLQueryAsync(QueryRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var connection = await context.GetOpenConnectionAsync();

            return await connection.QueryAsync(request.Query);
        }

        // Özel query dilini parse edip SQL'e çevirir ve çalıştırır
        // !!! db save bak
        public async Task<QueryResultDTO> MyQueryAsync(QueryRequestDTO request)
        {
            // _logger.LogInformation("Query: {Query}", request.Query);

            var context = new DapperContext(request.DbType, request.ConnectionString);

            var dbType = request.DbType.ToLower();

            var connection = await context.GetOpenConnectionAsync();

            // Input stringini QueryModel'e çevir
            var model = _queryParserService.Parse(request.Query);

            // İlişkili tablolardaki foreign keyleri bul ve IncludeModel'leri güncelle
            _logger.LogInformation("Initial Model: {Model}", JsonSerializer.Serialize(model));
            foreach (var include in model.Includes)
            {
                var updatedInclude = await UpdateIncludeModel(connection, dbType, include);
                // _logger.LogInformation("Updated Include: {Include}", JsonSerializer.Serialize(updatedInclude));

                if (updatedInclude != null)
                {
                    include.TableKey = updatedInclude.TableKey;
                    include.IncludeKey = updatedInclude.IncludeKey;
                }
            }
            // _logger.LogInformation("Generated Model: {Model}", JsonSerializer.Serialize(model));

            var sql = _sqlBuilderService.BuildSqlQuery(dbType, model);
            // _logger.LogInformation("Generated SQL: {Sql}", sql);

            var data = await connection.QueryAsync(sql);
            data = data.ToList();

            // Output db bilgilerini ayarla
            if (!string.IsNullOrEmpty(request.OutputDbType) &&
                !string.IsNullOrEmpty(request.OutputConnectionString) &&
                !string.IsNullOrEmpty(request.OutputTableName))
            {
                Dictionary<string, string> columnDataTypes = await GetColumnDataTypesAsync(connection, dbType, model.Columns);
                _logger.LogInformation("Column Data Types: {ColumnDataTypes}", JsonSerializer.Serialize(columnDataTypes));

                string ensureSql = await EnsureTableAndColumnsAsync(connection,
                    request.OutputDbType.ToLower(),
                    request.OutputTableName,
                    columnDataTypes);

                _logger.LogInformation("Ensure Table and Columns SQL: {Sql}", ensureSql);

                // Eğer tablo yoksa oluştur
                if (!string.IsNullOrEmpty(ensureSql))
                {
                    await connection.ExecuteAsync(ensureSql);
                }

                // datadaki her bir satırı insert et
                foreach (IDictionary<string, object> row in data)
                {
                    _logger.LogInformation($"Row: {JsonSerializer.Serialize(row)}");

                    var insertSql = new StringBuilder();
                    insertSql.Append($"INSERT INTO {request.OutputTableName} (");
                    insertSql.Append(string.Join(", ", columnDataTypes.Keys.Select(c => c.Replace(".", "_"))));



                    insertSql.Append(") VALUES (");
                    insertSql.Append(string.Join(", ", row.Values.Select(v => $"'{v}'")));
                    insertSql.Append(");");

                    _logger.LogInformation("Insert SQL: {Sql}", insertSql.ToString());
                    await connection.ExecuteAsync(insertSql.ToString());
                }

            }

            return new QueryResultDTO
            {
                Sql = sql,
                Data = data
            };
        }

        // IncludeModel'deki tablo isimlerine göre foreign keyleri bulur ve IncludeModel'i günceller
        public async Task<IncludeModel?> UpdateIncludeModel(IDbConnection connection, string dbType, IncludeModel include)
        {
            if (include == null) return null;

            var fromTable = include.Table;
            var includeTable = include.IncludeTable;
            var pairs = await _queryRepo.GetForeignKeyPairAsync(connection, dbType, fromTable, includeTable);
            _logger.LogInformation("Try 1st ForeignKeyPair: {ForeignKeyPair}", JsonSerializer.Serialize(pairs));
            if (pairs != null)
            {
                return new IncludeModel
                {
                    Table = include.Table,
                    TableKey = pairs.ForeignKey,
                    IncludeTable = include.IncludeTable,
                    IncludeKey = pairs.ReferencedKey,
                    JoinType = include.JoinType
                };
            }
            else
            {
                pairs = await _queryRepo.GetForeignKeyPairAsync(connection, dbType, includeTable, fromTable);
                _logger.LogInformation("Try 2nd ForeignKeyPair: {ForeignKeyPair}", JsonSerializer.Serialize(pairs));

                if (pairs != null)
                {
                    return new IncludeModel
                    {
                        Table = include.Table,
                        TableKey = pairs.ReferencedKey,
                        IncludeTable = include.IncludeTable,
                        IncludeKey = pairs.ForeignKey,
                        JoinType = include.JoinType
                    };
                }
                else
                {
                    throw new Exception($"Could not find foreign key relationship between {fromTable} and {includeTable}");
                }
            }
        }

        // Veritabanını inceleyip tabloları ve ilişkileri döner
        public async Task<InspectResponseDTO> InspectDatabaseAsync(InspectRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var connection = await context.GetOpenConnectionAsync();

            var type = request.DbType.ToLower();

            // 1) Tabloları ve kolonları al
            var tables = await _queryRepo.GetTablesAndColumnsAsync(connection, type);

            // 3) Tablolar arası ilişkileri çek
            var relations = await _queryRepo.GetRelationshipsAsync(connection, type);

            // Tablolara ilişkileri ekle
            foreach (var table in tables)
            {
                foreach (var rel in relations)
                {
                    if (rel.FkTable == table.Table)
                    {
                        table.Relationships.Add(new TableRelationship
                        {
                            Key = rel.FkColumn,
                            RelationTable = rel.PkTable,
                            RelationKey = rel.PkColumn
                        });
                    }
                    else if (rel.PkTable == table.Table)
                    {
                        table.Relationships.Add(new TableRelationship
                        {
                            Key = rel.PkColumn,
                            RelationTable = rel.FkTable,
                            RelationKey = rel.FkColumn
                        });
                    }
                }
            }

            return new InspectResponseDTO
            {
                Tables = tables,
                Relationships = relations
            };
        }

        // Belirtilen tablo ve kolonlar için hedef veritabanında tabloyu ve eksik kolonları oluşturur
        // !!! kontrol et
        public async Task<string> EnsureTableAndColumnsAsync(IDbConnection connection, string dbType, string tableName, Dictionary<string, string> columnDataTypes)
        {
            // Tablo var mı kontrolü
            bool tableExists;
            var getTableSql = string.Empty;

            if (dbType == "postgresql" || dbType == "postgres")
            {
                getTableSql = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{tableName}'";
                tableExists = await connection.ExecuteScalarAsync<int>(getTableSql) > 0;
            }
            else if (dbType == "mssql")
            {
                getTableSql = $"SELECT COUNT(*) FROM sys.tables WHERE name = '{tableName}'";
                tableExists = await connection.ExecuteScalarAsync<int>(getTableSql) > 0;
            }
            else if (dbType == "mysql")
            {
                getTableSql = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{tableName}'";
                tableExists = await connection.ExecuteScalarAsync<int>(getTableSql) > 0;
            }
            else if (dbType == "oracle")
            {
                getTableSql = $"SELECT COUNT(*) FROM all_tables WHERE table_name = '{tableName.ToUpperInvariant()}'";
                tableExists = await connection.ExecuteScalarAsync<int>(getTableSql) > 0;
            }
            else
            {
                throw new NotSupportedException($"Unsupported DB type: {dbType}");
            }

            var sqlBuilder = new StringBuilder();
            _logger.LogInformation("Table Exists: {TableExists}", tableExists);

            if (!tableExists)
            {
                // Eğer tablo yoksa CREATE TABLE ile oluştur
                sqlBuilder.Append($"CREATE TABLE {tableName} (\n");
                int i = 0;
                foreach (var col in columnDataTypes)
                {
                    string colName = col.Key.Replace(".", "_");
                    string colType = ConvertDataType(col.Value, dbType);
                    sqlBuilder.Append($"    {colName} {colType} NULL");
                    if (i < columnDataTypes.Count - 1)
                    {
                        sqlBuilder.Append(",\n");
                    }
                    i++;
                }
                sqlBuilder.Append("\n);");
            }
            else
            {
                // Eğer tablo varsa eksik kolonları ALTER TABLE ile ekle
                foreach (var col in columnDataTypes)
                {
                    string colName = col.Key.Replace(".", "_");
                    bool columnExists;
                    string colCheckSql = string.Empty;
                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        colCheckSql = $"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{tableName}' AND column_name = '{colName}'";
                        columnExists = await connection.ExecuteScalarAsync<int>(colCheckSql) > 0;
                    }
                    else if (dbType == "mssql")
                    {
                        colCheckSql = $"SELECT COUNT(*) FROM sys.columns c JOIN sys.tables t ON c.object_id = t.object_id WHERE t.name = '{tableName}' AND c.name = '{colName}'";
                        columnExists = await connection.ExecuteScalarAsync<int>(colCheckSql) > 0;
                    }
                    else if (dbType == "mysql")
                    {
                        colCheckSql = $"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '{tableName}' AND column_name = '{colName}'";
                        columnExists = await connection.ExecuteScalarAsync<int>(colCheckSql) > 0;
                    }
                    else if (dbType == "oracle")
                    {
                        colCheckSql = $"SELECT COUNT(*) FROM all_tab_columns WHERE table_name = '{tableName.ToUpperInvariant()}' AND column_name = '{colName.ToUpperInvariant()}'";
                        columnExists = await connection.ExecuteScalarAsync<int>(colCheckSql) > 0;
                    }
                    else
                    {
                        throw new NotSupportedException($"Unsupported DB type: {dbType}");
                    }

                    if (!columnExists)
                    {
                        if (dbType == "oracle")
                        {
                            sqlBuilder.AppendLine($"ALTER TABLE {tableName.ToUpperInvariant()} ADD ({colName.ToUpperInvariant()} {col.Value} NULL);");
                        }
                        else
                        {
                            sqlBuilder.AppendLine(
                                $"ALTER TABLE {tableName} ADD {colName} {col.Value} NULL;");
                        }
                    }
                }
            }

            return sqlBuilder.ToString();
        }

        public async Task<Dictionary<string, string>> GetColumnDataTypesAsync(IDbConnection connection, string dbType, List<string> columns)
        {
            // column: datatype için dictionary
            var columnDataTypes = new Dictionary<string, string>();

            foreach (var col in columns)
            {
                var parts = col.Split('.');
                if (parts.Length == 2)
                {
                    var tableName = parts[0];
                    var columnName = parts[1];

                    var dataType = await _queryRepo.GetColumnDataTypeAsync(connection, dbType, tableName, columnName);

                    if (dataType != null)
                    {
                        columnDataTypes[col] = dataType.ToString();
                    }
                    else
                    {
                        throw new Exception($"Could not determine data type for column {col} in table {tableName}");
                    }
                }

                else if (parts.Length == 1)
                {
                    // COUNT, SUM, AVG, MIN, MAX gibi aggregate fonksiyonlar için
                    var func = parts[0].ToUpper();
                    if (func == "COUNT")
                    {
                        switch (dbType)
                        {
                            case "postgresql":
                            case "mysql":
                                columnDataTypes[col] = "INTEGER";
                                break;
                            case "mssql":
                                columnDataTypes[col] = "INT";
                                break;
                            case "oracle":
                                columnDataTypes[col] = "NUMBER(10)";
                                break;
                            default:
                                throw new NotSupportedException($"Unsupported DB type: {dbType}");
                        }
                    }
                    else if (func == "SUM" || func == "AVG" || func == "MIN" || func == "MAX")
                    {
                        switch (dbType)
                        {
                            case "postgresql":
                            case "mysql":
                                columnDataTypes[col] = "DECIMAL";
                                break;
                            case "mssql":
                                columnDataTypes[col] = "DECIMAL(18,2)";
                                break;
                            case "oracle":
                                columnDataTypes[col] = "NUMBER(18,2)";
                                break;
                            default:
                                throw new NotSupportedException($"Unsupported DB type: {dbType}");
                        }
                    }
                    else
                    {
                        throw new Exception($"Could not determine data type for aggregate function {col}");
                    }
                }
                else
                {
                    throw new Exception($"Invalid column format: {col}");
                }
            }
            return columnDataTypes;
        }

        public Dictionary<string, string> ConvertColumnDataTypes(Dictionary<string, string> columnDataTypes, string toDbType)
        {
            var converted = new Dictionary<string, string>();

            foreach (var kvp in columnDataTypes)
            {
                var col = kvp.Key;
                var fromType = kvp.Value;
                var toType = ConvertDataType(fromType, toDbType);
                converted[col] = toType;
            }

            return converted;
        }

        public record ParsedType(string BaseType, int? Length, int? Precision, int? Scale);

        private static ParsedType ParseType(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return new ParsedType(raw ?? "", null, null, null);

            var s = raw.Trim().ToLowerInvariant();
            // match varchar(255) or numeric(10,2) or number(10,2)
            var m = Regex.Match(s, @"^(?<base>[a-z0-9_ ]+?)(\s*\(\s*(?<p>\d+)(\s*,\s*(?<s>\d+))?\s*\))?$");
            if (!m.Success) return new ParsedType(s, null, null, null);

            var baseType = m.Groups["base"].Value.Trim();
            int? p = null, sc = null;
            if (int.TryParse(m.Groups["p"].Value, out var pi)) p = pi;
            if (int.TryParse(m.Groups["s"].Value, out var si)) sc = si;

            // length vs precision: for varchar => Length = p, for numeric => Precision=p, Scale=sc
            if (baseType.Contains("char") || baseType.Contains("text") || baseType.Contains("clob"))
                return new ParsedType(baseType, p, null, null);
            return new ParsedType(baseType, null, p, sc);
        }

        public string ConvertDataType(string dataType, string toDbType)
        {
            var parsed = ParseType(dataType);
            var baseType = parsed.BaseType;
            var precision = parsed.Precision;
            var scale = parsed.Scale;
            var length = parsed.Length;

            string UseLen(int? len, int fallback) => len.HasValue && len.Value > 0 ? len.Value.ToString() : fallback.ToString();
            string UsePrec(int? pr, int fallback) => pr.HasValue && pr.Value > 0 ? pr.Value.ToString() : fallback.ToString();
            string UseScale(int? sc, int fallback) => sc.HasValue ? sc.Value.ToString() : fallback.ToString();

            toDbType = toDbType.ToLowerInvariant();
            baseType = baseType.ToLowerInvariant();

            if (toDbType == "postgresql" || toDbType == "postgres")
            {
                if (baseType.Contains("int")) return "INTEGER";
                if (baseType.Contains("bigint")) return "BIGINT";
                if (baseType.Contains("smallint") || baseType.Contains("tinyint")) return "SMALLINT";
                if (baseType.Contains("numeric") || baseType.Contains("decimal") || baseType.Contains("number"))
                    return $"NUMERIC({UsePrec(precision, 18)},{UseScale(scale, 2)})";
                if (baseType.Contains("float") || baseType.Contains("double")) return "DOUBLE PRECISION";
                if (baseType.Contains("char") || baseType.Contains("varchar"))
                    return $"VARCHAR({UseLen(length, 255)})";
                if (baseType.Contains("text") || baseType.Contains("clob")) return "TEXT";
                if (baseType.Contains("bool") || baseType.Contains("boolean")) return "BOOLEAN";
                if (baseType.Contains("date") && baseType.Contains("time")) return "TIMESTAMP";
                if (baseType.Contains("date")) return "DATE";
                return "VARCHAR(255)";
            }
            else if (toDbType == "mssql")
            {
                if (baseType.Contains("int")) return "INT";
                if (baseType.Contains("bigint")) return "BIGINT";
                if (baseType.Contains("smallint") || baseType.Contains("tinyint")) return "SMALLINT";
                if (baseType.Contains("numeric") || baseType.Contains("decimal") || baseType.Contains("number"))
                    return $"DECIMAL({UsePrec(precision, 18)},{UseScale(scale, 2)})";
                if (baseType.Contains("float") || baseType.Contains("double")) return "FLOAT";
                if (baseType.Contains("char") || baseType.Contains("varchar"))
                    return $"VARCHAR({UseLen(length, 255)})";
                if (baseType.Contains("text") || baseType.Contains("clob")) return "NVARCHAR(MAX)";
                if (baseType.Contains("bool") || baseType.Contains("boolean")) return "BIT";
                if (baseType.Contains("date") && baseType.Contains("time")) return "DATETIME2";
                if (baseType.Contains("date")) return "DATE";
                return "NVARCHAR(255)";
            }
            else if (toDbType == "mysql")
            {
                if (baseType.Contains("int")) return "INT";
                if (baseType.Contains("bigint")) return "BIGINT";
                if (baseType.Contains("smallint") || baseType.Contains("tinyint")) return "SMALLINT";
                if (baseType.Contains("numeric") || baseType.Contains("decimal") || baseType.Contains("number"))
                    return $"DECIMAL({UsePrec(precision, 18)},{UseScale(scale, 2)})";
                if (baseType.Contains("float") || baseType.Contains("double")) return "DOUBLE";
                if (baseType.Contains("char") || baseType.Contains("varchar"))
                    return $"VARCHAR({UseLen(length, 255)})";
                if (baseType.Contains("text") || baseType.Contains("clob")) return "TEXT";
                if (baseType.Contains("bool") || baseType.Contains("boolean")) return "TINYINT(1)";
                if (baseType.Contains("date") && baseType.Contains("time")) return "DATETIME";
                if (baseType.Contains("date")) return "DATE";
                return "VARCHAR(255)";
            }
            else if (toDbType == "oracle")
            {
                if (baseType.Contains("int") || baseType.Contains("integer")) return "NUMBER(10)";
                if (baseType.Contains("bigint")) return "NUMBER(19)";
                if (baseType.Contains("smallint") || baseType.Contains("tinyint")) return "NUMBER(5)";
                if (baseType.Contains("numeric") || baseType.Contains("decimal") || baseType.Contains("number"))
                    return $"NUMBER({UsePrec(precision, 18)},{UseScale(scale, 2)})";
                if (baseType.Contains("float") || baseType.Contains("double")) return "BINARY_FLOAT"; // or FLOAT(53)
                if (baseType.Contains("char") || baseType.Contains("varchar"))
                    return $"VARCHAR2({UseLen(length, 255)})";
                if (baseType.Contains("text") || baseType.Contains("clob")) return "CLOB";
                if (baseType.Contains("bool") || baseType.Contains("boolean")) return "NUMBER(1)";
                if (baseType.Contains("date") && baseType.Contains("time")) return "TIMESTAMP";
                if (baseType.Contains("date")) return "DATE";
                return "VARCHAR2(255)";
            }
            else
            {
                throw new NotSupportedException($"Unsupported target DB type: {toDbType}");
            }
        }
    }
}