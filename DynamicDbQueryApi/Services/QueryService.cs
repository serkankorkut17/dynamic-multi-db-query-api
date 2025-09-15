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
using DynamicDbQueryApi.Entities.Query;
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

        public QueryService(ILogger<QueryService> logger, IQueryParserService queryParserService, ISqlBuilderService sqlBuilderService, IQueryRepository queryRepo)
        {
            _logger = logger;
            _queryParserService = queryParserService;
            _sqlBuilderService = sqlBuilderService;
            _queryRepo = queryRepo;
        }

        // Düz SQL sorgusu çalıştırır
        public async Task<IEnumerable<dynamic>> SQLQueryAsync(QueryRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var connection = await context.GetOpenConnectionAsync();

            return await connection.QueryAsync(request.Query);
        }

        // Özel query dilini parse edip SQL'e çevirir ve çalıştırır
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
            var text = FilterPrinter.Dump(model.Filters);
            Console.WriteLine("Filters:\n" + text);

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
            _logger.LogInformation("Generated Model: {Model}", JsonSerializer.Serialize(model));

            var sql = _sqlBuilderService.BuildSqlQuery(dbType, model);
            _logger.LogInformation("Generated SQL: {Sql}", sql);

            var data = await connection.QueryAsync(sql);

            // Output db bilgilerini ayarla
            if (!string.IsNullOrEmpty(request.OutputDbType) &&
                !string.IsNullOrEmpty(request.OutputConnectionString) &&
                !string.IsNullOrEmpty(request.OutputTableName))
            {
                var outputContext = new DapperContext(request.OutputDbType, request.OutputConnectionString);
                var outputDbType = request.OutputDbType.ToLower();
                var outputTableName = request.OutputTableName;
                var outputConnection = await outputContext.GetOpenConnectionAsync();

                // Kolon aliaslarını al
                Dictionary<string, string> columnAliases = model.Columns.ToDictionary(c => c.Expression, c => c.Alias ?? c.Expression);
                // Input db'den kolonların data tiplerini al
                Dictionary<string, string> columnDataTypes = await GetColumnDataTypesAsync(connection, outputDbType, model.Columns);
                _logger.LogInformation("Column Data Types: {ColumnDataTypes}", JsonSerializer.Serialize(columnDataTypes));
                // Eğer hedef db tipi farklı ise kolonların data tiplerini dönüştür
                if (dbType != outputDbType)
                {
                    columnDataTypes = ConvertColumnDataTypes(columnDataTypes, outputDbType);
                    _logger.LogInformation("Converted Column Data Types: {ColumnDataTypes}", JsonSerializer.Serialize(columnDataTypes));
                }

                string ensureSql = await IsTableAndColumnsExistAsync(outputConnection,
                    outputDbType,
                    outputTableName,
                    columnDataTypes,
                    columnAliases);

                _logger.LogInformation("Ensure Table and Columns SQL: {Sql}", ensureSql);

                // Eğer tablo yoksa oluştur
                if (!string.IsNullOrEmpty(ensureSql))
                {
                    await outputConnection.ExecuteAsync(ensureSql);
                }

                var targetColumns = columnAliases.Values.ToList();

                var insertSql = $"INSERT INTO {outputTableName} ({string.Join(", ", targetColumns)}) VALUES ({string.Join(", ", targetColumns.Select((c, i) => $"@p{i}"))})";

                // Datadaki her bir satırı insert et
                foreach (IDictionary<string, object> row in data)
                {
                    var dp = new DynamicParameters();
                    for (int i = 0; i < targetColumns.Count; i++)
                    {
                        var key = targetColumns[i];
                        row.TryGetValue(key, out var val);

                        // Oracle'da boolean için 1/0 kullan
                        if (outputDbType == "oracle")
                        {
                            if (val is bool b)
                            {
                                dp.Add($"p{i}", b ? 1 : 0);
                            }
                            else if (val is string s && (s.Equals("true", StringComparison.OrdinalIgnoreCase) || s.Equals("false", StringComparison.OrdinalIgnoreCase)))
                            {
                                dp.Add($"p{i}", s.Equals("true", StringComparison.OrdinalIgnoreCase) ? 1 : 0);
                            }
                            else
                            {
                                dp.Add($"p{i}", val ?? DBNull.Value);
                            }
                        }
                        else
                        {
                            dp.Add($"p{i}", val ?? DBNull.Value);
                        }
                    }

                    await outputConnection.ExecuteAsync(insertSql, dp);
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
        public async Task<string> IsTableAndColumnsExistAsync(IDbConnection connection, string dbType, string tableName, Dictionary<string, string> columnDataTypes, Dictionary<string, string> columnAliases)
        {
            // Tablo var mı kontrolü
            bool tableExists = await _queryRepo.TableExistsAsync(connection, dbType, tableName);

            if (!tableExists)
            {
                // Eğer tablo yoksa CREATE TABLE ile oluştur
                return _sqlBuilderService.BuildCreateTableSql(tableName, columnDataTypes, columnAliases);
            }
            else
            {
                // Eğer tablo varsa eksik kolonları ALTER TABLE ile ekle
                var sqlBuilder = new StringBuilder();
                foreach (var col in columnDataTypes)
                {
                    string colName = columnAliases[col.Key];
                    string colType = col.Value;
                    bool columnExists = await _queryRepo.ColumnExistsInTableAsync(connection, dbType, tableName, colName);

                    if (!columnExists)
                    {
                        sqlBuilder.AppendLine(_sqlBuilderService.BuildAlterTableAddColumnSql(dbType, tableName, colName, colType));
                    }
                }
                return sqlBuilder.ToString();
            }
        }

        public async Task<Dictionary<string, string>> GetColumnDataTypesAsync(IDbConnection connection, string dbType, List<QueryColumnModel> columns)
        {
            // column: datatype için dictionary
            var columnDataTypes = new Dictionary<string, string>();

            foreach (var col in columns)
            {
                string colName = col.Expression;
                var parts = col.Expression.Split('.');
                if (parts.Length == 2)
                {
                    var tableName = parts[0];
                    var columnName = parts[1];

                    var dataType = await _queryRepo.GetColumnDataTypeAsync(connection, dbType, tableName, columnName);

                    if (dataType != null)
                    {
                        columnDataTypes[colName] = dataType.ToString();
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
                    if (func.Contains("COUNT"))
                    {
                        switch (dbType)
                        {
                            case "postgresql":
                            case "mysql":
                                columnDataTypes[colName] = "INTEGER";
                                break;
                            case "mssql":
                                columnDataTypes[colName] = "INT";
                                break;
                            case "oracle":
                                columnDataTypes[colName] = "NUMBER(10)";
                                break;
                            default:
                                throw new NotSupportedException($"Unsupported DB type: {dbType}");
                        }
                    }
                    else if (func.Contains("SUM") || func.Contains("AVG") || func.Contains("MIN") || func.Contains("MAX"))
                    {
                        switch (dbType)
                        {
                            case "postgresql":
                            case "mysql":
                                columnDataTypes[colName] = "DECIMAL";
                                break;
                            case "mssql":
                                columnDataTypes[colName] = "DECIMAL(18,2)";
                                break;
                            case "oracle":
                                columnDataTypes[colName] = "NUMBER(18,2)";
                                break;
                            default:
                                throw new NotSupportedException($"Unsupported DB type: {dbType}");
                        }
                    }
                    else
                    {
                        throw new Exception($"Could not determine data type for aggregate function {colName}");
                    }
                }
                else
                {
                    throw new Exception($"Invalid column format: {colName}");
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