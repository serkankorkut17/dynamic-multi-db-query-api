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
        public async Task<QueryResultDTO> SQLQueryAsync(QueryRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);
            var sql = request.Query;

            var connection = await context.GetOpenConnectionAsync();

            // Eğer connection null ise hata fırlat
            if (connection == null)
            {
                throw new Exception("Could not open database connection. Please check the connection string and database type.");
            }

            var data = await connection.QueryAsync(sql);

            bool writtenToOutputDb = false;

            // Output db bilgilerini ayarla
            if (request.WriteToOutputDb)
            {
                if (string.IsNullOrEmpty(request.OutputDbType) ||
                    string.IsNullOrEmpty(request.OutputConnectionString) ||
                    string.IsNullOrEmpty(request.OutputTableName))
                {
                    throw new Exception("OutputDbType, OutputConnectionString and OutputTableName must be provided to write to output database.");
                }

                var outputContext = new DapperContext(request.OutputDbType, request.OutputConnectionString);
                var outputDbType = request.OutputDbType.ToLower();
                var outputTableName = request.OutputTableName;
                var outputConnection = await outputContext.GetOpenConnectionAsync();

                await SaveDataToOutputDb(outputDbType, outputConnection, outputTableName, data);
                writtenToOutputDb = true;
            }

            // bağlantıyı kapat
            connection.Close();
            return new QueryResultDTO
            {
                Sql = sql,
                Data = data,
                WrittenToOutputDb = writtenToOutputDb
            };
        }

        // Özel query dilini parse edip SQL'e çevirir ve çalıştırır
        public async Task<QueryResultDTO> MyQueryAsync(QueryRequestDTO request)
        {
            // _logger.LogInformation("Query: {Query}", request.Query);

            var context = new DapperContext(request.DbType, request.ConnectionString);

            var dbType = request.DbType.ToLower();

            var connection = await context.GetOpenConnectionAsync();

            // Eğer connection null ise hata fırlat
            if (connection == null)
            {
                throw new Exception("Could not open database connection. Please check the connection string and database type.");
            }

            // Input stringini QueryModel'e çevir
            var model = _queryParserService.Parse(request.Query);

            // İlişkili tablolardaki foreign keyleri bul ve IncludeModel'leri güncelle
            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };
            _logger.LogInformation("Initial Model: {Model}", JsonSerializer.Serialize(model, options));
            var filtersText = FilterPrinter.Dump(model.Filters);
            Console.WriteLine("Filters:\n" + filtersText);

            var havingText = FilterPrinter.Dump(model.Having);
            Console.WriteLine("Having:\n" + havingText);

            foreach (var include in model.Includes)
            {
                // Eğer queryde tablo anahtarları belirtilmişse atla
                if (include.TableKey != null && include.IncludeKey != null)
                {
                    continue;
                }
                
                var updatedInclude = await UpdateIncludeModel(connection, dbType, include);
                // _logger.LogInformation("Updated Include: {Include}", JsonSerializer.Serialize(updatedInclude));

                if (updatedInclude != null)
                {
                    include.TableKey = updatedInclude.TableKey;
                    include.IncludeKey = updatedInclude.IncludeKey;
                }
            }
            // _logger.LogInformation("Generated Model: {Model}", JsonSerializer.Serialize(model, options));

            var sql = _sqlBuilderService.BuildSqlQuery(dbType, model);
            _logger.LogInformation("Generated SQL: {Sql}", sql);

            // return new QueryResultDTO
            // {
            //     Sql = sql,
            //     Data = new List<dynamic>(),
            //     WrittenToOutputDb = false
            // };

            var data = await connection.QueryAsync(sql);

            bool writtenToOutputDb = false;

            // Output db bilgilerini ayarla
            if (request.WriteToOutputDb)
            {
                if (string.IsNullOrEmpty(request.OutputDbType) ||
                    string.IsNullOrEmpty(request.OutputConnectionString) ||
                    string.IsNullOrEmpty(request.OutputTableName))
                {
                    throw new Exception("OutputDbType, OutputConnectionString and OutputTableName must be provided to write to output database.");
                }

                var outputContext = new DapperContext(request.OutputDbType, request.OutputConnectionString);
                var outputDbType = request.OutputDbType.ToLower();
                var outputTableName = request.OutputTableName;
                var outputConnection = await outputContext.GetOpenConnectionAsync();

                try
                {
                    await SaveDataToOutputDb(outputDbType, outputConnection, outputTableName, data);
                    writtenToOutputDb = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error saving data to output database");

                }
                outputConnection.Close();
            }

            // bağlantıyı kapat
            connection.Close();
            return new QueryResultDTO
            {
                Sql = sql,
                Data = data,
                WrittenToOutputDb = writtenToOutputDb
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
        public async Task<string> IsTableAndColumnsExistAsync(IDbConnection connection, string dbType, string tableName, Dictionary<string, string> columnDataTypes)
        {
            // Tablo var mı kontrolü
            bool tableExists = await _queryRepo.TableExistsAsync(connection, dbType, tableName);

            if (!tableExists)
            {
                // Eğer tablo yoksa CREATE TABLE ile oluştur
                return _sqlBuilderService.BuildCreateTableSql(dbType, tableName, columnDataTypes);
            }
            else
            {
                // Eğer tablo varsa eksik kolonları ALTER TABLE ile ekle
                var sqlBuilder = new StringBuilder();
                foreach (var col in columnDataTypes)
                {
                    string colName = col.Key;
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

        public Dictionary<string, string> GetColumnDataTypes(string outputDbType, IEnumerable<dynamic> data)
        {
            // Datayı listeye çevir
            var dataList = data?.ToList() ?? new List<dynamic>();
            var columnNames = new List<string>();
            if (dataList.Count > 0)
            {
                // İlk satırı IDictionary gibi cast et
                var firstRow = (IDictionary<string, object>)dataList[0];
                columnNames = firstRow.Keys.ToList();
            }

            // Column - DataType dictionary al
            var columnDataTypes = new Dictionary<string, string>();

            foreach (string col in columnNames)
            {
                bool isBoolean = true;
                for (int i = 0; i < dataList.Count; i++)
                {
                    var row = (IDictionary<string, object>)dataList[i];
                    if (row.ContainsKey(col))
                    {
                        // Eğer değer null ise atla
                        if (row[col] == null)
                        {
                            continue;
                        }

                        // Eğer değer bool ise db'ye göre uygun tipe çevir
                        if (row[col] is bool boolValue)
                        {
                            if (outputDbType == "postgres" || outputDbType == "postgresql")
                            {
                                columnDataTypes[col] = "BOOLEAN";
                            }
                            else if (outputDbType == "mssql" || outputDbType == "mssql")
                            {
                                columnDataTypes[col] = "BIT";
                            }
                            else if (outputDbType == "mysql" || outputDbType == "mysql")
                            {
                                columnDataTypes[col] = "TINYINT";
                            }
                            else if (outputDbType == "oracle" || outputDbType == "oracle")
                            {
                                columnDataTypes[col] = "NUMBER(1)";
                            }
                            else
                            {
                                columnDataTypes[col] = "BOOLEAN";
                            }
                            break;
                        }

                        // Eğer değer 1 veya 0 ise bool olarak değerlendir
                        if (isBoolean && row[col] is int intValue && (intValue == 0 || intValue == 1))
                        {
                            if (i == dataList.Count - 1)
                            {
                                if (outputDbType == "postgres" || outputDbType == "postgresql")
                                {
                                    columnDataTypes[col] = "BOOLEAN";
                                }
                                else if (outputDbType == "mssql" || outputDbType == "mssql")
                                {
                                    columnDataTypes[col] = "BIT";
                                }
                                else if (outputDbType == "mysql" || outputDbType == "mysql")
                                {
                                    columnDataTypes[col] = "TINYINT";
                                }
                                else if (outputDbType == "oracle" || outputDbType == "oracle")
                                {
                                    columnDataTypes[col] = "NUMBER(1)";
                                }
                                else
                                {
                                    columnDataTypes[col] = "BOOLEAN";
                                }
                            }
                            continue;
                        }
                        else
                        {
                            isBoolean = false;
                        }

                        // Eğer değer tam sayı ise db'ye göre uygun tipe çevir
                        if (row[col] is int || row[col] is long || row[col] is short || row[col] is byte)
                        {
                            if (outputDbType == "postgres" || outputDbType == "postgresql")
                            {
                                columnDataTypes[col] = "BIGINT";
                            }
                            else if (outputDbType == "mssql" || outputDbType == "mssql")
                            {
                                columnDataTypes[col] = "BIGINT";
                            }
                            else if (outputDbType == "mysql" || outputDbType == "mysql")
                            {
                                columnDataTypes[col] = "BIGINT";
                            }
                            else if (outputDbType == "oracle" || outputDbType == "oracle")
                            {
                                columnDataTypes[col] = "NUMBER(19)";
                            }
                            else
                            {
                                columnDataTypes[col] = "INTEGER";
                            }
                            break;
                        }

                        // Eğer System.UInt32 ise db'ye göre uygun tipe çevir
                        else if (row[col] is uint || row[col] is ulong || row[col] is ushort)
                        {
                            if (outputDbType == "postgres" || outputDbType == "postgresql")
                            {
                                columnDataTypes[col] = "BIGINT";
                            }
                            else if (outputDbType == "mssql" || outputDbType == "mssql")
                            {
                                columnDataTypes[col] = "BIGINT";
                            }
                            else if (outputDbType == "mysql" || outputDbType == "mysql")
                            {
                                columnDataTypes[col] = "BIGINT";
                            }
                            else if (outputDbType == "oracle" || outputDbType == "oracle")
                            {
                                columnDataTypes[col] = "NUMBER(19)";
                            }
                            else
                            {
                                columnDataTypes[col] = "INTEGER";
                            }
                            break;
                        }

                        // Eğer ondalıklı sayı ise db'ye göre uygun tipe çevir
                        else if (row[col] is float || row[col] is double || row[col] is decimal)
                        {
                            if (outputDbType == "postgres" || outputDbType == "postgresql")
                            {
                                columnDataTypes[col] = "DOUBLE PRECISION";
                            }
                            else if (outputDbType == "mssql" || outputDbType == "mssql")
                            {
                                columnDataTypes[col] = "FLOAT(53)";
                            }
                            else if (outputDbType == "mysql" || outputDbType == "mysql")
                            {
                                columnDataTypes[col] = "DOUBLE";
                            }
                            else if (outputDbType == "oracle" || outputDbType == "oracle")
                            {
                                columnDataTypes[col] = "BINARY_DOUBLE";
                            }
                            else
                            {
                                columnDataTypes[col] = "DOUBLE";
                            }
                            break;
                        }

                        // Eğer timestamp ise db'ye göre uygun tipe çevir
                        else if (row[col] is DateTime)
                        {
                            if (outputDbType == "postgres" || outputDbType == "postgresql")
                            {
                                columnDataTypes[col] = "TIMESTAMP";
                            }
                            else if (outputDbType == "mssql" || outputDbType == "mssql")
                            {
                                columnDataTypes[col] = "DATETIME2";
                            }
                            else if (outputDbType == "mysql" || outputDbType == "mysql")
                            {
                                columnDataTypes[col] = "DATETIME";
                            }
                            else if (outputDbType == "oracle" || outputDbType == "oracle")
                            {
                                columnDataTypes[col] = "TIMESTAMP";
                            }
                            else
                            {
                                columnDataTypes[col] = "TIMESTAMP";
                            }
                            break;
                        }

                        // Eğer saat ise db'ye göre uygun tipe çevir
                        else if (row[col] is TimeSpan)
                        {
                            if (outputDbType == "postgres" || outputDbType == "postgresql")
                            {
                                columnDataTypes[col] = "TIME";
                            }
                            else if (outputDbType == "mssql" || outputDbType == "mssql")
                            {
                                columnDataTypes[col] = "TIME";
                            }
                            else if (outputDbType == "mysql" || outputDbType == "mysql")
                            {
                                columnDataTypes[col] = "TIME";
                            }
                            else if (outputDbType == "oracle" || outputDbType == "oracle")
                            {
                                columnDataTypes[col] = "TIMESTAMP";
                            }
                            else
                            {
                                columnDataTypes[col] = "TIME";
                            }
                            break;
                        }

                        // Eğer json ise db'ye göre uygun tipe çevir
                        else if (row[col] is JsonElement || row[col] is IDictionary<string, object> || row[col] is IList<object>)
                        {
                            if (outputDbType == "postgres" || outputDbType == "postgresql")
                            {
                                columnDataTypes[col] = "JSONB";
                            }
                            else if (outputDbType == "mssql" || outputDbType == "mssql")
                            {
                                columnDataTypes[col] = "NVARCHAR(MAX)";
                            }
                            else if (outputDbType == "mysql" || outputDbType == "mysql")
                            {
                                columnDataTypes[col] = "JSON";
                            }
                            else if (outputDbType == "oracle" || outputDbType == "oracle")
                            {
                                columnDataTypes[col] = "CLOB";
                            }
                            else
                            {
                                columnDataTypes[col] = "TEXT";
                            }
                            break;
                        }
                        // Eğer string ise db'ye göre uygun tipe çevir
                        else
                        {
                            if (outputDbType == "postgres" || outputDbType == "postgresql")
                            {
                                columnDataTypes[col] = "TEXT";
                            }
                            else if (outputDbType == "mssql" || outputDbType == "mssql")
                            {
                                columnDataTypes[col] = "TEXT";
                            }
                            else if (outputDbType == "mysql" || outputDbType == "mysql")
                            {
                                columnDataTypes[col] = "TEXT";
                            }
                            else if (outputDbType == "oracle" || outputDbType == "oracle")
                            {
                                columnDataTypes[col] = "CLOB";
                            }
                            else
                            {
                                columnDataTypes[col] = "TEXT";
                            }
                        }
                    }
                }
            }
            return columnDataTypes;
        }

        public async Task SaveDataToOutputDb(string outputDbType, IDbConnection outputConnection, string outputTableName, IEnumerable<dynamic> data)
        {
            var columnDataTypes = GetColumnDataTypes(outputDbType, data);

            // Log column data types
            _logger.LogInformation("Column Data Types for Output DB: {ColumnDataTypes}", JsonSerializer.Serialize(columnDataTypes));

            string ensureSql = await IsTableAndColumnsExistAsync(outputConnection,
                outputDbType,
                outputTableName,
                columnDataTypes);

            _logger.LogInformation("Ensure Table and Columns SQL: {Sql}", ensureSql);

            // Eğer tablo yoksa oluştur
            if (!string.IsNullOrEmpty(ensureSql))
            {
                // Oracle: Tek komut yürütmede sondaki ; hata oluşturur
                if (string.Equals(outputDbType, "oracle", StringComparison.OrdinalIgnoreCase))
                {
                    ensureSql = Regex.Replace(ensureSql, @";\s*$", "");
                }
                await outputConnection.ExecuteAsync(ensureSql);
            }

            var targetColumns = columnDataTypes.Keys.ToList();

            string Param(int i) => string.Equals(outputDbType, "oracle", StringComparison.OrdinalIgnoreCase) ? $":p{i}" : $"@p{i}";
            string Q(string name) => string.Equals(outputDbType, "oracle", StringComparison.OrdinalIgnoreCase) ? $"\"{name.ToUpperInvariant()}\"" : name;

            var columnsSql = string.Join(", ", targetColumns.Select(Q));
            var valuesSql = string.Join(", ", targetColumns.Select((c, i) => Param(i)));
            var insertSql = $"INSERT INTO {Q(outputTableName)} ({columnsSql}) VALUES ({valuesSql})";

            _logger.LogInformation("Insert SQL: {Sql}", insertSql);

            // Datadaki her bir satırı insert et
            foreach (IDictionary<string, object> row in data)
            {
                var dp = new DynamicParameters();
                for (int i = 0; i < targetColumns.Count; i++)
                {
                    var key = targetColumns[i];
                    row.TryGetValue(key, out var val);

                    // mysql uint tipleri için int64 kullan
                    if (val is uint || val is ulong || val is ushort || val is byte)
                    {
                        val = Convert.ToInt64(val);
                    }

                    // Oracle'da boolean için 1/0 kullan
                    if (string.Equals(outputDbType, "oracle", StringComparison.OrdinalIgnoreCase))
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

            // Bağlantıyı döngü dışında kapatın
            outputConnection.Close();
        }
    }
}