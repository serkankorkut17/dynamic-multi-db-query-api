using System;
using System.ClientModel.Primitives;
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
using DynamicDbQueryApi.Helpers;
using DynamicDbQueryApi.Interfaces;
using Humanizer;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.VisualStudio.TextTemplating;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace DynamicDbQueryApi.Services
{
    public class QueryService : IQueryService
    {
        private readonly ILogger<QueryService> _logger;
        private readonly IQueryParserService _queryParserService;
        private readonly ISqlBuilderService _sqlBuilderService;
        private readonly IMongoPipelineBuilderService _mongoDbService;
        private readonly IInMemoryQueryService _inMemoryQueryService;
        private readonly IQueryRepository _queryRepo;

        public QueryService(ILogger<QueryService> logger, IQueryParserService queryParserService, ISqlBuilderService sqlBuilderService, IMongoPipelineBuilderService mongoDbService, IInMemoryQueryService inMemoryQueryService, IQueryRepository queryRepo)
        {
            _logger = logger;
            _queryParserService = queryParserService;
            _sqlBuilderService = sqlBuilderService;
            _mongoDbService = mongoDbService;
            _inMemoryQueryService = inMemoryQueryService;
            _queryRepo = queryRepo;
        }

        // Bütün db sorgularını çalıştırır (Postegre, MySQL, MSSQL, Oracle, MongoDB)
        public async Task<QueryResultDTO> AllQueryAsync(QueryRequestDTO request)
        {

            string dbType = request.DbType.ToLower();
            string ConnectionString = request.ConnectionString;
            string Query = request.Query;

            IEnumerable<dynamic> data = Array.Empty<dynamic>();
            string dslTranslated = "";
            if (dbType == "mongodb" || dbType == "mongo")
            {
                (dslTranslated, data) = await QueryFromMongoDbAsync(ConnectionString, Query);
            }
            else if (dbType == "postgres" || dbType == "postgresql" || dbType == "mysql" || dbType == "mssql" || dbType == "oracle")
            {
                (dslTranslated, data) = await QueryFromSqlDbAsync(dbType, ConnectionString, Query);
            }
            else if (dbType == "api")
            {
                (dslTranslated, data) = await QueryFromApiAsync(ConnectionString, Query);
            }
            else
            {
                throw new NotSupportedException($"Database type '{request.DbType}' is not supported.");
            }

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

                var outputDbType = request.OutputDbType.ToLower();
                var outputConnectionString = request.OutputConnectionString;
                var outputTableName = request.OutputTableName;

                if (outputDbType == "mongodb" || outputDbType == "mongo")
                {
                    await SaveDataToOutputMongoDb(outputConnectionString, outputTableName, data);
                    writtenToOutputDb = true;
                }
                else if (outputDbType == "postgres" || outputDbType == "postgresql" || outputDbType == "mysql" || outputDbType == "mssql" || outputDbType == "oracle")
                {
                    await SaveDataToOutputSqlDb(outputDbType, outputConnectionString, outputTableName, data);
                    writtenToOutputDb = true;
                }
                else
                {
                    throw new NotSupportedException($"Output database type '{request.OutputDbType}' is not supported.");
                }
            }

            return new QueryResultDTO
            {
                Sql = dslTranslated,
                Data = data,
                WrittenToOutputDb = writtenToOutputDb
            };
        }

        // MongoDB sorgusu çalıştırır
        public async Task<QueryResultDTO> MongoQueryAsync(QueryRequestDTO request)
        {
            // Input stringini QueryModel'e çevir
            var model = _queryParserService.Parse(request.Query);
            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };
            Console.WriteLine($"Model: {JsonSerializer.Serialize(model, options)}");

            var pipeline = _mongoDbService.BuildPipeline(model);
            Console.WriteLine("Pipeline:");
            foreach (var stage in pipeline)
            {
                Console.WriteLine(stage.ToJson());
            }

            // MongoDB bağlantısı oluştur
            var mongoCtx = new MongoContext(request.ConnectionString, null);
            var db = mongoCtx.GetDatabase();
            var collectionName = model.Table;

            // Aggregate komutunu çalıştır
            var command = new BsonDocument
            {
                { "aggregate", collectionName },
                { "pipeline", new BsonArray(pipeline) },
                { "collation", new BsonDocument { { "locale", "tr" }, { "strength", 1 } } },
                { "cursor", new BsonDocument() }
            };

            var cmdResult = await db.RunCommandAsync<BsonDocument>(command);

            // Sonuçları al
            var firstBatch = cmdResult.GetValue("cursor").AsBsonDocument.GetValue("firstBatch").AsBsonArray
                .Select(v => v.AsBsonDocument)
                .ToList();

            // Sonuçları düz .NET nesnelerine (Dictionary) dönüştür
            List<dynamic> data = firstBatch.Select(d => (dynamic)BsonHelpers.BsonDocumentToDictionary(d)).ToList();

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

                var outputContext = new MongoContext(request.OutputConnectionString, null);
                var outputDb = outputContext.GetDatabase();
                var outputCollection = outputDb.GetCollection<BsonDocument>(request.OutputTableName);
                var bsonDocs = firstBatch; // Zaten BsonDocument listesi
                if (bsonDocs.Count > 0)
                {
                    await outputCollection.InsertManyAsync(bsonDocs);
                    writtenToOutputDb = true;
                }

            }

            return new QueryResultDTO
            {
                Sql = "MongoDB Pipeline\n" + string.Join("\n", pipeline.Select(s => s.ToJson())),
                Data = data,
                WrittenToOutputDb = writtenToOutputDb
            };
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

                var outputDbType = request.OutputDbType.ToLower();
                var outputConnectionString = request.OutputConnectionString;
                var outputTableName = request.OutputTableName;

                await SaveDataToOutputSqlDb(outputDbType, outputConnectionString, outputTableName, data);
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

                var outputDbType = request.OutputDbType.ToLower();
                var outputConnectionString = request.OutputConnectionString;
                var outputTableName = request.OutputTableName;

                try
                {
                    await SaveDataToOutputSqlDb(outputDbType, outputConnectionString, outputTableName, data);
                    writtenToOutputDb = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error saving data to output database");

                }
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

        public async Task<InspectResponseDTO> InspectMongoDbAsync(InspectRequestDTO request)
        {
            var response = new InspectResponseDTO();
            var ctx = new MongoContext(request.ConnectionString, null);
            var db = ctx.GetDatabase();

            // Get all collection names
            var collectionNames = (await db.ListCollectionNamesAsync()).ToList();

            foreach (var collectionName in collectionNames)
            {
                var collection = db.GetCollection<BsonDocument>(collectionName);

                // İlk 100 dokümanı örnek al
                var sample = await collection.Find(new BsonDocument()).Limit(100).ToListAsync();

                var fieldSet = new HashSet<string>();
                var fieldTypes = new Dictionary<string, BsonType>();

                foreach (var doc in sample)
                {
                    foreach (var elem in doc.Elements)
                    {
                        fieldSet.Add(elem.Name);

                        if (!fieldTypes.ContainsKey(elem.Name))
                        {
                            fieldTypes[elem.Name] = elem.Value.BsonType;
                        }
                    }
                }

                var columns = fieldSet.Select(fieldName => new ColumnModel
                {
                    Name = fieldName,
                    DataType = BsonHelpers.MapBsonTypeToSqlType(fieldTypes.GetValueOrDefault(fieldName, BsonType.String)),
                    IsNullable = true,
                }).ToList();

                response.Tables.Add(new TableModel
                {
                    Table = collectionName,
                    Columns = columns
                });
            }

            return response;
        }

        // API endpoint'ini inceleyip şemayı döner
        public async Task<InspectResponseDTO> InspectApiAsync(InspectRequestDTO request)
        {
            var response = new InspectResponseDTO();

            try
            {
                // API'den veri çek
                using var httpClient = new HttpClient();
                var apiResponse = await httpClient.GetAsync(request.ConnectionString);
                apiResponse.EnsureSuccessStatusCode();
                var jsonString = await apiResponse.Content.ReadAsStringAsync();

                // JSON'u parse et
                List<dynamic> apiData;
                using (JsonDocument doc = JsonDocument.Parse(jsonString))
                {
                    if (doc.RootElement.ValueKind == JsonValueKind.Array)
                    {
                        apiData = doc.RootElement.EnumerateArray()
                            .Select(e => JsonSerializer.Deserialize<Dictionary<string, object>>(e.GetRawText())!)
                            .Cast<dynamic>()
                            .ToList();
                    }
                    else if (doc.RootElement.ValueKind == JsonValueKind.Object)
                    {
                        // Nesne içinde array içeren ilk property'yi ara
                        JsonElement arrayElement = default;
                        string? arrayPropertyName = null;

                        foreach (var property in doc.RootElement.EnumerateObject())
                        {
                            if (property.Value.ValueKind == JsonValueKind.Array)
                            {
                                arrayElement = property.Value;
                                arrayPropertyName = property.Name;
                                break;
                            }
                        }

                        if (arrayElement.ValueKind != JsonValueKind.Undefined)
                        {
                            _logger.LogInformation($"Found array property '{arrayPropertyName}' in response root object");
                            apiData = arrayElement.EnumerateArray()
                                .Select(e => JsonSerializer.Deserialize<Dictionary<string, object>>(e.GetRawText())!)
                                .Cast<dynamic>()
                                .ToList();
                        }
                        else
                        {
                            // Hiç array bulunamadı, nesnenin kendisini kullan
                            apiData = new List<dynamic>
                            {
                                JsonSerializer.Deserialize<Dictionary<string, object>>(jsonString)!
                            };
                        }
                    }
                    else
                    {
                        apiData = new List<dynamic>
                        {
                            JsonSerializer.Deserialize<Dictionary<string, object>>(jsonString)!
                        };
                    }
                }

                if (apiData.Count == 0)
                {
                    _logger.LogWarning("API returned empty data, cannot infer schema");
                    return response;
                }

                // Şemayı çıkar - ilk birkaç kaydı kullanarak
                var sampleSize = Math.Min(10, apiData.Count);
                var samples = apiData.Take(sampleSize).ToList();

                // Tüm alanları topla
                var allFields = new Dictionary<string, HashSet<string>>();

                foreach (var sample in samples)
                {
                    var dict = (IDictionary<string, object>)sample;
                    foreach (var kvp in dict)
                    {
                        if (!allFields.ContainsKey(kvp.Key))
                        {
                            allFields[kvp.Key] = new HashSet<string>();
                        }

                        // Değerin tipini belirle
                        string dataType = InferDataType(kvp.Value);
                        allFields[kvp.Key].Add(dataType);
                    }
                }

                // TableModel oluştur (API endpoint'i "table" gibi davranır)
                var table = new TableModel
                {
                    Table = "api_data",
                    Columns = new List<ColumnModel>()
                };

                foreach (var field in allFields.OrderBy(f => f.Key))
                {
                    // Eğer birden fazla tip varsa, en genel tipi seç
                    string finalType = field.Value.Count > 1 ? "string" : field.Value.First();

                    table.Columns.Add(new ColumnModel
                    {
                        Name = field.Key,
                        DataType = finalType,
                        IsNullable = true // API verisinde her alan nullable kabul edilir
                    });
                }

                response.Tables.Add(table);
                _logger.LogInformation($"API inspection completed. Found {table.Columns.Count} fields in response data");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error inspecting API endpoint");
                throw;
            }

            return response;
        }

        // Değerin veri tipini çıkar
        private string InferDataType(object? value)
        {
            if (value == null) return "null";

            // JsonElement olabilir
            if (value is JsonElement je)
            {
                return je.ValueKind switch
                {
                    JsonValueKind.String => "string",
                    JsonValueKind.Number => "number",
                    JsonValueKind.True or JsonValueKind.False => "boolean",
                    JsonValueKind.Object => "object",
                    JsonValueKind.Array => "array",
                    JsonValueKind.Null => "null",
                    _ => "string"
                };
            }

            // .NET tipleri
            return value switch
            {
                string => "string",
                int or long or short or byte => "integer",
                float or double or decimal => "number",
                bool => "boolean",
                DateTime or DateTimeOffset => "date",
                _ => value.GetType().IsArray || value is System.Collections.IEnumerable ? "array" : "object"
            };
        }

        // Veritabanını inceleyip tabloları ve ilişkileri döner
        public async Task<InspectResponseDTO> InspectDatabaseAsync(InspectRequestDTO request)
        {
            if (request.DbType.ToLower() == "mongodb" || request.DbType.ToLower() == "mongo")
            {
                return await InspectMongoDbAsync(request);
            }
            else if (request.DbType.ToLower() == "api")
            {
                return await InspectApiAsync(request);
            }

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

        public async Task SaveDataToOutputSqlDb(string outputDbType, string outputConnectionString, string outputTableName, IEnumerable<dynamic> data)
        {
            var outputContext = new DapperContext(outputDbType, outputConnectionString);
            var outputConnection = await outputContext.GetOpenConnectionAsync();

            // JsonElement tiplerini dönüştür (ön işleme)
            var processedData = data.Select(row =>
            {
                var dict = (IDictionary<string, object>)row;
                var processedRow = new Dictionary<string, object?>();

                foreach (var kvp in dict)
                {
                    var value = kvp.Value;

                    // JsonElement'i uygun tipe dönüştür
                    if (value is JsonElement je)
                    {
                        value = je.ValueKind switch
                        {
                            JsonValueKind.String => je.GetString(),
                            JsonValueKind.Number => je.TryGetInt64(out long l) ? l : je.GetDouble(),
                            JsonValueKind.True => true,
                            JsonValueKind.False => false,
                            JsonValueKind.Null => null,
                            JsonValueKind.Object => je.GetRawText(), // JSON string olarak kaydet
                            JsonValueKind.Array => je.GetRawText(),  // JSON string olarak kaydet
                            _ => je.GetRawText()
                        };
                    }

                    processedRow[kvp.Key] = value;
                }

                return (dynamic)processedRow;
            }).ToList();

            var columnDataTypes = GetColumnDataTypes(outputDbType, processedData);

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
            foreach (IDictionary<string, object> row in processedData)
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
                            // NULL değerler için DbType belirt
                            if (val == null)
                            {
                                dp.Add($"p{i}", null, GetDbTypeForColumn(outputDbType, columnDataTypes[key]));
                            }
                            else
                            {
                                dp.Add($"p{i}", val);
                            }
                        }
                    }
                    else
                    {
                        // NULL değerler için DbType belirt
                        if (val == null)
                        {
                            dp.Add($"p{i}", null, GetDbTypeForColumn(outputDbType, columnDataTypes[key]));
                        }
                        else
                        {
                            dp.Add($"p{i}", val);
                        }
                    }
                }
                await outputConnection.ExecuteAsync(insertSql, dp);
            }

            // Bağlantıyı döngü dışında kapatın
            outputConnection.Close();
        }

        public async Task SaveDataToOutputMongoDb(string outputConnectionString, string outputTableName, IEnumerable<dynamic> data)
        {
            var outputContext = new MongoContext(outputConnectionString, null);
            var outputDb = outputContext.GetDatabase();
            var outputCollection = outputDb.GetCollection<BsonDocument>(outputTableName);

            var bsonDocs = data
                .Select(BsonHelpers.ToBsonDocumentSafe)
                .Where(doc => doc != null)
                .Cast<BsonDocument>()
                .ToList();

            if (bsonDocs.Count > 0)
            {
                await outputCollection.InsertManyAsync(bsonDocs);
            }
        }

        public async Task<(string dslTranslated, IEnumerable<dynamic> data)> QueryFromSqlDbAsync(string dbType, string connectionString, string query)
        {
            var context = new DapperContext(dbType, connectionString);

            var connection = await context.GetOpenConnectionAsync();

            // Eğer connection null ise hata fırlat
            if (connection == null)
            {
                throw new Exception("Could not open database connection. Please check the connection string and database type.");
            }

            // Input stringini QueryModel'e çevir
            var model = _queryParserService.Parse(query);

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

            var data = await connection.QueryAsync(sql);

            // bağlantıyı kapat
            connection.Close();

            return (sql, data);
        }

        public async Task<(string dslTranslated, IEnumerable<dynamic> data)> QueryFromMongoDbAsync(string connectionString, string query)
        {
            // Input stringini QueryModel'e çevir
            var model = _queryParserService.Parse(query);
            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };
            Console.WriteLine($"Model: {JsonSerializer.Serialize(model, options)}");

            var pipeline = _mongoDbService.BuildPipeline(model);
            string dslTranslated = "MongoDB Pipeline\n" + string.Join("\n", pipeline.Select(s => s.ToJson()));

            Console.WriteLine("Pipeline:");
            foreach (var stage in pipeline)
            {
                Console.WriteLine(stage.ToJson());
            }

            // MongoDB bağlantısı oluştur
            var mongoCtx = new MongoContext(connectionString, null);
            var db = mongoCtx.GetDatabase();
            var collectionName = model.Table;

            // Aggregate komutunu çalıştır
            var command = new BsonDocument
            {
                { "aggregate", collectionName },
                { "pipeline", new BsonArray(pipeline) },
                { "collation", new BsonDocument { { "locale", "tr" }, { "strength", 1 } } },
                { "cursor", new BsonDocument() }
            };

            var cmdResult = await db.RunCommandAsync<BsonDocument>(command);

            // Sonuçları al
            var firstBatch = cmdResult.GetValue("cursor").AsBsonDocument.GetValue("firstBatch").AsBsonArray
                .Select(v => v.AsBsonDocument)
                .ToList();

            // Sonuçları düz .NET nesnelerine (Dictionary) dönüştür
            List<dynamic> data = firstBatch.Select(d => (dynamic)BsonHelpers.BsonDocumentToDictionary(d)).ToList();

            return (dslTranslated, data);
        }

        public async Task<(string dslTranslated, IEnumerable<dynamic> data)> QueryFromApiAsync(string connectionString, string query)
        {
            // API'den veri çek
            using var httpClient = new HttpClient();
            var response = await httpClient.GetAsync(connectionString);
            response.EnsureSuccessStatusCode();
            var jsonString = await response.Content.ReadAsStringAsync();

            // JSON'u dynamic listeye çevir
            List<dynamic> apiData;
            using (JsonDocument doc = JsonDocument.Parse(jsonString))
            {
                if (doc.RootElement.ValueKind == JsonValueKind.Array)
                {
                    apiData = doc.RootElement.EnumerateArray()
                        .Select(e => JsonSerializer.Deserialize<Dictionary<string, object>>(e.GetRawText())!)
                        .Cast<dynamic>()
                        .ToList();
                }
                else if (doc.RootElement.ValueKind == JsonValueKind.Object)
                {
                    // Nesne içinde array içeren ilk property'yi ara
                    JsonElement arrayElement = default;
                    string? arrayPropertyName = null;

                    foreach (var property in doc.RootElement.EnumerateObject())
                    {
                        if (property.Value.ValueKind == JsonValueKind.Array)
                        {
                            arrayElement = property.Value;
                            arrayPropertyName = property.Name;
                            break;
                        }
                    }

                    if (arrayElement.ValueKind != JsonValueKind.Undefined)  // Fix: ValueEquals yerine ValueKind kontrolü
                    {
                        _logger.LogInformation($"Found array property '{arrayPropertyName}' in response root object");
                        apiData = arrayElement.EnumerateArray()
                            .Select(e => JsonSerializer.Deserialize<Dictionary<string, object>>(e.GetRawText())!)
                            .Cast<dynamic>()
                            .ToList();
                    }
                    else
                    {
                        // Hiç array bulunamadı, nesnenin kendisini kullan
                        apiData = new List<dynamic>
                        {
                            JsonSerializer.Deserialize<Dictionary<string, object>>(jsonString)!
                        };
                    }
                }
                else
                {
                    // Ne array ne object, sadece tek bir değerse
                    apiData = new List<dynamic>
                    {
                        JsonSerializer.Deserialize<Dictionary<string, object>>(jsonString)!
                    };
                }
            }

            // Query varsa filtrele/transform et
            IEnumerable<dynamic> filteredData = apiData;
            string dslTranslated = "API Data (no transformation)";

            if (!string.IsNullOrWhiteSpace(query))
            {
                // QueryModel parse et
                var model = _queryParserService.Parse(query);

                // In-memory filtreleme/transformation (LINQ ile)
                filteredData = _inMemoryQueryService.ApplyQuery(apiData, model);
                dslTranslated = $"API Query: {query}";
            }

            return (dslTranslated, filteredData);
        }

        // Veritabanı kolon tipi için DbType döner
        private DbType GetDbTypeForColumn(string outputDbType, string sqlDataType)
        {
            var upperType = sqlDataType.ToUpperInvariant();

            if (upperType.Contains("INT") || upperType.Contains("NUMBER"))
                return DbType.Int64;

            if (upperType.Contains("DOUBLE") || upperType.Contains("FLOAT") || upperType.Contains("BINARY_DOUBLE"))
                return DbType.Double;

            if (upperType.Contains("DECIMAL") || upperType.Contains("NUMERIC"))
                return DbType.Decimal;

            if (upperType.Contains("BOOL") || upperType.Contains("BIT"))
                return DbType.Boolean;

            if (upperType.Contains("TIME") || upperType.Contains("DATE"))
                return DbType.DateTime;

            if (upperType.Contains("JSON") || upperType.Contains("TEXT") || upperType.Contains("CLOB") || upperType.Contains("VARCHAR") || upperType.Contains("CHAR"))
                return DbType.String;

            if (upperType.Contains("BINARY") || upperType.Contains("BLOB"))
                return DbType.Binary;

            // Default
            return DbType.String;
        }
    }
}