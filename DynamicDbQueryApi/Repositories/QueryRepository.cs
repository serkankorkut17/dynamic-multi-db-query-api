using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Dapper;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;
using DynamicDbQueryApi.Interfaces;

namespace DynamicDbQueryApi.Repositories
{
    public class QueryRepository : IQueryRepository
    {
        private readonly ILogger<QueryRepository> _logger;
        private readonly ISchemaSqlProvider _sqlProvider;

        public QueryRepository(ILogger<QueryRepository> logger, ISchemaSqlProvider sqlProvider)
        {
            _logger = logger;
            _sqlProvider = sqlProvider;
        }

        // Belirtilen tablo ve kolona ait data type'ı döner
        public async Task<string?> GetColumnDataTypeAsync(IDbConnection connection, string dbType, string tableName, string columnName)
        {
            string? schema = null;
            string? schemaQuery = null;
            if (dbType.ToLower() == "postgresql" || dbType.ToLower() == "postgres")
            {
                // PostgreSQL'de varsayılan şema public
                schemaQuery = "SELECT current_schema;";
                schema = await connection.QueryFirstOrDefaultAsync<string>(schemaQuery) ?? "public";
            }
            else if (dbType.ToLower() == "mssql" || dbType.ToLower() == "sqlserver")
            {
                // MSSQL'de varsayılan şema dbo
                schemaQuery = "SELECT SCHEMA_NAME();";
                schema = await connection.QueryFirstOrDefaultAsync<string>(schemaQuery) ?? "dbo";
            }
            else if (dbType.ToLower() == "oracle")
            {
                // Oracle'da varsayılan şema kullanıcı adı ile aynı
                schemaQuery = "SELECT USER FROM dual";
                schema = await connection.QueryFirstOrDefaultAsync<string>(schemaQuery) ?? "SYSTEM";
            }

            var sql = _sqlProvider.GetColumnDataTypeQuery(dbType, schema, tableName, columnName);

            var row = await connection.QueryFirstOrDefaultAsync(sql);

            // Row içinde data_type veya DATA_TYPE anahtarını bul
            IDictionary<string, object>? dict = row as IDictionary<string, object>;
            if (dict != null)
            {
                if (dbType.ToLower() == "oracle")
                {
                    if (dict.TryGetValue("DATA_TYPE", out var value))
                    {
                        return value?.ToString();
                    }
                }
                else
                {
                    if (dict.TryGetValue("data_type", out var value))
                    {
                        return value?.ToString();
                    }
                    if (dict.TryGetValue("DATA_TYPE", out value))
                    {
                        return value?.ToString();
                    }
                }
            }

            return null;
        }

        // Belirtilen tablolara ait foreign key çiftini döner
        public async Task<ForeignKeyPair?> GetForeignKeyPairAsync(IDbConnection connection, string dbType, string fromTable, string includeTable)
        {
            var includeSql = _sqlProvider.GetIncludeQuery(dbType, fromTable, includeTable);

            var includeResult = await connection.QueryFirstOrDefaultAsync(includeSql);
            _logger.LogInformation("Include Query Result: {Result}", JsonSerializer.Serialize(includeResult as object));

            if (includeResult != null)
            {
                var pair = new ForeignKeyPair();
                IDictionary<string, object>? dict = includeResult as IDictionary<string, object>;

                if (dict != null)
                {
                    if (dbType.ToLower() == "oracle")
                    {
                        // Oracle kolon isimleri büyük harfli döner
                        pair.ForeignKey = dict["FK_COLUMN"]?.ToString() ?? "";
                        pair.ReferencedKey = dict["REFERENCED_COLUMN"]?.ToString() ?? "";
                    }
                    else
                    {
                        pair.ForeignKey = dict["fk_column"]?.ToString() ?? "";
                        pair.ReferencedKey = dict["referenced_column"]?.ToString() ?? "";
                    }
                }
                return pair;
            }
            return null;
        }

        // Tabloları ve kolonlarını döner
        public async Task<List<TableModel>> GetTablesAndColumnsAsync(IDbConnection connection, string dbType)
        {
            // 1) Tabloları al
            string tablesSql = _sqlProvider.GetTablesQuery(dbType);
            var tableNames = (await connection.QueryAsync<string>(tablesSql)).ToList();

            _logger.LogInformation("Tables: {Tables}", JsonSerializer.Serialize(tableNames));

            // 2) Her tablo için column çek
            var result = new List<TableModel>();

            foreach (var table in tableNames)
            {
                string columnsSql = _sqlProvider.GetColumnsQuery(dbType, table);

                IEnumerable<dynamic> columns = await connection.QueryAsync(columnsSql);

                var columnModels = new List<ColumnModel>();
                foreach (var col in columns)
                {
                    IDictionary<string, object>? dict = col as IDictionary<string, object>;
                    if (dict != null)
                    {
                        var columnModel = new ColumnModel();
                        if (dbType.ToLower() == "oracle")
                        {
                            columnModel.Name = dict["NAME"]?.ToString() ?? "";
                            columnModel.DataType = dict["DATA_TYPE"]?.ToString() ?? "";
                            columnModel.IsNullable = (dict["NULLABLE"]?.ToString() ?? "N") == "Y";
                        }
                        else
                        {
                            columnModel.Name = dict["name"]?.ToString() ?? "";
                            columnModel.DataType = dict["data_type"]?.ToString() ?? "";
                            columnModel.IsNullable = (dict["is_nullable"]?.ToString() ?? "NO") == "YES";
                        }
                        columnModels.Add(columnModel);
                    }
                }
                result.Add(new TableModel { Table = table, Columns = columnModels });
            }

            return result;
        }

        // Tablolar arası ilişkileri döner
        public async Task<List<RelationshipModel>> GetRelationshipsAsync(IDbConnection connection, string dbType)
        {
            var sql = _sqlProvider.GetRelationshipsQuery(dbType);
            var result = await connection.QueryAsync(sql);

            var relationships = new List<RelationshipModel>();
            foreach (var row in result)
            {
                IDictionary<string, object>? dict = row as IDictionary<string, object>;
                if (dict != null)
                {
                    var rel = new RelationshipModel();
                    if (dbType.ToLower() == "oracle")
                    {
                        rel.ConstraintName = dict["CONSTRAINT_NAME"]?.ToString() ?? "";
                        rel.FkTable = dict["FK_TABLE"]?.ToString() ?? "";
                        rel.FkColumn = dict["FK_COLUMN"]?.ToString() ?? "";
                        rel.PkTable = dict["PK_TABLE"]?.ToString() ?? "";
                        rel.PkColumn = dict["PK_COLUMN"]?.ToString() ?? "";
                    }
                    else
                    {
                        rel.ConstraintName = dict["constraint_name"]?.ToString() ?? "";
                        rel.FkTable = dict["fk_table"]?.ToString() ?? "";
                        rel.FkColumn = dict["fk_column"]?.ToString() ?? "";
                        rel.PkTable = dict["pk_table"]?.ToString() ?? "";
                        rel.PkColumn = dict["pk_column"]?.ToString() ?? "";
                    }
                    relationships.Add(rel);
                }
            }

            return relationships;
        }
    }

}