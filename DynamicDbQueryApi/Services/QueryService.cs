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
using Serilog;

namespace DynamicDbQueryApi.Services
{
    public class QueryService : IQueryService
    {
        private readonly ILogger<QueryService> _logger;
        private readonly IQueryParserService _queryParserService;
        private readonly ISqlBuilderService _sqlBuilderService;
        private readonly IDbSchemaService _dbSchemaService;

        public QueryService(ILogger<QueryService> logger, IQueryParserService queryParserService, ISqlBuilderService sqlBuilderService, IDbSchemaService dbSchemaService)
        {
            _logger = logger;
            _queryParserService = queryParserService;
            _sqlBuilderService = sqlBuilderService;
            _dbSchemaService = dbSchemaService;
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
            _logger.LogInformation("Query: {Query}", request.Query);

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
                _logger.LogInformation("Updated Include: {Include}", JsonSerializer.Serialize(updatedInclude));

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
            data = data.ToList();

            return new QueryResultDTO
            {
                Sql = sql,
                Data = data
            };
        }

        public async Task<IncludeModel?> UpdateIncludeModel(IDbConnection connection, string dbType, IncludeModel include)
        {
            if (include == null) return null;

            var fromTable = include.Table;
            var includeTable = include.IncludeTable;
            var pairs = await _dbSchemaService.GetForeignKeyPairAsync(connection, dbType, fromTable, includeTable);
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
                pairs = await _dbSchemaService.GetForeignKeyPairAsync(connection, dbType, includeTable, fromTable);
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

        public async Task<IEnumerable<dynamic>> InspectDatabaseAsync(InspectRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var connection = await context.GetOpenConnectionAsync();

            var type = request.DbType.ToLower();

            // 1) Tabloları al
            string tablesSql = _dbSchemaService.GetTablesQuery(type);
            var tableNames = (await connection.QueryAsync<string>(tablesSql)).ToList();

            // 2) Her tablo için column çek
            var result = new List<object>();

            foreach (var table in tableNames)
            {
                string columnsSql = _dbSchemaService.GetColumnsQuery(type, table);

                IEnumerable<dynamic> columns = await connection.QueryAsync(columnsSql);

                result.Add(new
                {
                    table = table,
                    columns = columns
                });
            }

            // 3) Tablolar arası ilişkileri çek
            string relationsSql = _dbSchemaService.GetRelationshipsQuery(type);

            var relationsRaw = (await connection.QueryAsync(relationsSql)).ToList();

            // _logger.LogInformation("Raw Relations: {Relations}", JsonSerializer.Serialize(relationsRaw));

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