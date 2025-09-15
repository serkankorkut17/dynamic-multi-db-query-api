using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;

namespace DynamicDbQueryApi.Interfaces
{
    public interface IQueryRepository
    {
        Task<ForeignKeyPair?> GetForeignKeyPairAsync(IDbConnection conn, string dbType, string fromTable, string includeTable);
        Task<string?> GetColumnDataTypeAsync(IDbConnection connection, string dbType, string tableName, string columnName);
        Task<List<TableModel>> GetTablesAndColumnsAsync(IDbConnection connection, string dbType);
        Task<List<RelationshipModel>> GetRelationshipsAsync(IDbConnection connection, string dbType);
        Task<bool> TableExistsAsync(IDbConnection connection, string dbType, string tableName);
        Task<bool> ColumnExistsInTableAsync(IDbConnection connection, string dbType, string tableName, string columnName);
    }
}