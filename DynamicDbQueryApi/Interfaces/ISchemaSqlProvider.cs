using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;

namespace DynamicDbQueryApi.Interfaces
{
    public interface ISchemaSqlProvider
    {
        string GetIncludeQuery(string dbType, string fromTable, string includeTable);
        string GetTablesQuery(string dbType);
        string GetColumnsQuery(string dbType, string tableName);
        string GetRelationshipsQuery(string dbType);
        string GetColumnDataTypeQuery(string dbType, string? schema, string tableName, string columnName);
    }
}