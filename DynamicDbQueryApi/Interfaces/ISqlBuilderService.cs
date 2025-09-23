using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;
using DynamicDbQueryApi.Entities.Query;

namespace DynamicDbQueryApi.Interfaces
{
    public interface ISqlBuilderService
    {
        string BuildSqlQuery(string dbType, QueryModel model);
        string ConvertFilterToSql(string dbType, FilterModel filter);
        string BuildCreateTableSql(string dbType, string tableName, Dictionary<string, string> columnDataTypes);
        string BuildAlterTableAddColumnSql(string dbType, string tableName, string columnName, string dataType);
    }
}