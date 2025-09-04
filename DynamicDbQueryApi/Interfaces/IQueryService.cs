using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;

namespace DynamicDbQueryApi.Interfaces
{
    public interface IQueryService
    {
        Task<IEnumerable<dynamic>> QueryAsync(QueryRequestDTO request);
        QueryModel ParseQueryString(IDbConnection connection, string dbType, string queryString);
        string BuildSqlQuery(QueryModel queryModel);
        string GetIncludeQuery(string dbType, string fromTable, string includeTable);
        Task<IEnumerable<dynamic>> InspectDatabaseAsync(QueryRequestDTO request);
    }
}