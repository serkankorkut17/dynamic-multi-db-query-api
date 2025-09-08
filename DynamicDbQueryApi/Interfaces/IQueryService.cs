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
        string BuildSqlQuery(QueryModel queryModel);
        Task<IEnumerable<dynamic>> InspectDatabaseAsync(QueryRequestDTO request);
    }
}