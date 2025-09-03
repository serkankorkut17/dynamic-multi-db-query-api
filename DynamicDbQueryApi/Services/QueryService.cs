using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using DynamicDbQueryApi.Data;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Interfaces;

namespace DynamicDbQueryApi.Services
{
    public class QueryService : IQueryService
    {
        public async Task<IEnumerable<dynamic>> QueryAsync(QueryRequestDTO request)
        {
            var context = new DapperContext(request.DbType, request.ConnectionString);

            var connection = await context.GetOpenConnectionAsync();
            
            return await connection.QueryAsync(request.Query);
        }
    }
}