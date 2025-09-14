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
        Task<QueryResultDTO> MyQueryAsync(QueryRequestDTO request);
        Task<IEnumerable<dynamic>> SQLQueryAsync(QueryRequestDTO request);
        Task<InspectResponseDTO> InspectDatabaseAsync(InspectRequestDTO request);
    }
}