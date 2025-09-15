using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;
using DynamicDbQueryApi.Entities.Query;

namespace DynamicDbQueryApi.Interfaces
{
    public interface IQueryParserService
    {
        QueryModel Parse(string queryString);
    }
}