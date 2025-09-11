using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;

namespace DynamicDbQueryApi.Interfaces
{
    public interface ISqlBuilderService
    {
        string BuildSqlQuery(string dbType, QueryModel model);
        string ConvertFilterToSql(FilterModel filter);
    }
}