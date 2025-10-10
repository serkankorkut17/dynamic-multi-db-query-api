using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.Entities.Query;

namespace DynamicDbQueryApi.Interfaces
{
    public interface IInMemoryQueryService
    {
        IEnumerable<dynamic> ApplyQuery(List<dynamic> data, QueryModel model);
    }
}