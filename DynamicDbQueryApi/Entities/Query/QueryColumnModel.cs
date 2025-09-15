using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities.Query
{
    public class QueryColumnModel
    {
        public string Expression { get; set; } = "";
        public string? Alias { get; set; }
    }
}