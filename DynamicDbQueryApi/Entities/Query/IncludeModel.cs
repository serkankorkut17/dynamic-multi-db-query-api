using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities.Query
{
    public class IncludeModel
    {
        // 
        public string Table { get; set; } = "";
        public string? TableKey { get; set; } = null;

        public string IncludeTable { get; set; } = "";
        public string? IncludeKey { get; set; } = null;

        public string JoinType { get; set; } = "LEFT";
    }
}