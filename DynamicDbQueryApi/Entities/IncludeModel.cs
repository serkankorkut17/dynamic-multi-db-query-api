using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities
{
    public class IncludeModel
    {
        // 
        public string Table { get; set; } = "";
        public string TableKey { get; set; } = "";

        public string IncludeTable { get; set; } = "";
        public string IncludeKey { get; set; } = "";

        public string JoinType { get; set; } = "LEFT";
    }
}