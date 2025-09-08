using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.DTOs
{
    public class QueryModel
    {
        public string Table { get; set; } = "";
        public List<string> Columns { get; set; } = new List<string>();
        public FilterModel? Filters { get; set; }
        public List<IncludeModel> Includes { get; set; } = new List<IncludeModel>();
        public List<string> GroupBy { get; set; } = new List<string>();
        public List<OrderByModel> OrderBy { get; set; } = new List<OrderByModel>();
        
        public bool Distinct { get; set; } = false;
        public int? Limit { get; set; }
        public int? Offset { get; set; }
    }
}