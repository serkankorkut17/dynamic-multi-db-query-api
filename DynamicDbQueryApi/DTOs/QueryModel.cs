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
        public List<FilterModel> Filters { get; set; } = new List<FilterModel>();
        public IncludeModel Include { get; set; } = new IncludeModel();
        public string? GroupBy { get; set; }
    }
}