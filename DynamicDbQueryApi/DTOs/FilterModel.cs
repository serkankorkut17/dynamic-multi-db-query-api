using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.DTOs
{
    public abstract class FilterModel { }

    // =, !=, <, <=, >, >=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL
    public class ConditionFilterModel : FilterModel
    {
        public string Column { get; set; } = "";
        public string Operator { get; set; } = "=";
        public string Value { get; set; } = "";
    }
    
    // AND, OR
    public class LogicalFilterModel : FilterModel
    {
        public string LogicalOperator { get; set; } = "AND";
        public List<FilterModel> Filters { get; set; } = new List<FilterModel>();
    }
}