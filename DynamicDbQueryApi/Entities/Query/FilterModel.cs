using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities.Query
{
    public abstract class FilterModel { }

    // Karşılaştırma operatorleri
    public enum ComparisonOperator
    {
        Eq,         // =
        Neq,        // !=
        Lt,         // <
        Lte,        // <=
        Gt,         // >
        Gte,        // >=
        Like,       // LIKE -> %val%
        Contains,   // CONTAINS -> %val%
        BeginsWith, // BEGINSWITH -> val%
        EndsWith,   // ENDSWITH -> %val
        IsNull,     // IS NULL
        IsNotNull   // IS NOT NULL
    }

    // Mantıksal operator
    public enum LogicalOperator
    {
        And,
        Or,
    }

    // =, !=, <, <=, >, >=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL
    public class ConditionFilterModel : FilterModel
    {
        public string Column { get; set; } = "";
        public ComparisonOperator Operator { get; set; } = ComparisonOperator.Eq;
        public string? Value { get; set; }
    }

    // AND, OR
    public class LogicalFilterModel : FilterModel
    {
        public LogicalOperator Operator { get; set; } = LogicalOperator.And;
        // public List<FilterModel> Filters { get; set; } = new List<FilterModel>();
        public FilterModel Left { get; set; } = null!;
        public FilterModel Right { get; set; } = null!;
    }
}