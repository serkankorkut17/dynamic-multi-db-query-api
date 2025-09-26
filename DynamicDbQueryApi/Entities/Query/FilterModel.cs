using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities.Query
{
    [JsonDerivedType(typeof(ConditionFilterModel), "condition")]
    [JsonDerivedType(typeof(LogicalFilterModel), "logical")]
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
        ILike,      // ILIKE -> %val% (case insensitive)
        NotLike,    // NOT LIKE -> NOT %val%
        NotILike,   // NOT ILIKE -> NOT %val% (case insensitive)
        Contains,   // CONTAINS -> %val%
        IContains,  // ICONTAINS -> %val% (case insensitive)
        NotContains,// NOT CONTAINS -> NOT %val%
        NotIContains,// NOT ICONTAINS -> NOT %val% (case insensitive)
        BeginsWith, // BEGINSWITH -> val%
        IBeginsWith,// IBEGINSWITH -> val% (case insensitive)
        NotBeginsWith, // NOT BEGINSWITH -> NOT val%
        NotIBeginsWith,// NOT IBEGINSWITH -> NOT val% (case insensitive)
        EndsWith,   // ENDSWITH -> %val
        IEndsWith,  // IENDSWITH -> %val (case insensitive)
        NotEndsWith,// NOT ENDSWITH -> NOT %val
        NotIEndsWith,// NOT IENDSWITH -> NOT %val (case insensitive)
        IsNull,     // IS NULL
        IsNotNull,  // IS NOT NULL
        In,         // IN (val1, val2, ...)
        NotIn,      // NOT IN (val1, val2, ...)
        Between,    // BETWEEN (val1 AND val2)
        NotBetween, // NOT BETWEEN (val1 AND val2)
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