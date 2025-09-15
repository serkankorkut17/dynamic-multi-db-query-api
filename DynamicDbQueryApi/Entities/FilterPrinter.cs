using System;
using System.Collections.Generic;
using System.Text;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;
using DynamicDbQueryApi.Entities.Query;

public static class FilterPrinter
{
    // Public entry: dönen string'i Console.WriteLine ile yazdırabilirsin
    public static string Dump(FilterModel? node)
    {
        var path = new HashSet<FilterModel>(); // recursion path for cycle detection
        return DumpInternal(node, 0, path);
    }

    static string DumpInternal(FilterModel? node, int depth, HashSet<FilterModel> path)
    {
        var indent = new string(' ', depth * 2);
        if (node == null) return indent + "<null>";

        // detect cycles on current recursion path
        if (path.Contains(node)) return indent + "<cycle detected>";

        // push
        path.Add(node);

        var sb = new StringBuilder();

        if (node is ConditionFilterModel c)
        {
            sb.Append(indent);
            sb.Append("Condition: ");
            sb.Append(c.Column ?? "<empty-column>");
            sb.Append(' ');
            sb.Append(OpToString(c.Operator));
            sb.Append(' ');
            sb.Append(FormatValue(c.Value));
        }
        else if (node is LogicalFilterModel l)
        {
            sb.Append(indent);
            sb.Append("Logical: ");
            sb.AppendLine(l.Operator.ToString().ToUpperInvariant());

            // Left
            sb.AppendLine(DumpInternal(l.Left, depth + 1, path));

            // Right
            sb.Append(DumpInternal(l.Right, depth + 1, path));
        }
        else
        {
            sb.Append(indent);
            sb.Append("<unknown node type: ");
            sb.Append(node.GetType().Name);
            sb.Append('>');
        }

        // pop
        path.Remove(node);

        return sb.ToString();
    }

    static string OpToString(ComparisonOperator op) => op switch
    {
        ComparisonOperator.Eq => "=",
        ComparisonOperator.Neq => "!=",
        ComparisonOperator.Lt => "<",
        ComparisonOperator.Lte => "<=",
        ComparisonOperator.Gt => ">",
        ComparisonOperator.Gte => ">=",
        ComparisonOperator.Like => "LIKE",
        ComparisonOperator.Contains => "CONTAINS",
        ComparisonOperator.BeginsWith => "BEGINSWITH",
        ComparisonOperator.EndsWith => "ENDSWITH",
        ComparisonOperator.IsNull => "IS NULL",
        ComparisonOperator.IsNotNull => "IS NOT NULL",
        _ => op.ToString()
    };

    static string FormatValue(string? v)
    {
        if (v is null) return "NULL";
        // show quoted if contains whitespace or special chars
        if (v.IndexOfAny(new char[] { ' ', '\t', '\n', '\r', '\'' }) >= 0)
        {
            // escape single quote for readability
            var esc = v.Replace("'", "''");
            return $"'{esc}'";
        }
        return v;
    }
}
