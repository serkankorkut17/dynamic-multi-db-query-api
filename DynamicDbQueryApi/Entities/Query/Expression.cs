using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.Entities.Query
{
    [JsonDerivedType(typeof(IdentifierExpr), "Identifier")]
    [JsonDerivedType(typeof(StringLiteralExpr), "String")]
    [JsonDerivedType(typeof(NumberLiteralExpr), "Number")]
    [JsonDerivedType(typeof(BooleanLiteralExpr), "Boolean")]
    [JsonDerivedType(typeof(NullLiteralExpr), "Null")]
    [JsonDerivedType(typeof(FunctionExpr), "Function")]
    [JsonDerivedType(typeof(ArithmeticExpr), "Arithmetic")]
    // Basit ifade modeli: kolon, literal, fonksiyon, aritmetik
    public abstract class Expression { }

    // table.column veya column olabilir
    public sealed class IdentifierExpr : Expression
    {
        public string Name { get; set; } = "";
    }

    // 'string'
    public sealed class StringLiteralExpr : Expression
    {
        public string Value { get; set; } = "";
    }

    // 999.99
    public sealed class NumberLiteralExpr : Expression
    {
        public decimal Value { get; set; }
    }

    // true, false
    public sealed class BooleanLiteralExpr : Expression
    {
        public bool Value { get; set; }
    }

    // null
    public sealed class NullLiteralExpr : Expression
    {
    }

    // SUM(col), COUNT(*), AVG(col), UPPER(col), LOWER(col), ROUND(col, 2)
    public sealed class FunctionExpr : Expression
    {
        public string Name { get; set; } = "";
        public List<Expression> Arguments { get; set; } = new List<Expression>();
        public bool IsDistinct { get; set; } = false; // COUNT(DISTINCT col)
    }

    public enum ArithmeticOperator
    {
        Add,
        Subtract,
        Multiply,
        Divide
    }

    // col1 + col2, col1 - col2, col1 * col2, col1 / col2
    public sealed class ArithmeticExpr : Expression
    {
        public Expression Left { get; set; } = null!;
        public ArithmeticOperator Operator { get; set; } = ArithmeticOperator.Add;
        public Expression Right { get; set; } = null!;
    }
}