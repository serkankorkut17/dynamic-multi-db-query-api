using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;
using DynamicDbQueryApi.Interfaces;

namespace DynamicDbQueryApi.Services
{
    public class SqlBuilderService : ISqlBuilderService
    {
        public string BuildSqlQuery(string dbType, QueryModel model)
        {
            // SELEFCT and FROM Part
            string columns = model.Columns.Any() ? string.Join(", ", model.Columns) : "*";
            string table = model.Table;

            string sql = "";

            // DISTINCT Part
            if (model.Distinct)
            {
                sql += "SELECT DISTINCT ";
            }
            else
            {
                sql += "SELECT ";
            }
            sql = $"{sql}{columns} FROM {table}";

            // JOINS Part
            if (model.Includes.Any())
            {
                foreach (var include in model.Includes)
                {
                    sql += $" {include.JoinType.ToUpper()} JOIN {include.IncludeTable} ON {include.Table}.{include.TableKey} = {include.IncludeTable}.{include.IncludeKey}";
                }
            }

            var text = FilterPrinter.Dump(model.Filters);
            Console.WriteLine(text);

            // FILTERS Part
            if (model.Filters != null)
            {
                var whereClause = ConvertFilterToSql(model.Filters);
                sql += $" WHERE {whereClause}";
            }


            // GROUP BY Part
            if (model.GroupBy.Any())
            {
                sql += " GROUP BY " + string.Join(", ", model.GroupBy);
            }

            // HAVING Part
            if (model.Having != null)
            {
                var havingClause = ConvertFilterToSql(model.Having);
                sql += $" HAVING {havingClause}";
            }

            // ORDER BY Part
            if (model.OrderBy.Any())
            {
                sql += " ORDER BY " + string.Join(", ", model.OrderBy.Select(o => $"{o.Column} {(o.Desc ? "DESC" : "ASC")}"));
            }

            // LIMIT and OFFSET Part
            if (model.Limit.HasValue && model.Limit.Value > 0)
            {
                if (dbType == "postgresql" || dbType == "postgres")
                {
                    sql += $" LIMIT {model.Limit.Value} OFFSET {(model.Offset.HasValue ? model.Offset.Value : 0)}";
                }
                else if (dbType == "mysql")
                {
                    sql += $" LIMIT {model.Limit.Value} OFFSET {(model.Offset.HasValue ? model.Offset.Value : 0)}";
                }
                else if ((dbType == "mssql" || dbType == "sqlserver") && model.OrderBy.Any())
                {
                    sql += $" OFFSET {(model.Offset.HasValue ? model.Offset.Value : 0)} ROWS FETCH NEXT {model.Limit.Value} ROWS ONLY";
                }
                else if ((dbType == "oracle") && model.OrderBy.Any())
                {
                    sql += $" OFFSET {(model.Offset.HasValue ? model.Offset.Value : 0)} ROWS FETCH NEXT {model.Limit.Value} ROWS ONLY";
                }
            }

            return sql;
        }

        public string ConvertFilterToSql(FilterModel filter)
        {
            if (filter == null) return "1=1";

            if (filter is ConditionFilterModel condition)
            {
                if (condition.Operator == ComparisonOperator.IsNull)
                {
                    return $"{condition.Column} IS NULL";
                }
                else if (condition.Operator == ComparisonOperator.IsNotNull)
                {
                    return $"{condition.Column} IS NOT NULL";
                }
                else if (condition.Value == null)
                {
                    throw new ArgumentException($"Value cannot be null for operator {condition.Operator}");
                }
                else if (condition.Operator == ComparisonOperator.Like ||
                         condition.Operator == ComparisonOperator.Contains ||
                         condition.Operator == ComparisonOperator.BeginsWith ||
                         condition.Operator == ComparisonOperator.EndsWith)
                {
                    string pattern = condition.Operator switch
                    {
                        ComparisonOperator.Like => $"{condition.Value}",
                        ComparisonOperator.Contains => $"%{condition.Value}%",
                        ComparisonOperator.BeginsWith => $"{condition.Value}%",
                        ComparisonOperator.EndsWith => $"%{condition.Value}",
                        _ => throw new NotSupportedException($"Unsupported operator {condition.Operator}")
                    };
                    return $"{condition.Column} LIKE '{pattern}'";
                }
                else
                {
                    string sqlOperator = condition.Operator switch
                    {
                        ComparisonOperator.Eq => "=",
                        ComparisonOperator.Neq => "!=",
                        ComparisonOperator.Lt => "<",
                        ComparisonOperator.Lte => "<=",
                        ComparisonOperator.Gt => ">",
                        ComparisonOperator.Gte => ">=",
                        _ => throw new NotSupportedException($"Unsupported operator {condition.Operator}")
                    };

                    // değer sayısal ise tırnak kullanma
                    if (double.TryParse(condition.Value, out _))
                    {
                        return $"{condition.Column} {sqlOperator} {condition.Value}";
                    }
                    else
                    {
                        return $"{condition.Column} {sqlOperator} '{condition.Value}'";
                    }
                }
            }
            else if (filter is LogicalFilterModel logical)
            {
                string left = ConvertFilterToSql(logical.Left);
                string right = ConvertFilterToSql(logical.Right);
                string op = logical.Operator.ToString().ToUpperInvariant();

                return $"({left} {op} {right})";
            }
            else
            {
                throw new NotSupportedException($"Filter type {filter.GetType()} is not supported.");
            }
        }
    }
}