using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;
using DynamicDbQueryApi.Entities.Query;
using DynamicDbQueryApi.Interfaces;

namespace DynamicDbQueryApi.Services
{
    public class SqlBuilderService : ISqlBuilderService
    {
        // QueryModel'den SQL sorgusu oluşturma
        public string BuildSqlQuery(string dbType, QueryModel model)
        {
            // SELECT and FROM Part
            StringBuilder columnsBuilder = new StringBuilder();
            if (model.Columns.Any())
            {
                for (int i = 0; i < model.Columns.Count; i++)
                {
                    var c = model.Columns[i];
                    if (i > 0) columnsBuilder.Append(", ");

                    // Kolon bir fonksiyon ise fonksiyonu ve içindeki argümanları işle
                    var funcInfo = GetFunction(c.Expression);
                    if (funcInfo != null)
                    {
                        var funcName = funcInfo.Value.funcName;
                        var inner = funcInfo.Value.inner;

                        var args = inner.Split(',').Select(a => a.Trim()).ToList();

                        // Fonksiyonu oluştur
                        var funcSql = BuildFunction(dbType, funcName, args);
                        if (!string.IsNullOrEmpty(c.Alias))
                        {
                            columnsBuilder.Append($"{funcSql} AS {c.Alias}");
                        }
                        else
                        {
                            columnsBuilder.Append(funcSql);
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(c.Alias))
                        {
                            columnsBuilder.Append($"{c.Expression} AS {c.Alias}");
                        }
                        else
                        {
                            columnsBuilder.Append(c.Expression);
                        }
                    }
                }
            }
            else
            {
                columnsBuilder.Append("*");
            }
            string columns = columnsBuilder.ToString();
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

            // FILTERS Part
            if (model.Filters != null)
            {
                var whereClause = ConvertFilterToSql(dbType, model.Filters);
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
                var havingClause = ConvertFilterToSql(dbType, model.Having);
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

        // Kolonun bir fonksiyon olup olmadığını kontrol etme (örneğin COUNT(*), SUM(col) vb.)
        public (string funcName, string inner)? GetFunction(string column)
        {
            if (string.IsNullOrWhiteSpace(column)) return null;

            // Regex: func ( ... ) şeklinde olanları yakala -> ...
            var pattern = $@"^(?<func>[A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(?<inner>.*?)\s*\)$";
            var funcMatch = Regex.Match(column, pattern, RegexOptions.IgnoreCase);

            if (funcMatch.Success)
            {
                var funcName = funcMatch.Groups["func"].Value.ToUpperInvariant();
                var inner = funcMatch.Groups["inner"].Value.Trim();

                return (funcName, inner);
            }
            return null;
        }

        public string BuildFunction(string dbType, string functionName, List<string> args)
        {
            // Aggregate fonksiyonlarını oluştur (COUNT, SUM, AVG, MIN, MAX)
            var aggregateFuncs = new[] { "COUNT", "SUM", "AVG", "MIN", "MAX" };
            if (aggregateFuncs.Contains(functionName))
            {
                var joinedArgs = string.Join(", ", args);
                return $"{functionName.ToUpperInvariant()}({joinedArgs})";
            }

            // Sayısal fonksiyonları oluştur (ABS, CEIL, FLOOR, ROUND, SQRT, POWER, MOD, EXP, LOG, LOG10)
            var numericFuncs = new[] { "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "SQRT", "POWER", "MOD", "EXP", "LOG", "LOG10" };
            if (numericFuncs.Contains(functionName))
            {
                if (functionName.Equals("ROUND", StringComparison.OrdinalIgnoreCase))
                {
                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        if (args.Count == 1)
                        {
                            return $"ROUND({args[0]}::numeric, 0)";
                        }
                        else if (args.Count == 2)
                        {
                            return $"ROUND({args[0]}::numeric, {args[1]})";
                        }
                    }
                    else
                    {
                        if (args.Count == 1)
                        {
                            return $"ROUND({args[0]}, 0)";
                        }
                        else if (args.Count == 2)
                        {
                            return $"ROUND({args[0]}, {args[1]})";
                        }
                    }
                }
                if (functionName.Equals("MOD", StringComparison.OrdinalIgnoreCase) && args.Count == 2)
                {
                    if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return $"({args[0]} % {args[1]})";
                    }
                    else
                    {
                        return $"MOD({args[0]}, {args[1]})";
                    }
                }

                else if (functionName.Equals("CEIL", StringComparison.OrdinalIgnoreCase) || functionName.Equals("CEILING", StringComparison.OrdinalIgnoreCase) && args.Count == 1)
                {
                    return $"CEILING({args[0]})";
                }

                else if (functionName.Equals("LOG", StringComparison.OrdinalIgnoreCase))
                {
                    if (args.Count == 2)
                    {
                        if (dbType == "mssql" || dbType == "sqlserver")
                        {
                            return $"(LOG({args[0]}) / LOG({args[1]}))";
                        }
                        else
                        {
                            return $"LOG({args[1]}, {args[0]})";
                        }
                    }
                    else if (args.Count == 1)
                    {
                        if (dbType == "mssql" || dbType == "sqlserver")
                        {
                            return $"LOG({args[0]})";
                        }
                        else
                        {
                            return $"LN({args[0]})";
                        }
                    }
                }

                else
                {
                    var joinedArgs = string.Join(", ", args);
                    return $"{functionName}({joinedArgs})";
                }
            }

            // Metin fonksiyonlarını oluştur (LENGTH, LEN, SUBSTRING, SUBSTR, CONCAT, LOWER, UPPER, TRIM, LTRIM, RTRIM, INDEXOF, REPLACE, REVERSE)
            var stringFuncs = new[] { "LENGTH", "LEN", "SUBSTRING", "SUBSTR", "CONCAT", "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM", "INDEXOF", "REPLACE", "REVERSE" };
            if (stringFuncs.Contains(functionName))
            {
                if ((functionName.Equals("LENGTH", StringComparison.OrdinalIgnoreCase) || functionName.Equals("LEN", StringComparison.OrdinalIgnoreCase)) && args.Count == 1)
                {
                    if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return $"LEN({args[0]})";
                    }
                    else if (dbType == "mysql")
                    {
                        return $"CHAR_LENGTH({args[0]})";
                    }
                    else
                    {
                        return $"LENGTH({args[0]})";
                    }
                }
                else if ((functionName.Equals("SUBSTRING", StringComparison.OrdinalIgnoreCase) || functionName.Equals("SUBSTR", StringComparison.OrdinalIgnoreCase)) && (args.Count == 2 || args.Count == 3))
                {
                    // 1-based index kullanımı için 1 ekle
                    if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        if (args.Count == 2)
                        {
                            return $"SUBSTRING({args[0]}, {args[1]} + 1, LEN({args[0]}) - {args[1]})";
                        }
                        else
                        {
                            return $"SUBSTRING({args[0]}, {args[1]} + 1, {args[2]})";
                        }
                    }
                    else
                    {
                        if (args.Count == 2)
                        {
                            return $"SUBSTR({args[0]}, {args[1]} + 1)";
                        }
                        else
                        {
                            return $"SUBSTR({args[0]}, {args[1]} + 1, {args[2]})";
                        }
                    }
                }
                else if (functionName.Equals("CONCAT", StringComparison.OrdinalIgnoreCase) && args.Count >= 2)
                {
                    if (dbType == "oracle")
                    {
                        return string.Join(" || ", args);
                    }
                    else if (dbType == "mysql")
                    {
                        List<string> modifiedArgs = new List<string>();
                        foreach (var arg in args)
                        {
                            // null koruması ekle
                            modifiedArgs.Add($"COALESCE({arg}, '')");
                        }
                        return $"CONCAT({string.Join(", ", modifiedArgs)})";
                    }
                    else
                    {
                        var joinedArgs = string.Join(", ", args);
                        return $"CONCAT({joinedArgs})";
                    }
                }
                else if ((functionName.Equals("TRIM", StringComparison.OrdinalIgnoreCase) || functionName.Equals("LTRIM", StringComparison.OrdinalIgnoreCase) || functionName.Equals("RTRIM", StringComparison.OrdinalIgnoreCase)) && args.Count == 1)
                {
                    return $"{functionName}({args[0]})";
                }
                else if (functionName.Equals("INDEXOF", StringComparison.OrdinalIgnoreCase) && args.Count == 2)
                {
                    // 0-based index için 1 çıkar
                    if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return $"(CHARINDEX({args[1]}, {args[0]}) - 1)";
                    }
                    else if (dbType == "mysql")
                    {
                        return $"(INSTR({args[0]}, {args[1]}) - 1)";
                    }
                    else if (dbType == "oracle")
                    {
                        return $"(INSTR({args[0]}, {args[1]}) - 1)";
                    }
                    else if (dbType == "postgresql" || dbType == "postgres")
                    {
                        return $"(STRPOS({args[0]}, {args[1]}) - 1)";
                    }
                    else
                    {
                        return $"(INSTR({args[0]}, {args[1]}) - 1)";
                    }
                }
                else if (functionName.Equals("REPLACE", StringComparison.OrdinalIgnoreCase) && args.Count == 3)
                {
                    var joinedArgs = string.Join(", ", args);
                    return $"REPLACE({joinedArgs})";
                }
                else if (functionName.Equals("REVERSE", StringComparison.OrdinalIgnoreCase) && args.Count == 1)
                {
                    return $"REVERSE({args[0]})";
                }
                else
                {
                    var joinedArgs = string.Join(", ", args);
                    return $"{functionName}({joinedArgs})";
                }
            }

            // Null fonksiyonlarını oluştur (COALESCE, IFNULL, ISNULL, NVL)
            var nullFuncs = new[] { "COALESCE", "IFNULL", "ISNULL", "NVL" };
            if (nullFuncs.Contains(functionName) && args.Count >= 2)
            {
                var joinedArgs = string.Join(", ", args);
                return $"COALESCE({joinedArgs})";
            }

            // Tarih fonksiyonlarını oluştur (NOW, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, DATEADD, DATEDIFF, DATENAME, DATEPART, DAY, MONTH, YEAR)
            // var dateFuncs = new[] { "NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATEADD", "DATEDIFF", "DATENAME", "DATEPART", "DAY", "MONTH", "YEAR" };

            throw new NotSupportedException($"Function {functionName} with {args.Count} args is not supported.");
        }

        // FilterModel'den SQL WHERE/HAVING koşulu oluşturma
        public string ConvertFilterToSql(string dbType, FilterModel filter)
        {
            if (filter == null) return "1=1";

            if (filter is ConditionFilterModel condition)
            {
                var comparisonOperator = condition.Operator;

                var columnName = condition.Column;
                var funcInfo = GetFunction(columnName);
                if (funcInfo != null)
                {
                    var funcName = funcInfo.Value.funcName;
                    var inner = funcInfo.Value.inner;
                    var args = inner.Split(',').Select(a => a.Trim()).ToList();
                    columnName = BuildFunction(dbType, funcName, args);
                }

                var value = condition.Value;
                var funcValueInfo = value != null ? GetFunction(value) : null;
                if (funcValueInfo != null)
                {
                    var funcName = funcValueInfo.Value.funcName;
                    var inner = funcValueInfo.Value.inner;
                    var args = inner.Split(',').Select(a => a.Trim()).ToList();
                    value = BuildFunction(dbType, funcName, args);
                }

                if (comparisonOperator == ComparisonOperator.IsNull)
                {
                    return $"{columnName} IS NULL";
                }
                else if (comparisonOperator == ComparisonOperator.IsNotNull)
                {
                    return $"{columnName} IS NOT NULL";
                }
                else if (value == null)
                {
                    throw new ArgumentException($"Value cannot be null for operator {comparisonOperator}");
                }
                else if (comparisonOperator == ComparisonOperator.Like ||
                         comparisonOperator == ComparisonOperator.Contains ||
                         comparisonOperator == ComparisonOperator.BeginsWith ||
                         comparisonOperator == ComparisonOperator.EndsWith)
                {
                    // Stringdeki tırnakları kaldır
                    string valueRaw = value.ToString().Trim('\'');
                    string pattern = comparisonOperator switch
                    {
                        ComparisonOperator.Like => $"{valueRaw}",
                        ComparisonOperator.Contains => $"%{valueRaw}%",
                        ComparisonOperator.BeginsWith => $"{valueRaw}%",
                        ComparisonOperator.EndsWith => $"%{valueRaw}",
                        _ => throw new NotSupportedException($"Unsupported operator {comparisonOperator}")
                    };
                    return $"{columnName} LIKE '{pattern}'";
                }
                else
                {
                    string sqlOperator = comparisonOperator switch
                    {
                        ComparisonOperator.Eq => "=",
                        ComparisonOperator.Neq => "!=",
                        ComparisonOperator.Lt => "<",
                        ComparisonOperator.Lte => "<=",
                        ComparisonOperator.Gt => ">",
                        ComparisonOperator.Gte => ">=",
                        _ => throw new NotSupportedException($"Unsupported operator {comparisonOperator}")
                    };

                    // Eğer value true ya da false ise, MSSQL'de 1 ya da 0 olarak kullan
                    if (value.Equals("true", StringComparison.OrdinalIgnoreCase) || value.Equals("false", StringComparison.OrdinalIgnoreCase))
                    {
                        if (dbType == "mssql" || dbType == "sqlserver")
                        {
                            value = value.Equals("true", StringComparison.OrdinalIgnoreCase) ? "1" : "0";
                        }
                        else if (dbType == "postgresql" || dbType == "postgres")
                        {
                            value = value.Equals("true", StringComparison.OrdinalIgnoreCase) ? "TRUE" : "FALSE";
                        }
                        else if (dbType == "mysql")
                        {
                            value = value.Equals("true", StringComparison.OrdinalIgnoreCase) ? "1" : "0";
                        }
                        else if (dbType == "oracle")
                        {
                            value = value.Equals("true", StringComparison.OrdinalIgnoreCase) ? "1" : "0";
                        }
                    }

                    return $"{columnName} {sqlOperator} {value}";
                    // değer sayısal ise tırnak kullanma
                    // if (double.TryParse(value, out _))
                    // {
                    //     return $"{columnName} {sqlOperator} {value}";
                    // }
                    // else
                    // {
                    //     return $"{columnName} {sqlOperator} '{value}'";
                    // }
                }
            }
            else if (filter is LogicalFilterModel logical)
            {
                string left = ConvertFilterToSql(dbType, logical.Left);
                string right = ConvertFilterToSql(dbType, logical.Right);
                string op = logical.Operator.ToString().ToUpperInvariant();

                return $"({left} {op} {right})";
            }
            else
            {
                throw new NotSupportedException($"Filter type {filter.GetType()} is not supported.");
            }
        }

        public string BuildInsertSql(string tableName, string column, string value)
        {
            var insertSQL = $"INSERT INTO {tableName} ({column}) VALUES (@{column});";
            return insertSQL;
        }


        // ALTER TABLE ADD COLUMN SQL sorgusu oluşturma
        public string BuildAlterTableAddColumnSql(string dbType, string tableName, string columnName, string dataType)
        {
            string alterSQL;
            if (dbType == "oracle")
            {
                alterSQL = $"ALTER TABLE {tableName.ToUpperInvariant()} ADD ({columnName.ToUpperInvariant()} {dataType.ToUpperInvariant()} NULL);";
            }
            else
            {
                alterSQL = $"ALTER TABLE {tableName} ADD {columnName} {dataType} NULL;";
            }

            return alterSQL;
        }

        // CREATE TABLE SQL sorgusu oluşturma
        public string BuildCreateTableSql(string tableName, Dictionary<string, string> columnDataTypes, Dictionary<string, string> columnAliases)
        {
            var sqlBuilder = new StringBuilder();

            sqlBuilder.Append($"CREATE TABLE {tableName} (\n");
            int i = 0;
            foreach (var col in columnDataTypes)
            {
                string colName = columnAliases[col.Key];
                string colType = col.Value;
                sqlBuilder.Append($"    {colName} {colType} NULL");
                if (i < columnDataTypes.Count - 1)
                {
                    sqlBuilder.Append(",\n");
                }
                i++;
            }
            sqlBuilder.Append("\n);");

            return sqlBuilder.ToString();
        }
    }
}