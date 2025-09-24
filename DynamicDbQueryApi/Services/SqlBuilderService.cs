using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DynamicDbQueryApi.DTOs;
using DynamicDbQueryApi.Entities;
using DynamicDbQueryApi.Entities.Query;
using DynamicDbQueryApi.Interfaces;
using DynamicDbQueryApi.Helpers;

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
                    var columnSql = BuildSelectColumn(dbType, c);
                    columnsBuilder.Append(columnSql);
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
                StringBuilder groupByBuilder = new StringBuilder();
                for (int i = 0; i < model.GroupBy.Count; i++)
                {
                    var gb = model.GroupBy[i];
                    if (i > 0) groupByBuilder.Append(", ");

                    // Kolon bir fonksiyon ise fonksiyonu ve içindeki argümanları işle
                    var groupByColumnSql = BuildGroupByColumn(dbType, gb);
                    groupByBuilder.Append(groupByColumnSql);
                }
                var groupByClause = groupByBuilder.ToString();
                sql += $" GROUP BY {groupByClause}";
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
                StringBuilder orderByBuilder = new StringBuilder();
                for (int i = 0; i < model.OrderBy.Count; i++)
                {
                    var ob = model.OrderBy[i];
                    if (i > 0) orderByBuilder.Append(", ");

                    // Kolon bir fonksiyon ise fonksiyonu ve içindeki argümanları işle
                    var orderByColumnSql = BuildOrderByColumn(dbType, ob);
                    orderByBuilder.Append(orderByColumnSql);
                }
                var orderByClause = orderByBuilder.ToString();
                sql += $" ORDER BY {orderByClause}";
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
            var start = Regex.Match(column, @"^(?<func>[A-Za-z_][A-Za-z0-9_]*)\s*\(", RegexOptions.IgnoreCase);
            if (start.Success)
            {
                // Parantez başlangıç indeksini ve kapanış indeksini bul
                int openIdx = start.Index + start.Length - 1;
                int bodyStart = openIdx + 1;
                int closeIdx = StringHelpers.FindClosingParenthesis(column, openIdx);
                if (closeIdx == -1) throw new Exception("Could not find closing parenthesis for function in column.");

                var body = column.Substring(bodyStart, closeIdx - bodyStart).Trim();

                var funcName = start.Groups["func"].Value.ToUpperInvariant();
                // Fonksiyonu parse et
                return (funcName, body);
            }
            return null;
        }

        public string BuildSelectColumn(string dbType, QueryColumnModel column)
        {
            // Kolon bir fonksiyon ise fonksiyonu ve içindeki argümanları işle
            var funcInfo = GetFunction(column.Expression);
            if (funcInfo != null)
            {
                var funcName = funcInfo.Value.funcName;
                var inner = funcInfo.Value.inner;

                var args = StringHelpers.SplitByCommas(inner);

                // Fonksiyonu oluştur
                var funcSql = BuildFunction(dbType, funcName, args);
                if (!string.IsNullOrEmpty(column.Alias))
                {
                    return $"{funcSql} AS {column.Alias}";
                }
                else
                {
                    return funcSql;
                }
            }
            else if (IsDateOrTimestamp(column.Expression))
            {
                Console.WriteLine($"Text '{column.Expression}' looks like a date or timestamp. Converting.");

                return ConvertStringToDate(dbType, column.Expression) + (string.IsNullOrEmpty(column.Alias) ? "" : $" AS {column.Alias}");
            }
            else
            {
                if (!string.IsNullOrEmpty(column.Alias))
                {
                    return $"{column.Expression} AS {column.Alias}";
                }
                else
                {
                    return column.Expression;
                }
            }
        }

        public string BuildGroupByColumn(string dbType, string column)
        {
            // Kolon bir fonksiyon ise fonksiyonu ve içindeki argümanları işle
            var funcInfo = GetFunction(column);
            if (funcInfo != null)
            {
                var funcName = funcInfo.Value.funcName;
                var inner = funcInfo.Value.inner;
                var args = StringHelpers.SplitByCommas(inner);
                var funcSql = BuildFunction(dbType, funcName, args);
                return funcSql;
            }
            else
            {
                return column;
            }
        }

        public string BuildOrderByColumn(string dbType, OrderByModel orderBy)
        {
            // Kolon bir fonksiyon ise fonksiyonu ve içindeki argümanları işle
            var funcInfo = GetFunction(orderBy.Column);
            if (funcInfo != null)
            {
                var funcName = funcInfo.Value.funcName;
                var inner = funcInfo.Value.inner;
                var args = StringHelpers.SplitByCommas(inner);
                var funcSql = BuildFunction(dbType, funcName, args);
                return $"{funcSql} {(orderBy.Desc ? "DESC" : "ASC")}";
            }
            else
            {
                return $"{orderBy.Column} {(orderBy.Desc ? "DESC" : "ASC")}";
            }
        }

        // Fonksiyonları dbType'a göre oluşturma
        public string BuildFunction(string dbType, string functionName, List<string> args)
        {
            // Eğer argumanlarda fonksiyonlar varsa onları da işle
            for (int i = 0; i < args.Count; i++)
            {
                if (functionName.Equals("IF", StringComparison.OrdinalIgnoreCase) && i == 0)
                {
                    // IF fonksiyonunun ilk argümanı bir filtre olabilir, bu yüzden işlemiyoruz
                    continue;
                }

                if (functionName.Equals("CASE", StringComparison.OrdinalIgnoreCase) || functionName.Equals("IFS", StringComparison.OrdinalIgnoreCase))
                {
                    // CASE/IFS fonksiyonunun çift argümanları condition olabilir, bu yüzden işlemiyoruz
                    if (i % 2 == 0) continue;
                }
                var funcInfo = GetFunction(args[i]);
                if (funcInfo != null)
                {
                    var funcName = funcInfo.Value.funcName;
                    var inner = funcInfo.Value.inner;
                    var innerArgs = StringHelpers.SplitByCommas(inner);
                    var funcSql = BuildFunction(dbType, funcName, innerArgs);
                    args[i] = funcSql;
                }
            }

            // Conditional function
            if (functionName.Equals("IF", StringComparison.OrdinalIgnoreCase))
            {
                var condition = args[0];
                var trueValue = args[1];
                var falseValue = args[2];

                var options = new JsonSerializerOptions
                {
                    WriteIndented = true
                };
                FilterModel deserializedFilter = JsonSerializer.Deserialize<FilterModel>(condition, options)!;

                var conditionSql = ConvertFilterToSql(dbType, deserializedFilter);

                if (dbType == "mysql")
                    return $"IF({conditionSql}, {trueValue}, {falseValue})";
                if (dbType == "mssql" || dbType == "sqlserver")
                    return $"IIF({conditionSql}, {trueValue}, {falseValue})";

                // PostgreSQL / Oracle
                return $"CASE WHEN {conditionSql} THEN {trueValue} ELSE {falseValue} END";
            }

            if (functionName.Equals("CASE", StringComparison.OrdinalIgnoreCase) ||
                functionName.Equals("IFS", StringComparison.OrdinalIgnoreCase))
            {

                var whenParts = new List<string>();
                for (int i = 0; i < args.Count - 1; i += 2)
                {
                    var condArg = args[i].Trim();
                    var valArg = args[i + 1];

                    var condSql = condArg;
                    var options = new JsonSerializerOptions
                    {
                        WriteIndented = true
                    };
                    FilterModel deserializedFilter = JsonSerializer.Deserialize<FilterModel>(condArg, options)!;

                    condSql = ConvertFilterToSql(dbType, deserializedFilter);

                    whenParts.Add($"WHEN {condSql} THEN {valArg}");
                }
                var elseVal = args[^1];
                return $"CASE {string.Join(" ", whenParts)} ELSE {elseVal} END";
            }

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

                var joinedArgs = string.Join(", ", args);
                return $"{functionName}({joinedArgs})";
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
            var dateFuncs = new[] { "NOW", "GETDATE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "TODAY", "CURRENT_TIME", "TIME", "DATEADD", "DATEDIFF", "DATENAME", "DATEPART", "DAY", "MONTH", "YEAR" };
            if (dateFuncs.Contains(functionName))
            {
                if ((functionName.Equals("NOW", StringComparison.OrdinalIgnoreCase) || functionName.Equals("GETDATE", StringComparison.OrdinalIgnoreCase) || functionName.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)) && args.Count == 0)
                {
                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        return "now() AT TIME ZONE 'UTC'";
                    }
                    else if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return "GETUTCDATE()";
                    }
                    else if (dbType == "mysql")
                    {
                        return "UTC_TIMESTAMP()";
                    }
                    else if (dbType == "oracle")
                    {
                        return "SYS_EXTRACT_UTC(SYSTIMESTAMP)";
                    }
                    return "CURRENT_TIMESTAMP";
                }
                else if ((functionName.Equals("NOW", StringComparison.OrdinalIgnoreCase) || functionName.Equals("GETDATE", StringComparison.OrdinalIgnoreCase) || functionName.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)) && args.Count == 1)
                {
                    var tz = args[0];

                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        return $"now() AT TIME ZONE {tz}";
                    }
                    else if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return $"GETUTCDATE() AT TIME ZONE {tz}";
                    }
                    else if (dbType == "mysql")
                    {
                        return $"CONVERT_TZ(UTC_TIMESTAMP(), 'UTC', {tz})";
                    }
                    else if (dbType == "oracle")
                    {
                        return $"SYS_EXTRACT_UTC(SYSTIMESTAMP) AT TIME ZONE {tz}";
                    }
                    return "CURRENT_TIMESTAMP";
                }
                else if ((functionName.Equals("CURRENT_DATE", StringComparison.OrdinalIgnoreCase) ||
          functionName.Equals("TODAY", StringComparison.OrdinalIgnoreCase)) && args.Count == 0)
                {
                    // UTC date
                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        return "(CURRENT_DATE AT TIME ZONE 'UTC')::date";
                        // return "TO_CHAR(CURRENT_DATE AT TIME ZONE 'UTC', 'YYYY-MM-DD')";
                    }
                    else if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return "FORMAT(CAST(GETUTCDATE() AS DATE), 'yyyy-MM-dd')";
                    }
                    else if (dbType == "mysql")
                    {
                        return "DATE_FORMAT(UTC_DATE(), '%Y-%m-%d')";
                    }
                    else if (dbType == "oracle")
                    {
                        return "TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP))";
                        // return "TO_CHAR(TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP)), 'YYYY-MM-DD')";
                    }
                    return "CURRENT_DATE";
                }
                else if ((functionName.Equals("CURRENT_DATE", StringComparison.OrdinalIgnoreCase) ||
                          functionName.Equals("TODAY", StringComparison.OrdinalIgnoreCase)) && args.Count == 1)
                {
                    var tz = args[0];

                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        return $"(now() AT TIME ZONE {tz})::date";
                        // return $"TO_CHAR((now() AT TIME ZONE {tz})::date, 'YYYY-MM-DD')";
                    }
                    else if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return $"FORMAT(CAST(GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE {tz} AS date), 'yyyy-MM-dd')";
                    }
                    else if (dbType == "mysql")
                    {
                        return $"DATE_FORMAT(CONVERT_TZ(UTC_TIMESTAMP(), 'UTC', {tz}), '%Y-%m-%d')";
                    }
                    else if (dbType == "oracle")
                    {
                        return $"TRUNC(CAST(SYSTIMESTAMP AT TIME ZONE {tz} AS DATE))";
                        // return $"TO_CHAR(TRUNC(CAST(SYSTIMESTAMP AT TIME ZONE {tz} AS DATE)), 'YYYY-MM-DD')";
                    }
                    return "CURRENT_DATE";
                }
                else if ((functionName.Equals("CURRENT_TIME", StringComparison.OrdinalIgnoreCase) || functionName.Equals("TIME", StringComparison.OrdinalIgnoreCase)) && args.Count == 0)
                {
                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        return "TO_CHAR(NOW() AT TIME ZONE 'UTC', 'HH24:MI:SS')";
                    }
                    else if (dbType == "mysql")
                    {
                        return "UTC_TIME()";
                    }
                    else if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return "CONVERT (TIME, CURRENT_TIMESTAMP)";
                    }
                    else if (dbType == "oracle")
                    {
                        return "TO_CHAR(SYSDATE, 'HH24:MI:SS')";
                    }
                    else
                    {
                        return "CURRENT_TIME";
                    }
                }
                else if ((functionName.Equals("CURRENT_TIME", StringComparison.OrdinalIgnoreCase) ||
          functionName.Equals("TIME", StringComparison.OrdinalIgnoreCase)) && args.Count == 1)
                {
                    var tz = args[0];

                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        return $"TO_CHAR(NOW() AT TIME ZONE {tz}, 'HH24:MI:SS')";
                    }
                    else if (dbType == "mysql")
                    {
                        return $"DATE_FORMAT(CONVERT_TZ(UTC_TIMESTAMP(), 'UTC', {tz}), '%H:%i:%s')";
                    }
                    else if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return $"FORMAT(GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE {tz}, 'HH:mm:ss')";
                    }
                    else if (dbType == "oracle")
                    {
                        return $"TO_CHAR(SYSTIMESTAMP AT TIME ZONE {tz}, 'HH24:MI:SS')";
                    }
                    else
                    {
                        return "CURRENT_TIME";
                    }
                }
                else if (functionName.Equals("DATEADD", StringComparison.OrdinalIgnoreCase) && args.Count == 3)
                {
                    var interval = args[0].Trim('\'').ToUpperInvariant();
                    var number = args[2];
                    var date = args[1];

                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        if (date.StartsWith("'") && date.EndsWith("'"))
                        {
                            date = ConvertStringToDate(dbType, date);
                        }
                        return $"({date} + INTERVAL '{number} {interval}')";
                    }
                    else if (dbType == "mysql")
                    {
                        return $"DATE_ADD({date}, INTERVAL {number} {interval})";
                    }
                    else if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return $"DATEADD({interval}, {number}, {date})";
                    }
                    else if (dbType == "oracle")
                    {
                        if (date.StartsWith("'") && date.EndsWith("'"))
                        {
                            date = ConvertStringToDate(dbType, date);
                        }
                        if (interval == "SECOND" || interval == "SECONDS")
                        {
                            return $"({date} + {number} / 86400)";
                        }
                        else if (interval == "MINUTE" || interval == "MINUTES")
                        {
                            return $"({date} + {number} / 1440)";
                        }
                        else if (interval == "HOUR" || interval == "HOURS")
                        {
                            return $"({date} + {number} / 24)";
                        }
                        else if (interval == "DAY" || interval == "DAYS")
                        {
                            return $"({date} + {number})";
                        }
                        else if (interval == "WEEK" || interval == "WEEKS")
                        {
                            return $"({date} + {number} * 7)";
                        }
                        else if (interval == "MONTH" || interval == "MONTHS")
                        {
                            return $"ADD_MONTHS({date}, {number})";
                        }
                        else if (interval == "YEAR" || interval == "YEARS")
                        {
                            return $"ADD_MONTHS({date}, {number} * 12)";
                        }
                        else
                        {
                            throw new NotSupportedException($"Interval {interval} is not supported in Oracle DATEADD emulation.");
                        }
                    }
                }
                else if (functionName.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase) && args.Count == 3)
                {
                    var interval = args[0].Trim('\'').ToUpperInvariant();
                    var startDate = args[1];
                    var endDate = args[2];

                    if (dbType == "postgresql" || dbType == "postgres")
                    {
                        if (startDate.StartsWith("'") && startDate.EndsWith("'"))
                        {
                            startDate = ConvertStringToDate(dbType, startDate);
                        }
                        if (endDate.StartsWith("'") && endDate.EndsWith("'"))
                        {
                            endDate = ConvertStringToDate(dbType, endDate);
                        }
                        if (interval == "SECOND")
                        {
                            return $"FLOOR(EXTRACT(EPOCH FROM ({args[2]}::TIMESTAMP - {args[1]}::TIMESTAMP)))";
                        }
                        else if (interval == "MINUTE")
                        {
                            return $"FLOOR(EXTRACT(EPOCH FROM ({args[2]}::TIMESTAMP - {args[1]}::TIMESTAMP)) / 60)";
                        }
                        else if (interval == "HOUR")
                        {
                            return $"FLOOR(EXTRACT(EPOCH FROM ({args[2]}::TIMESTAMP - {args[1]}::TIMESTAMP)) / 3600)";
                        }
                        else if (interval == "DAY")
                        {
                            return $"({endDate} - {startDate})";
                        }
                        else if (interval == "WEEK")
                        {
                            return $"FLOOR((({endDate} - {startDate})::double precision) / 7)";
                        }
                        else if (interval == "MONTH")
                        {
                            return $"(DATE_PART('year', AGE({endDate}, {startDate})) * 12 + DATE_PART('month', AGE({endDate}, {startDate})))";
                        }
                        else if (interval == "YEAR")
                        {
                            return $"DATE_PART('year', AGE({endDate}, {startDate}))";
                        }
                        else
                        {
                            throw new NotSupportedException($"Interval {interval} is not supported in PostgreSQL DATEDIFF emulation.");
                        }
                    }
                    else if (dbType == "mysql")
                    {
                        return $"TIMESTAMPDIFF({interval}, {startDate}, {endDate})";
                    }
                    else if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        var diff = $"DATEDIFF({interval}, {startDate}, {endDate})";
                        return
                            $"CASE WHEN {endDate} >= {startDate} " +
                            $"THEN {diff} - CASE WHEN DATEADD(YEAR, {diff}, {startDate}) > {endDate} THEN 1 ELSE 0 END " +
                            $"ELSE {diff} + CASE WHEN DATEADD(YEAR, {diff}, {startDate}) < {endDate} THEN 1 ELSE 0 END END";

                    }
                    else if (dbType == "oracle")
                    {
                        if (startDate.StartsWith("'") && startDate.EndsWith("'"))
                        {
                            startDate = ConvertStringToDate(dbType, startDate);
                        }
                        if (endDate.StartsWith("'") && endDate.EndsWith("'"))
                        {
                            endDate = ConvertStringToDate(dbType, endDate);
                        }
                        if (interval == "SECOND")
                        {
                            return $"FLOOR(({endDate} - {startDate}) * 86400)";
                        }
                        else if (interval == "MINUTE")
                        {
                            return $"FLOOR(({endDate} - {startDate}) * 1440)";
                        }
                        else if (interval == "HOUR")
                        {
                            return $"FLOOR(({endDate} - {startDate}) * 24)";
                        }
                        else if (interval == "DAY")
                        {
                            return $"({endDate} - {startDate})";
                        }
                        else if (interval == "WEEK")
                        {
                            return $"FLOOR(({endDate} - {startDate}) / 7)";
                        }
                        else if (interval == "MONTH")
                        {
                            return $"FLOOR(MONTHS_BETWEEN({endDate}, {startDate}))";
                        }
                        else if (interval == "YEAR")
                        {
                            return $"FLOOR(MONTHS_BETWEEN({endDate}, {startDate}) / 12)";
                        }
                        else
                        {
                            throw new NotSupportedException($"Interval {interval} is not supported in Oracle DATEDIFF emulation.");
                        }
                    }
                }
                else if ((functionName.Equals("DAY", StringComparison.OrdinalIgnoreCase) || functionName.Equals("MONTH", StringComparison.OrdinalIgnoreCase) || functionName.Equals("YEAR", StringComparison.OrdinalIgnoreCase)) && args.Count == 1)
                {
                    var date = args[0];

                    if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        return $"{functionName}({date})";
                    }
                    else if (dbType == "postgresql" || dbType == "postgres")
                    {
                        // date variable eğer '' ise DATE ya da TIMESTAMP'a cast et
                        if (date.StartsWith("'") && date.EndsWith("'"))
                        {
                            date = ConvertStringToDate(dbType, date);
                        }

                        return $"EXTRACT({functionName} FROM {date})";
                    }
                    else if (dbType == "mysql")
                    {
                        return $"{functionName}({date})";
                    }
                    else if (dbType == "oracle")
                    {
                        if (date.StartsWith("'") && date.EndsWith("'"))
                        {
                            date = ConvertStringToDate(dbType, date);
                        }
                        return $"EXTRACT({functionName} FROM {date})";
                    }
                }
                else if ((functionName.Equals("DATENAME", StringComparison.OrdinalIgnoreCase) || functionName.Equals("TO_CHAR", StringComparison.OrdinalIgnoreCase)) && args.Count == 2)
                {
                    var interval = args[0].Trim('\'').ToUpperInvariant();
                    var date = args[1];

                    if (dbType == "mssql" || dbType == "sqlserver")
                    {
                        if (interval == "DAY")
                        {
                            interval = "WEEKDAY";
                        }

                        return $"DATENAME({interval}, {date})";
                    }
                    else if (dbType == "postgresql" || dbType == "postgres")
                    {
                        if (date.StartsWith("'") && date.EndsWith("'"))
                        {
                            date = ConvertStringToDate(dbType, date);
                        }
                        return $"TRIM(TO_CHAR({date}, '{interval}'))";
                    }
                    else if (dbType == "mysql")
                    {
                        if (interval == "MONTH")
                        {
                            return $"MONTHNAME({date})";
                        }
                        else if (interval == "DAY")
                        {
                            return $"DAYNAME({date})";
                        }
                        else
                        {
                            throw new NotSupportedException($"Interval {interval} is not supported in MySQL DATENAME emulation.");
                        }
                    }
                    else if (dbType == "oracle")
                    {
                        if (date.StartsWith("'") && date.EndsWith("'"))
                        {
                            date = ConvertStringToDate(dbType, date);
                        }
                        return $"TRIM(TO_CHAR({date}, '{interval}'))";
                    }
                }
            }

            throw new NotSupportedException($"Function {functionName} with {args.Count} args is not supported.");
        }

        // Oracle String to DATE/TIMESTAMP dönüşümü
        public string ConvertStringToDate(string dbType, string dateString)
        {
            if (!IsDateOrTimestamp(dateString))
            {
                return dateString;
            }

            if (dbType == "oracle")
            {
                // Tarih formatını belirle
                if (dateString.Contains(":"))
                {
                    dateString = dateString.Replace("T", " "); // 'T' karakterini boşlukla değiştir
                    return $"TO_TIMESTAMP({dateString}, 'YYYY-MM-DD HH24:MI:SS')";
                }
                else
                {
                    return $"TO_DATE({dateString}, 'YYYY-MM-DD')";
                }
            }
            else if (dbType == "postgresql" || dbType == "postgres")
            {
                if (dateString.Contains(":"))
                {
                    return $"{dateString}::TIMESTAMP";
                }
                else
                {
                    return $"{dateString}::DATE";
                }
            }
            else
            {
                return $"{dateString}";
            }
        }

        private static bool IsDateOrTimestamp(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return false;
            s = s.Trim();

            // 'YYYY-MM-DD' ya da YYYY-MM-DD
            var dateOnly = new Regex(@"^'?(\d{4}-\d{2}-\d{2})'?$", RegexOptions.Compiled);

            // 'YYYY-MM-DD HH:MM:SS' ya da 'YYYY-MM-DDTHH:MM:SS' ya da YYYY-MM-DD HH:MM:SS ya da YYYY-MM-DDTHH:MM:SS
            var tsSpace = new Regex(@"^'?(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})(\.\d{1,3})?'?$", RegexOptions.Compiled);
            var tsIso = new Regex(@"^'?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d{1,3})?(Z|[+\-]\d{2}:\d{2})?'?$", RegexOptions.Compiled);

            return dateOnly.IsMatch(s) || tsSpace.IsMatch(s) || tsIso.IsMatch(s);
        }

        public string ConcatExpressions(string dbType, List<string> expressions)
        {
            if (expressions == null || expressions.Count == 0) return "";

            if (dbType == "oracle")
            {
                return string.Join(" || ", expressions);
            }
            else if (dbType == "mysql")
            {
                List<string> modifiedArgs = new List<string>();
                foreach (var arg in expressions)
                {
                    // null koruması ekle
                    modifiedArgs.Add($"COALESCE({arg}, '')");
                }
                return $"CONCAT({string.Join(", ", modifiedArgs)})";
            }
            else
            {
                return $"CONCAT({string.Join(", ", expressions)})";
            }
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
                    List<string> args;
                    if (string.IsNullOrEmpty(inner))
                    {
                        args = new List<string>();
                    }
                    else
                    {
                        args = StringHelpers.SplitByCommas(inner);
                    }
                    columnName = BuildFunction(dbType, funcName, args);
                }

                var value = condition.Value;
                var funcValueInfo = value != null ? GetFunction(value) : null;
                if (funcValueInfo != null)
                {
                    var funcName = funcValueInfo.Value.funcName;
                    var inner = funcValueInfo.Value.inner;
                    var args = StringHelpers.SplitByCommas(inner);
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

                    // Eğer value fonksiyon ise CONCAT ile 
                    if (funcValueInfo != null)
                    {
                        if (comparisonOperator == ComparisonOperator.Like)
                        {
                            valueRaw = value.ToString();
                        }
                        else if (comparisonOperator == ComparisonOperator.Contains)
                        {
                            valueRaw = ConcatExpressions(dbType, new List<string> { "'%'", value.ToString(), "'%'" });
                        }
                        else if (comparisonOperator == ComparisonOperator.BeginsWith)
                        {
                            valueRaw = ConcatExpressions(dbType, new List<string> { value.ToString(), "'%'" });
                        }
                        else if (comparisonOperator == ComparisonOperator.EndsWith)
                        {
                            valueRaw = ConcatExpressions(dbType, new List<string> { "'%'", value.ToString() });
                        }
                        return $"{columnName} LIKE {valueRaw}";
                    }
                    else
                    {
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
        public string BuildCreateTableSql(string dbType, string tableName, Dictionary<string, string> columnDataTypes)
        {
            var sqlBuilder = new StringBuilder();

            sqlBuilder.Append($"CREATE TABLE {tableName} (\n");
            int i = 0;
            foreach (var col in columnDataTypes)
            {
                string colName = col.Key;
                string colType = col.Value;
                if (dbType == "oracle")
                {
                    sqlBuilder.Append($"    {colName.ToUpperInvariant()} {colType.ToUpperInvariant()}");
                }
                else
                {
                    sqlBuilder.Append($"    {colName} {colType} NULL");
                }

                if (i < columnDataTypes.Count - 1)
                {
                    sqlBuilder.Append(",\n");
                }
                i++;
            }
            sqlBuilder.Append("\n)");

            return sqlBuilder.ToString();
        }
    }
}