using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DynamicDbQueryApi.Entities.Query;
using DynamicDbQueryApi.Helpers;
using DynamicDbQueryApi.Interfaces;

namespace DynamicDbQueryApi.Services
{
    public class InMemoryQueryService : IInMemoryQueryService
    {
        public IEnumerable<dynamic> ApplyQuery(List<dynamic> data, QueryModel model)
        {
            IEnumerable<dynamic> result = data;

            // FILTER uygula
            if (model.Filters != null)
            {
                result = result.Where(row => ExecuteFilter(row, model.Filters));
            }

            // FETCH (projection) uygula
            if (model.Columns != null && model.Columns.Any() && model.Columns[0].Expression != "*")
            {
                result = result.Select(row =>
                {
                    var dict = (IDictionary<string, object?>)row;
                    var projected = new Dictionary<string, object?>();
                    foreach (var col in model.Columns)
                    {
                        var key = col.Expression;
                        var alias = col.Alias ?? key;

                        // Fonksiyon kontrolü
                        var funcInfo = GetFunction(key);
                        if (funcInfo != null)
                        {
                            var funcName = funcInfo.Value.funcName;
                            var inner = funcInfo.Value.inner;
                            var args = StringHelpers.SplitByCommas(inner);
                            var value = ExecuteFunction(dict, funcName, args);
                            projected[alias] = value;
                        }
                        else
                        {
                            // Normal alan
                            key = key.Contains('.') ? key.Split('.').Last() : key;
                            if (dict.ContainsKey(key))
                            {
                                projected[alias] = dict[key];
                            }
                        }
                    }
                    return (dynamic)projected;
                });
            }

            // ORDERBY uygula
            if (model.OrderBy != null && model.OrderBy.Any())
            {
                foreach (var order in model.OrderBy)
                {
                    var column = order.Column;

                    // Fonksiyon kontrolü - eğer column bir fonksiyonsa, ExecuteFunction kullan
                    var funcInfo = GetFunction(column);

                    if (funcInfo != null)
                    {
                        var funcName = funcInfo.Value.funcName;
                        var inner = funcInfo.Value.inner;
                        var args = StringHelpers.SplitByCommas(inner);

                        // Fonksiyon için sıralama
                        result = order.Desc
                            ? result.OrderByDescending(r => ExecuteFunction((IDictionary<string, object?>)r, funcName, args))
                            : result.OrderBy(r => ExecuteFunction((IDictionary<string, object?>)r, funcName, args));
                    }
                    else
                    {
                        // Normal field için sıralama - mevcut yöntem
                        var key = column.Contains('.') ? column.Split('.').Last() : column;
                        result = order.Desc
                            ? result.OrderByDescending(r =>
                            {
                                var d = (IDictionary<string, object>)r;
                                return d.TryGetValue(key, out var val) ? val : null;
                            })
                            : result.OrderBy(r =>
                            {
                                var d = (IDictionary<string, object>)r;
                                return d.TryGetValue(key, out var val) ? val : null;
                            });
                    }
                }
            }

            // OFFSET/LIMIT uygula
            if (model.Offset.HasValue && model.Offset > 0)
                result = result.Skip(model.Offset.Value);
            if (model.Limit.HasValue && model.Limit > 0)
                result = result.Take(model.Limit.Value);

            return result;
        }

        private bool ExecuteFilter(dynamic row, FilterModel filter)
        {
            if (filter is ConditionFilterModel cond)
            {
                var dict = (IDictionary<string, object?>)row;
                var key = cond.Column;
                var compareValue = cond.Value?.Trim('\'', '"');

                // Fonksiyon kontrolü
                object? value;
                var funcInfo = GetFunction(key);
                if (funcInfo != null)
                {
                    var funcName = funcInfo.Value.funcName;
                    var inner = funcInfo.Value.inner;
                    var args = StringHelpers.SplitByCommas(inner);
                    value = ExecuteFunction(dict, funcName, args);
                }
                else
                {
                    // Normal alan
                    key = key.Contains('.') ? key.Split('.').Last() : key;
                    if (!dict.ContainsKey(key)) return false;
                    value = dict[key];
                }

                // Sağ taraf için fonksiyon kontrolü
                object? rightValue;
                if (compareValue != null)
                {
                    var rightFuncInfo = GetFunction(compareValue);
                    if (rightFuncInfo != null)
                    {
                        var rightFuncName = rightFuncInfo.Value.funcName;
                        var rightInner = rightFuncInfo.Value.inner;
                        var rightArgs = StringHelpers.SplitByCommas(rightInner);
                        rightValue = ExecuteFunction(dict, rightFuncName, rightArgs);
                    }
                    else
                    {
                        rightValue = compareValue;
                    }
                }
                else
                {
                    rightValue = null;
                }

                return CompareValues(value ?? string.Empty, rightValue, cond.Operator);
            }
            else if (filter is LogicalFilterModel logical)
            {
                var left = ExecuteFilter(row, logical.Left);
                var right = ExecuteFilter(row, logical.Right);
                return logical.Operator == LogicalOperator.And ? left && right : left || right;
            }
            return false;
        }

        private bool CompareValues(object left, object? right, ComparisonOperator op)
        {
            if (left == null && right == null)
            {
                return op == ComparisonOperator.Eq || op == ComparisonOperator.IsNull;
            }

            if (left == null)
            {
                return op == ComparisonOperator.IsNull;
            }

            if (right == null)
            {
                return op == ComparisonOperator.IsNotNull;
            }

            string leftStr = left?.ToString() ?? string.Empty;
            string rightStr = right?.ToString() ?? string.Empty;

            // Sayısal karşılaştırmalar için dönüşüm denemeleri
            bool isLeftNumeric = double.TryParse(leftStr, out double leftNum);
            bool isRightNumeric = double.TryParse(rightStr, out double rightNum);

            switch (op)
            {
                case ComparisonOperator.Eq:
                    if (isLeftNumeric && isRightNumeric)
                        return leftNum == rightNum;
                    return leftStr == rightStr;

                case ComparisonOperator.Neq:
                    if (isLeftNumeric && isRightNumeric)
                        return leftNum != rightNum;
                    return leftStr != rightStr;

                case ComparisonOperator.Gt:
                    if (isLeftNumeric && isRightNumeric)
                        return leftNum > rightNum;
                    return string.Compare(leftStr, rightStr) > 0;

                case ComparisonOperator.Gte:
                    if (isLeftNumeric && isRightNumeric)
                        return leftNum >= rightNum;
                    return string.Compare(leftStr, rightStr) >= 0;

                case ComparisonOperator.Lt:
                    if (isLeftNumeric && isRightNumeric)
                        return leftNum < rightNum;
                    return string.Compare(leftStr, rightStr) < 0;

                case ComparisonOperator.Lte:
                    if (isLeftNumeric && isRightNumeric)
                        return leftNum <= rightNum;
                    return string.Compare(leftStr, rightStr) <= 0;

                case ComparisonOperator.Contains:
                    return leftStr.Contains(rightStr);

                case ComparisonOperator.IContains:
                    return leftStr.Contains(rightStr, StringComparison.OrdinalIgnoreCase);

                case ComparisonOperator.NotContains:
                    return !leftStr.Contains(rightStr);

                case ComparisonOperator.NotIContains:
                    return !leftStr.Contains(rightStr, StringComparison.OrdinalIgnoreCase);

                case ComparisonOperator.BeginsWith:
                    return leftStr.StartsWith(rightStr);

                case ComparisonOperator.IBeginsWith:
                    return leftStr.StartsWith(rightStr, StringComparison.OrdinalIgnoreCase);

                case ComparisonOperator.NotBeginsWith:
                    return !leftStr.StartsWith(rightStr);

                case ComparisonOperator.NotIBeginsWith:
                    return !leftStr.StartsWith(rightStr, StringComparison.OrdinalIgnoreCase);

                case ComparisonOperator.EndsWith:
                    return leftStr.EndsWith(rightStr);

                case ComparisonOperator.IEndsWith:
                    return leftStr.EndsWith(rightStr, StringComparison.OrdinalIgnoreCase);

                case ComparisonOperator.NotEndsWith:
                    return !leftStr.EndsWith(rightStr);

                case ComparisonOperator.NotIEndsWith:
                    return !leftStr.EndsWith(rightStr, StringComparison.OrdinalIgnoreCase);

                case ComparisonOperator.Like:
                case ComparisonOperator.ILike:
                    {
                        string pattern = rightStr.Replace("%", ".*").Replace("_", ".");
                        var regex = op == ComparisonOperator.ILike ?
                            new Regex($"^{pattern}$", RegexOptions.IgnoreCase) :
                            new Regex($"^{pattern}$");
                        return regex.IsMatch(leftStr);
                    }

                case ComparisonOperator.NotLike:
                case ComparisonOperator.NotILike:
                    {
                        string pattern = rightStr.Replace("%", ".*").Replace("_", ".");
                        var regex = op == ComparisonOperator.NotILike ?
                            new Regex($"^{pattern}$", RegexOptions.IgnoreCase) :
                            new Regex($"^{pattern}$");
                        return !regex.IsMatch(leftStr);
                    }

                case ComparisonOperator.In:
                    {
                        var values = rightStr.Split(',').Select(s => s.Trim());
                        return values.Any(v => string.Equals(v, leftStr, StringComparison.OrdinalIgnoreCase));
                    }

                case ComparisonOperator.NotIn:
                    {
                        var values = rightStr.Split(',').Select(s => s.Trim());
                        return !values.Any(v => string.Equals(v, leftStr, StringComparison.OrdinalIgnoreCase));
                    }

                case ComparisonOperator.Between:
                    {
                        var parts = rightStr.Split(',');
                        if (parts.Length != 2) return false;

                        if (isLeftNumeric && double.TryParse(parts[0], out double lowerBound) &&
                            double.TryParse(parts[1], out double upperBound))
                        {
                            return leftNum >= lowerBound && leftNum <= upperBound;
                        }

                        return string.Compare(leftStr, parts[0].Trim()) >= 0 &&
                               string.Compare(leftStr, parts[1].Trim()) <= 0;
                    }

                case ComparisonOperator.NotBetween:
                    {
                        var parts = rightStr.Split(',');
                        if (parts.Length != 2) return true;

                        if (isLeftNumeric && double.TryParse(parts[0], out double lowerBound) &&
                            double.TryParse(parts[1], out double upperBound))
                        {
                            return leftNum < lowerBound || leftNum > upperBound;
                        }

                        return string.Compare(leftStr, parts[0].Trim()) < 0 ||
                               string.Compare(leftStr, parts[1].Trim()) > 0;
                    }

                default:
                    return false;
            }
        }

        // Kolonun bir fonksiyon olup olmadığını kontrol etme
        public (string funcName, string inner)? GetFunction(string column)
        {
            if (string.IsNullOrEmpty(column)) return null;

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
                return (funcName, body);
            }
            return null;
        }

        // Fonksiyonları çalıştırma
        private object? ExecuteFunction(IDictionary<string, object?> row, string functionName, List<string> args)
        {
            List<object?> evaluatedArgs = new List<object?>();
            foreach (var arg in args)
            {
                // Eğer argüman bir fonksiyonsa, önce onu çalıştır
                var nestedFunc = GetFunction(arg);
                if (nestedFunc != null)
                {
                    var nestedFuncName = nestedFunc.Value.funcName;
                    var nestedInner = nestedFunc.Value.inner;
                    var nestedArgs = StringHelpers.SplitByCommas(nestedInner);
                    evaluatedArgs.Add(ExecuteFunction(row, nestedFuncName, nestedArgs));
                }
                else if (arg == "*")
                {
                    evaluatedArgs.Add("*");
                }
                else if (arg.StartsWith("'") && arg.EndsWith("'"))
                {
                    evaluatedArgs.Add(arg.Substring(1, arg.Length - 2));
                }
                else if (int.TryParse(arg, out int intVal))
                {
                    evaluatedArgs.Add(intVal);
                }
                else if (double.TryParse(arg, out double doubleVal))
                {
                    evaluatedArgs.Add(doubleVal);
                }
                else if (bool.TryParse(arg, out bool boolVal))
                {
                    evaluatedArgs.Add(boolVal);
                }
                else if (DateTime.TryParse(arg, out DateTime dateVal))
                {
                    evaluatedArgs.Add(dateVal);
                }
                else
                {
                    // Alan adı
                    var key = arg.Contains('.') ? arg.Split('.').Last() : arg;
                    if (row.ContainsKey(key))
                    {
                        evaluatedArgs.Add(row[key]);
                    }
                    else
                    {
                        evaluatedArgs.Add(null);
                    }
                }
            }

            // String Fonksiyonları
            if (functionName == "LENGTH" || functionName == "LEN")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("LENGTH function requires 1 argument");
                return evaluatedArgs[0]?.ToString()?.Length ?? 0;
            }

            if (functionName == "SUBSTRING" || functionName == "SUBSTR")
            {
                if (evaluatedArgs.Count < 2 || evaluatedArgs.Count > 3)
                    throw new Exception("SUBSTRING function requires 2 or 3 arguments");

                string str = evaluatedArgs[0]?.ToString() ?? "";
                if (!int.TryParse(evaluatedArgs[1]?.ToString(), out int start))
                    return string.Empty;

                if (evaluatedArgs.Count == 2)
                {
                    return start < str.Length ? str.Substring(start) : string.Empty;
                }
                else
                {
                    if (!int.TryParse(evaluatedArgs[2]?.ToString(), out int length))
                        return string.Empty;

                    if (start < str.Length)
                    {
                        return start + length <= str.Length
                            ? str.Substring(start, length)
                            : str.Substring(start);
                    }
                    return string.Empty;
                }
            }

            if (functionName == "CONCAT")
            {
                if (evaluatedArgs.Count < 1) throw new Exception("CONCAT function requires at least 1 argument");
                return string.Join("", evaluatedArgs.Select(a => a?.ToString() ?? ""));
            }

            if (functionName == "UPPER")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("UPPER function requires 1 argument");
                return evaluatedArgs[0]?.ToString()?.ToUpper() ?? "";
            }

            if (functionName == "LOWER")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("LOWER function requires 1 argument");
                return evaluatedArgs[0]?.ToString()?.ToLower() ?? "";
            }

            if (functionName == "TRIM")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("TRIM function requires 1 argument");
                return evaluatedArgs[0]?.ToString()?.Trim() ?? "";
            }

            if (functionName == "LTRIM")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("LTRIM function requires 1 argument");
                return evaluatedArgs[0]?.ToString()?.TrimStart() ?? "";
            }

            if (functionName == "RTRIM")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("RTRIM function requires 1 argument");
                return evaluatedArgs[0]?.ToString()?.TrimEnd() ?? "";
            }

            if (functionName == "REPLACE")
            {
                if (evaluatedArgs.Count != 3) throw new Exception("REPLACE function requires 3 arguments");
                return evaluatedArgs[0]?.ToString()?.Replace(
                    evaluatedArgs[1]?.ToString() ?? "",
                    evaluatedArgs[2]?.ToString() ?? "") ?? "";
            }

            if (functionName == "INDEXOF")
            {
                if (evaluatedArgs.Count < 2 || evaluatedArgs.Count > 3)
                    throw new Exception("INDEXOF function requires 2 or 3 arguments");

                string str = evaluatedArgs[0]?.ToString() ?? "";
                string search = evaluatedArgs[1]?.ToString() ?? "";

                if (evaluatedArgs.Count == 2)
                {
                    return str.IndexOf(search);
                }
                else
                {
                    if (!int.TryParse(evaluatedArgs[2]?.ToString(), out int startIndex))
                        return -1;

                    return startIndex < str.Length ? str.IndexOf(search, startIndex) : -1;
                }
            }

            // Sayısal Fonksiyonlar
            if (functionName == "ABS")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("ABS function requires 1 argument");
                if (double.TryParse(evaluatedArgs[0]?.ToString(), out double num))
                    return Math.Abs(num);
                return null;
            }

            if (functionName == "CEIL" || functionName == "CEILING")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("CEIL function requires 1 argument");
                if (double.TryParse(evaluatedArgs[0]?.ToString(), out double num))
                    return Math.Ceiling(num);
                return null;
            }

            if (functionName == "FLOOR")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("FLOOR function requires 1 argument");
                if (double.TryParse(evaluatedArgs[0]?.ToString(), out double num))
                    return Math.Floor(num);
                return null;
            }

            if (functionName == "ROUND")
            {
                if (evaluatedArgs.Count < 1 || evaluatedArgs.Count > 2)
                    throw new Exception("ROUND function requires 1 or 2 arguments");

                if (!double.TryParse(evaluatedArgs[0]?.ToString(), out double num))
                    return null;

                if (evaluatedArgs.Count == 1)
                {
                    return Math.Round(num);
                }
                else
                {
                    if (!int.TryParse(evaluatedArgs[1]?.ToString(), out int decimals))
                        return Math.Round(num);

                    return Math.Round(num, decimals);
                }
            }

            if (functionName == "SQRT")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("SQRT function requires 1 argument");
                if (double.TryParse(evaluatedArgs[0]?.ToString(), out double num) && num >= 0)
                    return Math.Sqrt(num);
                return null;
            }

            if (functionName == "POWER")
            {
                if (evaluatedArgs.Count != 2) throw new Exception("POWER function requires 2 arguments");
                if (double.TryParse(evaluatedArgs[0]?.ToString(), out double baseNum) &&
                    double.TryParse(evaluatedArgs[1]?.ToString(), out double exponent))
                    return Math.Pow(baseNum, exponent);
                return null;
            }

            if (functionName == "MOD")
            {
                if (evaluatedArgs.Count != 2) throw new Exception("MOD function requires 2 arguments");
                if (double.TryParse(evaluatedArgs[0]?.ToString(), out double dividend) &&
                    double.TryParse(evaluatedArgs[1]?.ToString(), out double divisor) &&
                    divisor != 0)
                    return dividend % divisor;
                return null;
            }

            // Tarih Fonksiyonları
            if (functionName == "NOW" || functionName == "GETDATE" || functionName == "CURRENT_TIMESTAMP")
            {
                return DateTime.UtcNow;
            }

            if (functionName == "TODAY" || functionName == "CURRENT_DATE")
            {
                return DateTime.UtcNow.Date;
            }

            if (functionName == "TIME" || functionName == "CURRENT_TIME")
            {
                var now = DateTime.UtcNow;
                return new TimeSpan(now.Hour, now.Minute, now.Second);
            }

            if (functionName == "DATEADD")
            {
                if (evaluatedArgs.Count != 3) throw new Exception("DATEADD function requires 3 arguments");

                string interval = evaluatedArgs[0]?.ToString()?.ToLower() ?? "";

                if (!DateTime.TryParse(evaluatedArgs[1]?.ToString(), out DateTime date) ||
                    !int.TryParse(evaluatedArgs[2]?.ToString(), out int value))
                    return null;

                return interval switch
                {
                    "year" => date.AddYears(value),
                    "month" => date.AddMonths(value),
                    "week" => date.AddDays(value * 7),
                    "day" => date.AddDays(value),
                    "hour" => date.AddHours(value),
                    "minute" => date.AddMinutes(value),
                    "second" => date.AddSeconds(value),
                    _ => null
                };
            }

            if (functionName == "DATEDIFF")
            {
                if (evaluatedArgs.Count != 3) throw new Exception("DATEDIFF function requires 3 arguments");

                string interval = evaluatedArgs[0]?.ToString()?.ToLower() ?? "";

                if (!DateTime.TryParse(evaluatedArgs[1]?.ToString(), out DateTime date1) ||
                    !DateTime.TryParse(evaluatedArgs[2]?.ToString(), out DateTime date2))
                    return null;

                var diff = date2.Subtract(date1);

                return interval switch
                {
                    "year" => date2.Year - date1.Year,
                    "month" => (date2.Year - date1.Year) * 12 + date2.Month - date1.Month,
                    "week" => (int)Math.Floor(diff.TotalDays / 7),
                    "day" => (int)diff.TotalDays,
                    "hour" => (int)diff.TotalHours,
                    "minute" => (int)diff.TotalMinutes,
                    "second" => (int)diff.TotalSeconds,
                    _ => null
                };
            }

            if (functionName == "DATENAME")
            {
                if (evaluatedArgs.Count != 2) throw new Exception("DATENAME function requires 2 arguments");

                string part = evaluatedArgs[0]?.ToString()?.ToLower() ?? "";

                if (!DateTime.TryParse(evaluatedArgs[1]?.ToString(), out DateTime date))
                    return null;

                return part switch
                {
                    "month" => date.ToString("MMMM"),
                    "day" => date.ToString("dddd"),
                    "year" => date.Year.ToString(),
                    "hour" => date.ToString("HH"),
                    "minute" => date.ToString("mm"),
                    "second" => date.ToString("ss"),
                    "dayofweek" => date.ToString("dddd"),
                    "dayofyear" => date.DayOfYear.ToString(),
                    _ => null
                };
            }

            if (functionName == "DAY")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("DAY function requires 1 argument");
                return DateTime.TryParse(evaluatedArgs[0]?.ToString(), out DateTime date) ? date.Day : null;
            }

            if (functionName == "MONTH")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("MONTH function requires 1 argument");
                return DateTime.TryParse(evaluatedArgs[0]?.ToString(), out DateTime date) ? date.Month : null;
            }

            if (functionName == "YEAR")
            {
                if (evaluatedArgs.Count != 1) throw new Exception("YEAR function requires 1 argument");
                return DateTime.TryParse(evaluatedArgs[0]?.ToString(), out DateTime date) ? date.Year : null;
            }

            // Null Fonksiyonları
            if (functionName == "COALESCE" || functionName == "IFNULL" || functionName == "NVL")
            {
                if (evaluatedArgs.Count < 1) throw new Exception($"{functionName} function requires at least 1 argument");

                foreach (var arg in evaluatedArgs)
                {
                    if (arg != null)
                        return arg;
                }

                return null;
            }

            // Koşullu fonksiyon
            if (functionName == "IFS" || functionName == "CASE")
            {
                if (evaluatedArgs.Count < 2 || evaluatedArgs.Count % 2 != 0)
                    throw new Exception("IFS function requires pairs of condition,value and optional default");

                for (int i = 0; i < evaluatedArgs.Count - 1; i += 2)
                {
                    bool conditionMet = false;

                    if (evaluatedArgs[i] is bool b)
                    {
                        conditionMet = b;
                    }
                    else if (bool.TryParse(evaluatedArgs[i]?.ToString(), out bool parsedBool))
                    {
                        conditionMet = parsedBool;
                    }
                    else if (evaluatedArgs[i]?.ToString()?.Equals("true", StringComparison.OrdinalIgnoreCase) == true)
                    {
                        conditionMet = true;
                    }

                    if (conditionMet)
                    {
                        return evaluatedArgs[i + 1];
                    }
                }

                // Son değer default değer
                if (evaluatedArgs.Count % 2 == 1)
                {
                    return evaluatedArgs.Last();
                }

                return null;
            }

            // Desteklenmeyen fonksiyon
            throw new Exception($"Unsupported function: {functionName}");
        }

    }
}