using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DynamicDbQueryApi.Entities.Query;
using DynamicDbQueryApi.Helpers;
using DynamicDbQueryApi.Interfaces;
using MongoDB.Bson;

namespace DynamicDbQueryApi.Services
{
    public class MongoPipelineBuilderService : IMongoPipelineBuilderService
    {
        private readonly ILogger<MongoPipelineBuilderService> _logger;
        private BsonDocument? _addFieldsDoc;
        public MongoPipelineBuilderService(ILogger<MongoPipelineBuilderService> logger)
        {
            _logger = logger;
        }

        public List<BsonDocument> BuildPipeline(QueryModel model)
        {
            // MongoDB pipeline oluşturma işlemleri
            var stages = new List<BsonDocument>();
            string collectionName = model.Table;

            // 1) INCLUDE (LOOKUP + UNWIND) İşlemleri
            if (model.Includes != null && model.Includes.Any())
            {
                foreach (var inc in model.Includes)
                {
                    var (lookup, unwind) = BuildLookupStage(inc);
                    stages.Add(lookup);
                    if (unwind != null)
                        stages.Add(unwind);
                }
            }

            // ADD FIELDS dokumanı (fonksiyonlar için)
            var addFieldsDoc = new BsonDocument();
            _addFieldsDoc = addFieldsDoc;
            stages.Add(new BsonDocument("$addFields", addFieldsDoc));
            var groupDoc = new BsonDocument();

            // 2) FILTER (MATCH) İşlemi
            if (model.Filters != null)
            {
                var matchDoc = BuildMatchFromFilter(addFieldsDoc, groupDoc, collectionName, model.Filters, isAfterGroup: false);
                if (matchDoc != null && !matchDoc.IsBsonNull && matchDoc.ElementCount > 0)
                {
                    stages.Add(new BsonDocument("$match", matchDoc));
                }
            }

            // GROUP dokumanı (group by için)
            stages.Add(new BsonDocument("$group", groupDoc));

            // 3) GROUP BY (GROUP) İşlemi
            if (model.GroupBy != null && model.GroupBy.Any())
            {
                BuildGroupByStage(addFieldsDoc, groupDoc, collectionName, model.GroupBy);

                // 4) HAVING (MATCH) İşlemi - Group sonrası filtreleme
                if (model.Having != null)
                {
                    var havingMatch = BuildMatchFromFilter(addFieldsDoc, groupDoc, collectionName, model.Having, isAfterGroup: true);
                    if (havingMatch != null && havingMatch.ElementCount > 0)
                        stages.Add(new BsonDocument("$match", havingMatch));
                }

                // 5) FETCH (PROJECT) İşlemi - Group sonrası projection
                var proj = new BsonDocument();
                foreach (var col in model.Columns)
                {
                    var columnName = col.Expression;
                    var alias = string.IsNullOrWhiteSpace(col.Alias) ? StringHelpers.NormalizeString(columnName) : col.Alias;
                    var bsonValue = GetFieldName(addFieldsDoc, groupDoc, collectionName, columnName, alias, isAfterGroup: true);
                    proj[alias] = bsonValue;
                }

                stages.Add(new BsonDocument("$project", proj));
            }
            else
            {
                // 6) FETCH (PROJECT) İşlemi - Group yoksa direkt projection
                if (model.Columns != null && model.Columns.Any())
                {
                    var proj = new BsonDocument();
                    foreach (var col in model.Columns)
                    {
                        var columnName = col.Expression;
                        var alias = string.IsNullOrWhiteSpace(col.Alias) ? StringHelpers.NormalizeString(columnName) : col.Alias;
                        var bsonValue = GetFieldName(addFieldsDoc, groupDoc, collectionName, columnName, alias, isAfterGroup: true);
                        proj[alias] = bsonValue;
                    }
                    // Eğer fetch içinde aggregate fonksiyonu varsa yani groupDoc boş değilse _id = null olarak tek group oluştur
                    if (groupDoc != null && groupDoc.ElementCount > 0)
                    {
                        var singleGroupDoc = new BsonDocument { { "_id", BsonNull.Value } };
                        // Mevcut groupDoc içeriğini singleGroupDoc'a kopyala
                        foreach (var elem in groupDoc.Elements)
                        {
                            singleGroupDoc.Add(elem);
                        }
                        groupDoc.Clear();
                        foreach (var elem in singleGroupDoc.Elements)
                        {
                            groupDoc.Add(elem);
                        }
                        // stages.Insert(stages.FindIndex(s => s.Contains("$group")), new BsonDocument("$group", groupDoc));
                    }

                    stages.Add(new BsonDocument("$project", proj));
                }
            }

            // 6) ORDER BY İşlemi
            if (model.OrderBy != null && model.OrderBy.Any())
            {
                var sort = new BsonDocument();
                foreach (var ob in model.OrderBy)
                {
                    var columnName = ob.Column;
                    var alias = StringHelpers.NormalizeString(columnName);
                    var bsonValue = GetFieldName(addFieldsDoc, groupDoc, collectionName, columnName, alias, isAfterGroup: true);
                    string fieldName = bsonValue?.ToString() ?? alias;
                    sort[fieldName] = ob.Desc ? -1 : 1;
                }
                stages.Add(new BsonDocument("$sort", sort));
            }

            // 7) OFFSET / LIMIT İşlemleri
            if (model.Offset.HasValue && model.Offset.Value > 0) stages.Add(new BsonDocument("$skip", model.Offset.Value));
            if (model.Limit.HasValue && model.Limit.Value > 0) stages.Add(new BsonDocument("$limit", model.Limit.Value));

            // Eğer addFields boş ise kaldır
            if (_addFieldsDoc != null && _addFieldsDoc.ElementCount == 0)
            {
                var idx = stages.FindIndex(s => s.Contains("$addFields"));
                if (idx >= 0) stages.RemoveAt(idx);
            }

            // Eğer groupDoc boş ise kaldır
            if (groupDoc != null && groupDoc.ElementCount == 0)
            {
                var idx = stages.FindIndex(s => s.Contains("$group"));
                if (idx >= 0) stages.RemoveAt(idx);
            }

            // Referans için _addFieldsDoc'u null yap
            _addFieldsDoc = null;

            return stages;
        }

        private BsonValue GetFieldName(BsonDocument addFieldsDoc, BsonDocument groupDoc, string table, string column, string alias, bool isAfterGroup = false)
        {
            // Eğer 'null' ise null döndür
            if (string.IsNullOrWhiteSpace(column))
            {
                return BsonNull.Value;
            }
            // Eğer 'bool' ise direk döndür
            if (bool.TryParse(column, out bool boolVal))
            {
                return new BsonBoolean(boolVal);
            }
            // Eğer 'string' ise direk döndür
            if (column.StartsWith("'") && column.EndsWith("'"))
            {
                return new BsonString(column.Trim('\''));
            }

            // Eğer int ise direk döndür
            if (int.TryParse(column, out int intVal))
            {
                return new BsonInt32(intVal);
            }

            // Eğer long ise direk döndür
            if (long.TryParse(column, out long longVal))
            {
                return new BsonInt64(longVal);
            }

            // Eğer 'number' ise direk döndür
            if (double.TryParse(column, out double number))
            {
                return new BsonDouble(number);
            }

            // Eğer 'date' ise direk döndür
            if (DateTime.TryParse(column, out DateTime dateVal))
            {
                return new BsonDateTime(dateVal);
            }

            // var m = Regex.Match(column, "^\"(.*)\"$|^'(.*)'$");
            // if (m.Success)
            // {
            //     var val = m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value;
            //     return new BsonString(val);
            // }

            // Eğer tek tırnaklı string ise tırnak kaldır
            if ((column.StartsWith("\"") && column.EndsWith("\"")) || (column.StartsWith("'") && column.EndsWith("'")))
            {
                return new BsonString(column.Substring(1, column.Length - 2));
            }

            List<string> aggregateFuncs = new List<string> { "COUNT", "SUM", "AVG", "MIN", "MAX" };

            // Eğer fonksiyon ise fonksiyonu işle
            var funcInfo = GetFunction(column);
            if (funcInfo != null)
            {
                var funcName = funcInfo.Value.funcName;
                var inner = funcInfo.Value.inner;
                var args = StringHelpers.SplitByCommas(inner);
                var bsonValue = BuildFunction(groupDoc, table, funcName, args, isAfterGroup);
                // Eğer alias verilmişse addFields dokumanına ekle
                if (!string.IsNullOrWhiteSpace(alias) && addFieldsDoc != null)
                {
                    addFieldsDoc[alias] = bsonValue;
                    return new BsonString($"${alias}");
                }
                else
                {
                    // Alias yoksa benzersiz bir alias oluşturup addFields dokumanına ekle
                    var i = 1;
                    if (addFieldsDoc != null && !aggregateFuncs.Contains(funcName))
                    {
                        while (addFieldsDoc.Contains($"{funcName}_{i}"))
                        {
                            i++;
                        }
                        var uniqueAlias = $"{funcName}_{i}";
                        addFieldsDoc[uniqueAlias] = bsonValue;
                        return new BsonString($"${uniqueAlias}");
                    }
                    else
                    {
                        // addFieldsDoc null ise direkt fonksiyon döndür
                        return bsonValue;
                    }
                }
            }

            // Eğer column 'table.column' formatında ise table ile birlikte kullan
            var parts = column.Split('.');
            if (parts.Length == 2)
            {
                var colTable = parts[0].Trim();
                var colName = parts[1].Trim();

                // Eğer table ile col table aynı ise direk colName kullan
                if (string.Equals(colTable, table, StringComparison.OrdinalIgnoreCase))
                {
                    return new BsonString($"${colName}");
                }
                else
                {
                    // Farklı table ise 'table.column' formatında kullan
                    return new BsonString($"${colTable}.{colName}");
                }
            }
            else if (parts.Length == 1)
            {
                var colName = parts[0].Trim();
                return new BsonString($"${colName}");
            }
            else
            {
                throw new Exception("Invalid column format.");
            }

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

        // Fonksiyon isimlerine göre MongoDB karşılığı oluşturma
        private BsonValue BuildFunction(BsonDocument groupDoc, string table, string functionName, List<string> args, bool isAfterGroup = false)
        {
            List<BsonValue> bsonArgs = new List<BsonValue>();
            // Eğer argumanlarda fonksiyonlar varsa onları da işle
            for (int i = 0; i < args.Count; i++)
            {
                // !!! fix
                var funcInfo = GetFunction(args[i]);
                if (funcInfo != null)
                {
                    var funcName = funcInfo.Value.funcName;
                    var inner = funcInfo.Value.inner;
                    var innerArgs = StringHelpers.SplitByCommas(inner);
                    var funcField = BuildFunction(groupDoc, table, funcName, innerArgs, isAfterGroup);
                    bsonArgs.Add(funcField);
                }
                else if (args[i].StartsWith("'") && args[i].EndsWith("'"))
                {
                    bsonArgs.Add(new BsonString(args[i].Trim('\'')));
                }
                else if (int.TryParse(args[i], out var intVal))
                {
                    bsonArgs.Add(new BsonInt32(intVal));
                }
                else if (double.TryParse(args[i], out var doubleVal))
                {
                    bsonArgs.Add(new BsonDouble(doubleVal));
                }
                else if (bool.TryParse(args[i], out var boolVal))
                {
                    bsonArgs.Add(new BsonBoolean(boolVal));
                }
                else
                {
                    // Eğer column 'table.column' formatında ise table ile birlikte kullan
                    var parts = args[i].Split('.');
                    if (parts.Length == 2)
                    {
                        var colTable = parts[0].Trim();
                        var colName = parts[1].Trim();

                        // Eğer table ile col table aynı ise direk colName kullan
                        if (string.Equals(colTable, table, StringComparison.OrdinalIgnoreCase))
                        {
                            return new BsonString($"${colName}");
                        }
                        else
                        {
                            // Farklı table ise 'table.column' formatında kullan
                            return new BsonString($"${colTable}.{colName}");
                        }
                    }
                    else if (parts.Length == 1)
                    {
                        var colName = parts[0].Trim();
                        return new BsonString($"${colName}");
                    }
                    else
                    {
                        throw new Exception("Invalid column format.");
                    }
                }
            }

            // Aggregate fonksiyonlarını oluştur (COUNT, SUM, AVG, MIN, MAX)
            var aggregateFuncs = new[] { "COUNT", "SUM", "AVG", "MIN", "MAX" };
            if (aggregateFuncs.Contains(functionName) && isAfterGroup)
            {
                var i = 1;
                string uniqueAlias = functionName;
                if (groupDoc != null)
                {
                    while (groupDoc.Contains($"{functionName}_{i}"))
                    {
                        i++;
                    }
                    uniqueAlias = $"{functionName}_{i}";
                }


                if (functionName.Equals("COUNT", StringComparison.OrdinalIgnoreCase))
                {
                    if (args.Count == 1 && args[0] == "*")
                    {

                        groupDoc.Add(uniqueAlias, new BsonDocument("$sum", 1));
                        return new BsonString($"${uniqueAlias}");
                    }
                    else if (args.Count == 1)
                    {
                        groupDoc.Add(uniqueAlias, new BsonDocument("$sum", new BsonDocument
                        {
                            {
                                "$cond",
                                new BsonArray
                                {
                                    new BsonDocument { { "$ifNull", new BsonArray { bsonArgs[0], false } } },
                                    1,
                                    0
                                }
                            }
                        }));
                        return new BsonString($"${uniqueAlias}");
                    }
                    else
                    {
                        throw new Exception("COUNT function requires exactly one argument or '*'.");
                    }
                }

                if (functionName.Equals("SUM", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    groupDoc.Add(uniqueAlias, new BsonDocument("$sum", bsonArgs[0]));
                    return new BsonString($"${uniqueAlias}");
                }

                if (functionName.Equals("AVG", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    groupDoc.Add(uniqueAlias, new BsonDocument("$avg", bsonArgs[0]));
                    return new BsonString($"${uniqueAlias}");
                }

                if (functionName.Equals("MIN", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    groupDoc.Add(uniqueAlias, new BsonDocument("$min", bsonArgs[0]));
                    return new BsonString($"${uniqueAlias}");
                }

                if (functionName.Equals("MAX", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    groupDoc.Add(uniqueAlias, new BsonDocument("$max", bsonArgs[0]));
                    return new BsonString($"${uniqueAlias}");
                }

                else
                {
                    throw new Exception($"Function {functionName} requires exactly one argument.");
                }
            }


            // Sayısal fonksiyonları oluştur (ABS, CEIL, FLOOR, ROUND, SQRT, POWER, MOD, EXP, LOG, LOG10)
            var numericFuncs = new[] { "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "SQRT", "POWER", "MOD", "EXP", "LOG", "LOG10" };
            if (numericFuncs.Contains(functionName))
            {
                if (functionName.Equals("ABS", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$abs", bsonArgs[0]);
                }

                if ((functionName.Equals("CEIL", StringComparison.OrdinalIgnoreCase) || functionName.Equals("CEILING", StringComparison.OrdinalIgnoreCase)) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$ceil", bsonArgs[0]);
                }

                if (functionName.Equals("FLOOR", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$floor", bsonArgs[0]);
                }

                if (functionName.Equals("ROUND", StringComparison.OrdinalIgnoreCase))
                {
                    if (bsonArgs.Count == 1)
                    {
                        return new BsonDocument("$round", new BsonArray { bsonArgs[0] });
                    }
                    else if (bsonArgs.Count == 2)
                    {
                        return new BsonDocument("$round", new BsonArray { bsonArgs[0], bsonArgs[1] });
                    }
                    else
                    {
                        throw new Exception("ROUND function requires 1 or 2 arguments.");
                    }
                }

                if (functionName.Equals("SQRT", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$sqrt", bsonArgs[0]);
                }

                if (functionName.Equals("POWER", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 2)
                {
                    return new BsonDocument("$pow", new BsonArray { bsonArgs[0], bsonArgs[1] });
                }

                if (functionName.Equals("MOD", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 2)
                {
                    return new BsonDocument("$mod", new BsonArray { bsonArgs[0], bsonArgs[1] });
                }

                if (functionName.Equals("EXP", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$exp", bsonArgs[0]);
                }

                if (functionName.Equals("LOG", StringComparison.OrdinalIgnoreCase) && (bsonArgs.Count == 1 || bsonArgs.Count == 2))
                {
                    if (bsonArgs.Count == 1)
                    {
                        return new BsonDocument("$ln", bsonArgs[0]);
                    }
                    else
                    {
                        // LOG(a,b) = LN(a) / LN(b)
                        return new BsonDocument("$divide", new BsonArray { new BsonDocument("$ln", bsonArgs[0]), new BsonDocument("$ln", bsonArgs[1]) });
                    }
                }

                if (functionName.Equals("LOG10", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$log10", bsonArgs[0]);
                }

                else
                {
                    throw new Exception($"Function {functionName} requires specific number of arguments.");
                }
            }

            // Metin fonksiyonlarını oluştur (LENGTH, LEN, SUBSTRING, SUBSTR, CONCAT, LOWER, UPPER, TRIM, LTRIM, RTRIM, INDEXOF, REPLACE, REVERSE)
            var stringFuncs = new[] { "LENGTH", "LEN", "SUBSTRING", "SUBSTR", "CONCAT", "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM", "INDEXOF", "REPLACE", "REVERSE" };
            if (stringFuncs.Contains(functionName))
            {
                if ((functionName.Equals("LENGTH", StringComparison.OrdinalIgnoreCase) || functionName.Equals("LEN", StringComparison.OrdinalIgnoreCase)) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$strLenCP", bsonArgs[0]);
                }

                if ((functionName.Equals("SUBSTRING", StringComparison.OrdinalIgnoreCase) || functionName.Equals("SUBSTR", StringComparison.OrdinalIgnoreCase)) && (bsonArgs.Count == 2 || bsonArgs.Count == 3))
                {
                    if (bsonArgs.Count == 2)
                    {
                        return new BsonDocument("$substrCP", new BsonArray { bsonArgs[0], bsonArgs[1], new BsonInt32(1000000) });
                    }
                    else
                    {
                        return new BsonDocument("$substrCP", new BsonArray { bsonArgs[0], bsonArgs[1], bsonArgs[2] });
                    }
                }

                if (functionName.Equals("CONCAT", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count >= 1)
                {
                    return new BsonDocument("$concat", new BsonArray(bsonArgs));
                }

                if (functionName.Equals("LOWER", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$toLower", bsonArgs[0]);
                }

                if (functionName.Equals("UPPER", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$toUpper", bsonArgs[0]);
                }

                if (functionName.Equals("TRIM", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$trim", new BsonDocument { { "input", bsonArgs[0] } });
                }

                if (functionName.Equals("LTRIM", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$ltrim", new BsonDocument { { "input", bsonArgs[0] } });
                }

                if (functionName.Equals("RTRIM", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$rtrim", new BsonDocument { { "input", bsonArgs[0] } });
                }

                if (functionName.Equals("INDEXOF", StringComparison.OrdinalIgnoreCase) && (bsonArgs.Count == 2 || bsonArgs.Count == 3))
                {
                    if (bsonArgs.Count == 2)
                    {
                        return new BsonDocument("$indexOfCP", new BsonArray { bsonArgs[0], bsonArgs[1] });
                    }
                    else
                    {
                        return new BsonDocument("$indexOfCP", new BsonArray { bsonArgs[0], bsonArgs[1], bsonArgs[2] });
                    }
                }

                if (functionName.Equals("REPLACE", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 3)
                {
                    return new BsonDocument("$replaceAll", new BsonDocument { { "input", bsonArgs[0] }, { "find", bsonArgs[1] }, { "replacement", bsonArgs[2] } });
                }

                if (functionName.Equals("REVERSE", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$reverseArray", new BsonArray { new BsonDocument("$split", new BsonArray { bsonArgs[0], "" }) });
                }

                else
                {
                    throw new Exception($"Function {functionName} requires specific number of arguments.");
                }
            }

            // Null fonksiyonlarını oluştur (COALESCE, IFNULL, ISNULL, NVL)
            var nullFuncs = new[] { "COALESCE", "IFNULL", "ISNULL", "NVL" };
            if (nullFuncs.Contains(functionName) && bsonArgs.Count >= 2)
            {
                BsonValue result = bsonArgs.Last();
                for (int i = bsonArgs.Count - 2; i >= 0; i--)
                {
                    result = new BsonDocument("$ifNull", new BsonArray { bsonArgs[i], result });
                }
                return result;
            }

            // Tarih fonksiyonlarını oluştur (NOW, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, DATEADD, DATEDIFF, DATENAME, DATEPART, DAY, MONTH, YEAR)
            var dateFuncs = new[] { "NOW", "GETDATE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "TODAY", "CURRENT_TIME", "TIME", "DATEADD", "DATEDIFF", "DATENAME", "DAY", "MONTH", "YEAR" };
            if (dateFuncs.Contains(functionName))
            {
                if ((functionName.Equals("NOW", StringComparison.OrdinalIgnoreCase) || functionName.Equals("GETDATE", StringComparison.OrdinalIgnoreCase) || functionName.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)) && args.Count == 0)
                {
                    return new BsonDocument("$toDate", "$$NOW");
                }

                // NOW but with timezone argument (e.g. NOW('Europe/Istanbul'))
                if ((functionName.Equals("NOW", StringComparison.OrdinalIgnoreCase) || functionName.Equals("GETDATE", StringComparison.OrdinalIgnoreCase) || functionName.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)) && args.Count == 1)
                {
                    var tz = args[0].Trim('\'', '"');
                    return new BsonDocument("$toDate", new BsonDocument("$dateToString", new BsonDocument
                    {
                        { "date", "$$NOW" },
                        { "timezone", tz }
                    }));
                }

                if ((functionName.Equals("CURRENT_DATE", StringComparison.OrdinalIgnoreCase) || functionName.Equals("TODAY", StringComparison.OrdinalIgnoreCase)) && args.Count == 0)
                {
                    return new BsonDocument("$dateTrunc", new BsonDocument
                    {
                        { "date", "$$NOW" },
                        { "unit", "day" }
                    });
                }

                if ((functionName.Equals("CURRENT_DATE", StringComparison.OrdinalIgnoreCase) || functionName.Equals("TODAY", StringComparison.OrdinalIgnoreCase)) && args.Count == 1)
                {
                    var tz = args[0].Trim('\'', '"');
                    return new BsonDocument("$dateTrunc", new BsonDocument
                    {
                        { "date", "$$NOW" },
                        { "unit", "day" },
                        { "timezone", tz }
                    });
                }

                if ((functionName.Equals("CURRENT_TIME", StringComparison.OrdinalIgnoreCase) || functionName.Equals("TIME", StringComparison.OrdinalIgnoreCase)) && args.Count == 0)
                {
                    return new BsonDocument("$dateToString", new BsonDocument
                    {
                        { "date", "$$NOW" },
                        { "format", "%H:%M:%S" }
                    });
                }

                if ((functionName.Equals("CURRENT_TIME", StringComparison.OrdinalIgnoreCase) || functionName.Equals("TIME", StringComparison.OrdinalIgnoreCase)) && args.Count == 1)
                {
                    var tz = args[0].Trim('\'', '"');
                    return new BsonDocument("$dateToString", new BsonDocument
                    {
                        { "date", "$$NOW" },
                        { "format", "%H:%M:%S" },
                        { "timezone", tz }
                    });
                }

                if (functionName.Equals("DATEADD", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 3)
                {
                    var interval = args[0].Trim('\'', '"').ToLowerInvariant();
                    var unit = interval switch
                    {
                        "year" => "year",
                        "quarter" => "quarter",
                        "month" => "month",
                        "week" => "week",
                        "day" => "day",
                        "hour" => "hour",
                        "minute" => "minute",
                        "second" => "second",
                        _ => throw new Exception("Invalid interval for DATEADD.")
                    };
                    return new BsonDocument("$dateAdd", new BsonDocument
                    {
                        { "startDate", bsonArgs[1] },
                        { "unit", unit },
                        { "amount", bsonArgs[2] }
                    });
                }

                if (functionName.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 3)
                {
                    var interval = args[0].Trim('\'', '"').ToLowerInvariant();
                    var unit = interval switch
                    {
                        "year" => "year",
                        "quarter" => "quarter",
                        "month" => "month",
                        "week" => "week",
                        "day" => "day",
                        "hour" => "hour",
                        "minute" => "minute",
                        "second" => "second",
                        _ => throw new Exception("Invalid interval for DATEDIFF.")
                    };
                    return new BsonDocument("$dateDiff", new BsonDocument
                    {
                        { "startDate", bsonArgs[1] },
                        { "endDate", bsonArgs[2] },
                        { "unit", unit }
                    });
                }

                if (functionName.Equals("DATENAME", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 2)
                {
                    var part = args[0].Trim('\'', '"').ToLowerInvariant();
                    var format = part switch
                    {
                        "year" => "%Y",
                        "month" => "%m",
                        "day" => "%d",
                        "hour" => "%H",
                        "minute" => "%M",
                        "second" => "%S",
                        "dayofweek" => "%A",
                        "dayofyear" => "%j",
                        _ => throw new Exception("Invalid part for DATENAME.")
                    };
                    return new BsonDocument("$dateToString", new BsonDocument
                    {
                        { "date", bsonArgs[1] },
                        { "format", format }
                    });
                }

                if (functionName.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 2)
                {
                    var part = args[0].Trim('\'', '"').ToLowerInvariant();
                    var datePart = part switch
                    {
                        "year" => new BsonDocument("$year", bsonArgs[1]),
                        "month" => new BsonDocument("$month", bsonArgs[1]),
                        "day" => new BsonDocument("$dayOfMonth", bsonArgs[1]),
                        "hour" => new BsonDocument("$hour", bsonArgs[1]),
                        "minute" => new BsonDocument("$minute", bsonArgs[1]),
                        "second" => new BsonDocument("$second", bsonArgs[1]),
                        "dayofweek" => new BsonDocument("$dayOfWeek", bsonArgs[1]),
                        "dayofyear" => new BsonDocument("$dayOfYear", bsonArgs[1]),
                        _ => throw new Exception("Invalid part for DATEPART.")
                    };
                    return datePart;
                }

                if (functionName.Equals("DAY", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$dayOfMonth", bsonArgs[0]);
                }

                if (functionName.Equals("MONTH", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$month", bsonArgs[0]);
                }

                if (functionName.Equals("YEAR", StringComparison.OrdinalIgnoreCase) && bsonArgs.Count == 1)
                {
                    return new BsonDocument("$year", bsonArgs[0]);
                }

                else
                {
                    throw new Exception($"Function {functionName} requires specific number of arguments.");
                }
            }

            throw new Exception($"Unsupported function: {functionName}");
        }

        // IncludeModel için MongoDB $lookup ve $unwind dökümanları oluşturma
        private (BsonDocument lookup, BsonDocument? unwind) BuildLookupStage(IncludeModel inc)
        {
            // Table ve IncludeTable için columnları belirle
            var localField = string.IsNullOrWhiteSpace(inc.TableKey) ? "_id" : inc.TableKey;
            var foreignField = string.IsNullOrWhiteSpace(inc.IncludeKey) ? $"{inc.Table}_id" : inc.IncludeKey;
            var leftCollection = inc.Table;
            var rightCollection = inc.IncludeTable;
            var joinType = inc.JoinType ?? "LEFT";

            // Include Table için LOOKUP aşaması
            var asName = inc.IncludeTable;
            var lookup = new BsonDocument("$lookup", new BsonDocument
                {
                    { "from", rightCollection },
                    { "localField", localField },
                    { "foreignField", foreignField },
                    { "as", asName }
                });

            var unwind = null as BsonDocument;

            if (!string.IsNullOrWhiteSpace(joinType) && joinType.Trim().ToUpperInvariant() == "INNER")
            {
                // INNER JOIN için unwind yap ve null olanları filtrele
                unwind = new BsonDocument("$unwind", new BsonDocument
                    {
                        { "path", $"${asName}" },
                        { "preserveNullAndEmptyArrays", false }
                    });

            }
            else if (!string.IsNullOrWhiteSpace(joinType) && joinType.Trim().ToUpperInvariant() == "LEFT")
            {
                // LEFT JOIN için unwind yap ve null olanları koru
                unwind = new BsonDocument("$unwind", new BsonDocument
                    {
                        { "path", $"${asName}" },
                        { "preserveNullAndEmptyArrays", true }
                    });
            }
            else if (!string.IsNullOrWhiteSpace(joinType) &&
                     (joinType.Trim().ToUpperInvariant() == "RIGHT" ||
                      joinType.Trim().ToUpperInvariant() == "FULL"))
            {
                //!!! tbc
                unwind = null;
            }
            else
            {
                // Default olarak LEFT JOIN yap
                unwind = new BsonDocument("$unwind", new BsonDocument
                    {
                        { "path", $"${asName}" },
                        { "preserveNullAndEmptyArrays", true }
                    });
            }

            return (lookup, unwind);
        }

        // FilterModel için MongoDB $match dökümanı oluşturma
        private BsonDocument BuildMatchFromFilter(BsonDocument addFieldsDoc, BsonDocument groupDoc, string collectionName, FilterModel filter, bool isAfterGroup = false)
        {
            if (filter is ConditionFilterModel c)
            {
                // If after group, condition may reference aggregated aliases (no $ prefix)

                return BuildCondition(addFieldsDoc, groupDoc, collectionName, c.Column, c.Operator, c.Value, isAfterGroup);
            }
            else if (filter is LogicalFilterModel l)
            {
                var left = BuildMatchFromFilter(addFieldsDoc, groupDoc, collectionName, l.Left, isAfterGroup);
                var right = BuildMatchFromFilter(addFieldsDoc, groupDoc, collectionName, l.Right, isAfterGroup);
                if (l.Operator == LogicalOperator.And)
                    return new BsonDocument("$and", new BsonArray { left, right });
                else
                    return new BsonDocument("$or", new BsonArray { left, right });
            }

            return new BsonDocument();
        }

        // ConditionFilterModel'i MongoDB karşılığına dönüştürme
        private BsonDocument BuildCondition(BsonDocument addFieldsDoc, BsonDocument groupDoc, string collectionName, string field, ComparisonOperator op, string? value, bool isAfterGroup = false)
        {
            var bsonField = GetFieldName(addFieldsDoc, groupDoc, collectionName, field, "", isAfterGroup);
            string fieldKey;
            if (bsonField.IsString)
            {
                fieldKey = bsonField.AsString;
            }
            else
            {
                fieldKey = field;
            }

            if (value == null && op != ComparisonOperator.IsNull && op != ComparisonOperator.IsNotNull)
            {
                throw new Exception("Value cannot be null for the specified operator.");
            }

            var bsonValue = value == null ? BsonNull.Value : GetFieldName(addFieldsDoc, groupDoc, collectionName, value, "", isAfterGroup: true) ?? BsonNull.Value;

            bool isValueString = bsonValue.IsString && !bsonValue.AsString.StartsWith("$");

            BsonArray operands = new BsonArray { bsonField, bsonValue };
            Console.WriteLine($"Text: {fieldKey}, {op}, {value}");



            switch (op)
            {
                case ComparisonOperator.Eq:
                    return new BsonDocument("$expr", new BsonDocument("$eq", operands));
                case ComparisonOperator.Neq:
                    return new BsonDocument("$expr", new BsonDocument("$ne", operands));
                case ComparisonOperator.Gt:
                    return new BsonDocument("$expr", new BsonDocument("$gt", operands));
                case ComparisonOperator.Gte:
                    return new BsonDocument("$expr", new BsonDocument("$gte", operands));
                case ComparisonOperator.Lt:
                    return new BsonDocument("$expr", new BsonDocument("$lt", operands));
                case ComparisonOperator.Lte:
                    return new BsonDocument("$expr", new BsonDocument("$lte", operands));
                case ComparisonOperator.Like:
                case ComparisonOperator.ILike:
                    {
                        var options = op == ComparisonOperator.ILike ? "i" : "";
                        if (isValueString)
                        {
                            var pattern = bsonValue.AsString.Replace("%", ".*").Replace("_", ".") ?? "";
                            return new BsonDocument("$expr", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", pattern },
                                { "options", options }
                            }));
                        }
                        else
                        {
                            return new BsonDocument("$expr", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", bsonValue },
                                { "options", options }
                            }));
                        }
                    }
                case ComparisonOperator.NotLike:
                case ComparisonOperator.NotILike:
                    {
                        var options = op == ComparisonOperator.NotILike ? "i" : "";
                        if (isValueString)
                        {
                            var pattern = bsonValue.AsString.Replace("%", ".*").Replace("_", ".") ?? "";
                            return new BsonDocument("$expr", new BsonDocument("$not", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", pattern },
                                { "options", options }
                            })));
                        }
                        else
                        {
                            return new BsonDocument("$expr", new BsonDocument("$not", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", bsonValue },
                                { "options", options }
                            })));
                        }
                    }
                case ComparisonOperator.Contains:
                case ComparisonOperator.IContains:
                    {
                        var options = op == ComparisonOperator.IContains ? "i" : "";
                        if (isValueString)
                        {
                            var pattern = bsonValue.AsString ?? "";
                            return new BsonDocument("$expr", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", Regex.Escape(pattern) },
                                { "options", options }
                            }));
                        }
                        else
                        {
                            // return new BsonDocument("$expr", new BsonDocument("$regexMatch", new BsonDocument
                            // {
                            //     { "input", bsonField },
                            //     { "regex", bsonValue },
                            //     { "options", options }
                            // }));
                            if (options == "i")
                            {
                                return new BsonDocument("$expr", new BsonDocument("$gte", new BsonArray
                                {
                                    new BsonDocument("$indexOfBytes", new BsonArray
                                    {
                                        new BsonDocument("$toLower", bsonField),
                                        new BsonDocument("$toLower", bsonValue)
                                    }),
                                    0
                                }));
                            }
                            else
                            {
                                return new BsonDocument("$expr", new BsonDocument("$gte", new BsonArray
                                {
                                    new BsonDocument("$indexOfBytes", new BsonArray { bsonField, bsonValue }),
                                    0
                                }));
                            }
                        }
                    }
                case ComparisonOperator.NotContains:
                case ComparisonOperator.NotIContains:
                    {
                        var options = op == ComparisonOperator.NotIContains ? "i" : "";
                        var pattern = bsonValue.AsString ?? "";
                        if (isValueString)
                        {
                            return new BsonDocument("$expr", new BsonDocument("$not", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", Regex.Escape(pattern) },
                                { "options", options }
                            })));
                        }
                        else
                        {
                            // return new BsonDocument("$expr", new BsonDocument("$not", new BsonDocument("$regexMatch", new BsonDocument
                            // {
                            //     { "input", bsonField },
                            //     { "regex", bsonValue },
                            //     { "options", options }
                            // })));
                            if (options == "i")
                            {
                                return new BsonDocument("$expr", new BsonDocument("$lt", new BsonArray
                                {
                                    new BsonDocument("$indexOfBytes", new BsonArray
                                    {
                                        new BsonDocument("$toLower", bsonField),
                                        new BsonDocument("$toLower", bsonValue)
                                    }),
                                    0
                                }));
                            }
                            else
                            {
                                return new BsonDocument("$expr", new BsonDocument("$lt", new BsonArray
                                {
                                    new BsonDocument("$indexOfBytes", new BsonArray { bsonField, bsonValue }),
                                    0
                                }));
                            }
                        }
                    }
                case ComparisonOperator.BeginsWith:
                case ComparisonOperator.IBeginsWith:
                    {
                        var options = op == ComparisonOperator.IBeginsWith ? "i" : "";
                        var pattern = "^" + (bsonValue.AsString != null ? Regex.Escape(bsonValue.AsString) : "");
                        if (isValueString)
                        {
                            return new BsonDocument("$expr", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", pattern },
                                { "options", options }
                            }));
                        }
                        else
                        {
                            return new BsonDocument("$expr", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", bsonValue },
                                { "options", options }
                            }));
                        }
                    }
                case ComparisonOperator.NotBeginsWith:
                case ComparisonOperator.NotIBeginsWith:
                    {
                        var options = op == ComparisonOperator.NotIBeginsWith ? "i" : "";
                        var pattern = "^" + (bsonValue.AsString != null ? Regex.Escape(bsonValue.AsString) : "");
                        if (isValueString)
                        {
                            return new BsonDocument("$expr", new BsonDocument("$not", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", pattern },
                                { "options", options }
                            })));
                        }
                        else
                        {
                            return new BsonDocument("$expr", new BsonDocument("$not", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", bsonValue },
                                { "options", options }
                            })));
                        }

                    }
                case ComparisonOperator.EndsWith:
                case ComparisonOperator.IEndsWith:
                    {
                        var options = op == ComparisonOperator.IEndsWith ? "i" : "";
                        var pattern = (bsonValue.AsString != null ? Regex.Escape(bsonValue.AsString) : "") + "$";
                        if (isValueString)
                        {
                            return new BsonDocument("$expr", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", pattern },
                                { "options", options }
                            }));
                        }
                        else
                        {
                            return new BsonDocument("$expr", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", bsonValue },
                                { "options", options }
                            }));
                        }

                    }
                case ComparisonOperator.NotEndsWith:
                case ComparisonOperator.NotIEndsWith:
                    {
                        var options = op == ComparisonOperator.NotIEndsWith ? "i" : "";
                        var pattern = (bsonValue != null ? Regex.Escape(bsonValue.AsString) : "") + "$";
                        if (isValueString)
                        {
                            return new BsonDocument("$expr", new BsonDocument("$not", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", pattern },
                                { "options", options }
                            })));
                        }
                        else
                        {
                            return new BsonDocument("$expr", new BsonDocument("$not", new BsonDocument("$regexMatch", new BsonDocument
                            {
                                { "input", bsonField },
                                { "regex", bsonValue },
                                { "options", options }
                            })));
                        }

                    }
                case ComparisonOperator.IsNull:
                    return new BsonDocument("$expr", new BsonDocument("$eq", new BsonArray { bsonField, BsonNull.Value }));
                case ComparisonOperator.IsNotNull:
                    return new BsonDocument("$expr", new BsonDocument("$ne", new BsonArray { bsonField, BsonNull.Value }));
                case ComparisonOperator.In:
                    {
                        var values = ToBsonArray(value);
                        return new BsonDocument("$expr", new BsonDocument("$in", new BsonArray { bsonField, values }));
                    }
                case ComparisonOperator.NotIn:
                    {
                        var values = ToBsonArray(value);
                        return new BsonDocument("$expr", new BsonDocument("$not", new BsonDocument("$in", new BsonArray { bsonField, values })));
                    }
                case ComparisonOperator.Between:
                    {
                        if (string.IsNullOrWhiteSpace(value))
                        {
                            throw new Exception("BETWEEN operator requires two comma-separated values.");
                        }
                        var parts = StringHelpers.SplitByCommas(value);
                        if (parts.Count != 2)
                        {
                            throw new Exception("BETWEEN operator requires two comma-separated values.");
                        }
                        var lower = GetFieldName(addFieldsDoc, groupDoc, collectionName, parts[0].Trim(), "") ?? BsonNull.Value;
                        var upper = GetFieldName(addFieldsDoc, groupDoc, collectionName, parts[1].Trim(), "") ?? BsonNull.Value;
                        return new BsonDocument("$expr", new BsonDocument("$and", new BsonArray
                        {
                            new BsonDocument("$gte", new BsonArray { bsonField, lower }),
                            new BsonDocument("$lte", new BsonArray { bsonField, upper })
                        }));
                    }
                case ComparisonOperator.NotBetween:
                    {
                        if (string.IsNullOrWhiteSpace(value))
                        {
                            throw new Exception("NOT BETWEEN operator requires two comma-separated values.");
                        }
                        var parts = StringHelpers.SplitByCommas(value);
                        if (parts.Count != 2)
                        {
                            throw new Exception("NOT BETWEEN operator requires two comma-separated values.");
                        }
                        var lower = GetFieldName(addFieldsDoc, groupDoc, collectionName, parts[0].Trim(), "") ?? BsonNull.Value;
                        var upper = GetFieldName(addFieldsDoc, groupDoc, collectionName, parts[1].Trim(), "") ?? BsonNull.Value;
                        return new BsonDocument("$expr", new BsonDocument("$or", new BsonArray
                        {
                            new BsonDocument("$lt", new BsonArray { bsonField, lower }),
                            new BsonDocument("$gt", new BsonArray { bsonField, upper })
                        }));
                    }
                default:
                    throw new Exception("Unsupported comparison operator.");
            }
        }

        // GroupBy için MongoDB $group dökümanı oluşturma
        private void BuildGroupByStage(BsonDocument addFieldsDoc, BsonDocument groupDoc, string collectionName, List<string> groupBy)
        {
            var idDoc = new BsonDocument();

            foreach (var g in groupBy)
            {
                var columnName = g;
                var alias = StringHelpers.NormalizeString(columnName);
                var bsonValue = GetFieldName(addFieldsDoc, groupDoc, collectionName, columnName, alias);
                string fieldName = bsonValue?.ToString() ?? alias;
                idDoc[alias] = bsonValue ?? BsonNull.Value;
            }

            groupDoc["_id"] = idDoc;
        }

        private BsonValue ParseBsonValue(string? s)
        {
            if (s == null) return BsonNull.Value;
            if (bool.TryParse(s, out var b)) return new BsonBoolean(b);
            if (int.TryParse(s, out var i)) return new BsonInt32(i);
            if (long.TryParse(s, out var l)) return new BsonInt64(l);
            if (double.TryParse(s, out var d)) return new BsonDouble(d);
            if (DateTime.TryParse(s, out var dateVal)) return new BsonDateTime(dateVal);
            if (DateTimeOffset.TryParse(s, out var dateOffVal)) return new BsonDateTime(dateOffVal.UtcDateTime);
            if (s.Equals("null", StringComparison.OrdinalIgnoreCase)) return BsonNull.Value;
            // If quoted string, strip quotes
            var m = Regex.Match(s, "^\"(.*)\"$|^'(.*)'$");
            if (m.Success)
            {
                var val = m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value;
                return new BsonString(val);
            }
            return new BsonString(s);
        }

        private BsonArray ToBsonArray(string? value)
        {
            if (string.IsNullOrEmpty(value)) return new BsonArray();
            var parts = StringHelpers.SplitByCommas(value)
                        .Select(p => ParseBsonValue(p.Trim()))
                        .ToArray();
            return new BsonArray(parts);
        }
    }
}