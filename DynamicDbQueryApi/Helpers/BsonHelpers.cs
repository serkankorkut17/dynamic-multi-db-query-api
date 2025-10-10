using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace DynamicDbQueryApi.Helpers
{
    public class BsonHelpers
    {
        // String'i uygun BsonValue'ye dönüştürür
        public static BsonValue ParseBsonValue(string? s)
        {
            if (s == null) return BsonNull.Value;
            if (bool.TryParse(s, out var b)) return new BsonBoolean(b);
            if (int.TryParse(s, out var i)) return new BsonInt32(i);
            if (long.TryParse(s, out var l)) return new BsonInt64(l);
            if (double.TryParse(s, out var d)) return new BsonDouble(d);
            if (DateTime.TryParse(s, out var dateVal)) return new BsonDateTime(dateVal);
            if (DateTimeOffset.TryParse(s, out var dateOffVal)) return new BsonDateTime(dateOffVal.UtcDateTime);
            if (s.Equals("null", StringComparison.OrdinalIgnoreCase)) return BsonNull.Value;
            // Eğer string çift tırnak veya tek tırnak içindeyse, tırnakları kaldır
            var m = Regex.Match(s, "^\"(.*)\"$|^'(.*)'$");
            if (m.Success)
            {
                var val = m.Groups[1].Success ? m.Groups[1].Value : m.Groups[2].Value;
                return new BsonString(val);
            }
            return new BsonString(s);
        }

        // Virgülle ayrılmış string'i BsonArray'e dönüştürür
        public static BsonArray ToBsonArray(string? value)
        {
            if (string.IsNullOrEmpty(value)) return new BsonArray();
            var parts = StringHelpers.SplitByCommas(value)
                        .Select(p => ParseBsonValue(p.Trim()))
                        .ToArray();
            return new BsonArray(parts);
        }

        // BsonDocument'i Dictionary'ye dönüştürür
        public static IDictionary<string, object?> BsonDocumentToDictionary(BsonDocument doc)
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var el in doc.Elements)
            {
                dict[el.Name] = ConvertBsonValue(el.Value);
            }
            return dict;
        }

        // BsonValue'yi uygun .NET tipine dönüştürür
        public static object? ConvertBsonValue(BsonValue v)
        {
            if (v == null || v.IsBsonNull) return null;
            if (v.BsonType == BsonType.DateTime) return v.ToUniversalTime();
            if (v.BsonType == BsonType.Timestamp)
            {
                var ts = v.AsBsonTimestamp;
                return DateTimeOffset.FromUnixTimeSeconds(ts.Timestamp).UtcDateTime;
            }
            if (v.IsBoolean) return v.AsBoolean;
            if (v.IsString) return v.AsString;
            if (v.IsInt32) return v.AsInt32;
            if (v.IsInt64) return v.AsInt64;
            if (v.IsDouble) return v.AsDouble;
            if (v.IsDecimal128) return Decimal128.ToDecimal(v.AsDecimal128);
            if (v.IsObjectId) return v.AsObjectId.ToString();
            if (v.IsGuid) return v.AsGuid;
            if (v.IsBsonArray) return v.AsBsonArray.Select(ConvertBsonValue).ToList();
            if (v.IsBsonDocument) return BsonDocumentToDictionary(v.AsBsonDocument);
            return v.ToString();
        }

        // dynamic objeyi BsonDocument'e dönüştürür
        public static BsonDocument? ToBsonDocumentSafe(dynamic row)
        {
            if (row is BsonDocument bd) return bd;

            if (row is IDictionary<string, object?> dict)
            {
                var bson = new BsonDocument();
                foreach (var kv in dict)
                {
                    bson[kv.Key] = ConvertToBsonValue(kv.Value);
                }
                return bson;
            }

            return BsonDocument.Parse(JsonSerializer.Serialize(row));
        }

        // object'i uygun BsonValue'ye dönüştürür
        public static BsonValue ConvertToBsonValue(object? value)
        {
            if (value == null) return BsonNull.Value;

            // Zaten BsonValue ise direkt döner
            if (value is BsonValue bv) return bv;

            switch (value)
            {
                case bool b: return BsonBoolean.Create(b);
                case int i: return BsonInt32.Create(i);
                case long l: return BsonInt64.Create(l);
                case short s: return BsonInt32.Create(s);
                case byte by: return BsonInt32.Create(by);
                case uint ui: return BsonInt64.Create(ui);
                case ulong ul: return BsonInt64.Create((long)ul);
                case ushort us: return BsonInt32.Create(us);
                case float f: return BsonDouble.Create(f);
                case double d: return BsonDouble.Create(d);
                case decimal dec: return BsonDecimal128.Create(dec);
                case DateTime dt: return new BsonDateTime(dt);
                case DateTimeOffset dto: return new BsonDateTime(dto.UtcDateTime);
                case TimeSpan ts: return BsonInt64.Create(ts.Ticks);
                case Guid g: return new BsonBinaryData(g, GuidRepresentation.Standard);
                case IEnumerable<object?> list:
                    return new BsonArray(list.Select(ConvertToBsonValue));
                case JsonElement jsonEl:
                    return BsonDocument.Parse(jsonEl.GetRawText());
                case IDictionary<string, object?> nestedDict:
                    return new BsonDocument(nestedDict.ToDictionary(k => k.Key, k => ConvertToBsonValue(k.Value)));
                case string str:
                    {
                        var trimmed = str.Trim();
                        if (StringHelpers.IsDateOrTimestamp(trimmed) && DateTimeOffset.TryParse(trimmed, out var dto))
                        {
                            return new BsonDateTime(dto.UtcDateTime);
                        }
                        if (int.TryParse(trimmed, out var si)) return BsonInt32.Create(si);
                        if (long.TryParse(trimmed, out var sl)) return BsonInt64.Create(sl);
                        if (decimal.TryParse(trimmed, out var sdec)) return BsonDecimal128.Create(sdec);
                        if (double.TryParse(trimmed, out var sd)) return BsonDouble.Create(sd);
                        if (bool.TryParse(trimmed, out var sb)) return BsonBoolean.Create(sb);

                        // JSON text?
                        if ((trimmed.StartsWith("{") && trimmed.EndsWith("}")) ||
                            (trimmed.StartsWith("[") && trimmed.EndsWith("]")))
                        {
                            try
                            {
                                return BsonDocument.Parse(trimmed);
                            }
                            catch { /* ignore */ }
                        }
                        return BsonString.Create(str);
                    }
                default:
                    return BsonValue.Create(value);
            }
        }

        // BsonType'ın değerini string olarak döner
        public static string MapBsonTypeToSqlType(BsonType bsonType)
        {
            return bsonType switch
            {
                BsonType.ObjectId => "string",
                BsonType.String => "string",
                BsonType.Int32 => "int",
                BsonType.Int64 => "bigint",
                BsonType.Double => "double",
                BsonType.Decimal128 => "decimal",
                BsonType.Boolean => "boolean",
                BsonType.DateTime => "datetime",
                BsonType.Timestamp => "timestamp",
                BsonType.Binary => "binary",
                BsonType.Array => "array",
                BsonType.Document => "json",
                _ => "string"
            };
        }
    }
    
}