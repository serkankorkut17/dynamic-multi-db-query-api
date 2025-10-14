using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

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
            if (value == null)
                return BsonNull.Value;

            // Primitive tipler
            if (value is string s)
                return new BsonString(s);

            if (value is int i)
                return new BsonInt32(i);

            if (value is long l)
                return new BsonInt64(l);

            if (value is double d)
                return new BsonDouble(d);

            if (value is decimal dec)
                return new BsonDecimal128(dec);

            if (value is bool b)
                return new BsonBoolean(b);

            if (value is DateTime dt)
                return new BsonDateTime(dt);

            if (value is DateTimeOffset dto)
                return new BsonDateTime(dto.UtcDateTime);

            // JsonElement'ler
            if (value is JsonElement je)
            {
                return je.ValueKind switch
                {
                    JsonValueKind.String => new BsonString(je.GetString() ?? ""),
                    JsonValueKind.Number => je.TryGetInt64(out long longVal)
                        ? new BsonInt64(longVal)
                        : new BsonDouble(je.GetDouble()),
                    JsonValueKind.True => BsonBoolean.True,
                    JsonValueKind.False => BsonBoolean.False,
                    JsonValueKind.Null => BsonNull.Value,
                    JsonValueKind.Array => BsonArray.Create(
                        je.EnumerateArray().Select(e => ConvertToBsonValue(e))
                    ),
                    JsonValueKind.Object => BsonDocument.Parse(je.GetRawText()),
                    _ => BsonNull.Value
                };
            }

            // Array/List
            if (value is IEnumerable<object> enumerable && !(value is string))
            {
                var array = new BsonArray();
                foreach (var item in enumerable)
                {
                    array.Add(ConvertToBsonValue(item));
                }
                return array;
            }

            // Dictionary
            if (value is IDictionary<string, object> dict)
            {
                var doc = new BsonDocument();
                foreach (var kvp in dict)
                {
                    doc[kvp.Key] = ConvertToBsonValue(kvp.Value);
                }
                return doc;
            }

            // JSON string kontrolü (string ama JSON formatında)
            if (value is string jsonStr &&
                (jsonStr.TrimStart().StartsWith("{") || jsonStr.TrimStart().StartsWith("[")))
            {
                try
                {
                    // JSON array
                    if (jsonStr.TrimStart().StartsWith("["))
                    {
                        return BsonSerializer.Deserialize<BsonArray>(jsonStr);
                    }
                    // JSON object
                    else
                    {
                        return BsonDocument.Parse(jsonStr);
                    }
                }
                catch
                {
                    // JSON parse başarısız, normal string olarak kaydet
                    return new BsonString(jsonStr);
                }
            }

            // Son çare: ToString() ile string'e çevir
            return new BsonString(value.ToString() ?? "");
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