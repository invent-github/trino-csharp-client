using System;
using Newtonsoft.Json;

namespace Trino.Client.Utils
{
    public static class DefaultJsonSerializerOptions
    {
        public static readonly JsonSerializerSettings Instance = new JsonSerializerSettings
        {
            Converters = { new LongNaNJsonConverter() }
        };
    }

    public class LongNaNJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
            JsonSerializer serializer)
        {
            var strValue = reader.Value?.ToString();

            if (strValue == "NaN")
            {
                return 0L;
            }

            if (long.TryParse(strValue, out var val))
            {
                return val;
            }

            return 0L;
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(long);
        }
    }
}