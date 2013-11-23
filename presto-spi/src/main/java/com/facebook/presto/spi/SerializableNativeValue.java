/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.util.Objects;

@JsonSerialize(using = SerializableNativeValue.Serializer.class)
@JsonDeserialize(using = SerializableNativeValue.Deserializer.class)
public class SerializableNativeValue
{
    private final Class<?> type;
    private final Comparable<?> value;

    public SerializableNativeValue(Class<?> type, Comparable<?> value)
    {
        this.type = Objects.requireNonNull(type, "type is null");
        this.value = value;
        if (value != null && !type.isInstance(value)) {
            throw new IllegalArgumentException(String.format("type %s does not match value %s", type.getClass(), value));
        }
    }

    public Class<?> getType()
    {
        return type;
    }

    public Comparable<?> getValue()
    {
        return value;
    }

    public static class Serializer
            extends JsonSerializer<SerializableNativeValue>
    {
        @Override
        public void serialize(SerializableNativeValue value, JsonGenerator generator, SerializerProvider provider)
                throws IOException
        {
            generator.writeStartObject();
            generator.writeStringField("type", value.getType().getCanonicalName());
            generator.writeFieldName("value");
            if (value.getValue() == null) {
                generator.writeNull();
            }
            else {
                writeValue(value, generator);
            }
            generator.writeEndObject();
        }

        private static void writeValue(SerializableNativeValue value, JsonGenerator jsonGenerator)
                throws IOException
        {
            ColumnType columnType = ColumnType.fromNativeType(value.getType());
            switch (columnType) {
                case STRING:
                    jsonGenerator.writeString((String) value.getValue());
                    break;
                case BOOLEAN:
                    jsonGenerator.writeBoolean((Boolean) value.getValue());
                    break;
                case LONG:
                    jsonGenerator.writeNumber((Long) value.getValue());
                    break;
                case DOUBLE:
                    jsonGenerator.writeNumber((Double) value.getValue());
                    break;
                default:
                    throw new AssertionError("Unknown type: " + columnType);
            }
        }
    }

    public static class Deserializer
            extends JsonDeserializer<SerializableNativeValue>
    {
        @Override
        public SerializableNativeValue deserialize(JsonParser jsonParser, DeserializationContext context)
                throws IOException
        {
            checkJson(jsonParser.nextFieldName(new SerializedString("type")));

            String typeString = jsonParser.nextTextValue();
            Class<?> type = extractClassType(typeString);
            ColumnType.fromNativeType(type); // Make sure the null is a valid type

            checkJson(jsonParser.nextFieldName(new SerializedString("value")));

            JsonToken token = jsonParser.nextToken();
            Comparable<?> value = (token == JsonToken.VALUE_NULL) ? null : readValue(type, jsonParser);
            checkJson(jsonParser.nextToken() == JsonToken.END_OBJECT);

            return new SerializableNativeValue(type, value);
        }

        private static Comparable<?> readValue(Class<?> type, JsonParser jsonParser)
                throws IOException
        {
            ColumnType columnType = ColumnType.fromNativeType(type);
            switch (columnType) {
                case STRING:
                    String value = jsonParser.getValueAsString();
                    checkJson(value != null);
                    return value;
                case BOOLEAN:
                    return jsonParser.getBooleanValue();
                case LONG:
                    return jsonParser.getLongValue();
                case DOUBLE:
                    return jsonParser.getDoubleValue();
                default:
                    throw new AssertionError("Unknown type: " + columnType);
            }
        }

        private static void checkJson(boolean condition)
        {
            if (!condition) {
                throw new IllegalArgumentException("Malformed SerializableNativeValue JSON object");
            }
        }

        private static Class<?> extractClassType(String typeString)
        {
            try {
                return Class.forName(typeString);
            }
            catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Unknown class type: " + typeString);
            }
        }
    }
}
