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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

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
            Class<?> type = value.getType();
            if (type == String.class) {
                jsonGenerator.writeString((String) value.getValue());
            }
            else if (type == Slice.class) {
                jsonGenerator.writeString(((Slice) value.getValue()).toStringUtf8());
            }
            else if (type == Boolean.class) {
                jsonGenerator.writeBoolean((Boolean) value.getValue());
            }
            else if (type == Long.class) {
                jsonGenerator.writeNumber((Long) value.getValue());
            }
            else if (type == Double.class) {
                jsonGenerator.writeNumber((Double) value.getValue());
            }
            else {
                throw new AssertionError("Unknown type: " + type);
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

            checkJson(jsonParser.nextFieldName(new SerializedString("value")));

            JsonToken token = jsonParser.nextToken();
            Comparable<?> value = (token == JsonToken.VALUE_NULL) ? null : readValue(type, jsonParser);
            checkJson(jsonParser.nextToken() == JsonToken.END_OBJECT);

            return new SerializableNativeValue(type, value);
        }

        private static Comparable<?> readValue(Class<?> type, JsonParser jsonParser)
                throws IOException
        {
            if (type == String.class) {
                String value = jsonParser.getValueAsString();
                checkJson(value != null);
                return value;
            }
            else if (type == Slice.class) {
                String value = jsonParser.getValueAsString();
                checkJson(value != null);
                return Slices.copiedBuffer(value, UTF_8);
            }
            else if (type == Boolean.class) {
                return jsonParser.getBooleanValue();
            }
            else if (type == Long.class) {
                return jsonParser.getLongValue();
            }
            else if (type.equals(Double.class)) {
                return jsonParser.getDoubleValue();
            }
            else {
                throw new AssertionError("Unknown type: " + type);
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
