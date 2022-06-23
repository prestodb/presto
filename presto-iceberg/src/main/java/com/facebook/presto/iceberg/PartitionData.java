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
package com.facebook.presto.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitionData
        implements StructLike
{
    private static final String PARTITION_VALUES_FIELD = "partitionValues";
    private static final JsonFactory FACTORY = new JsonFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY)
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    private final Object[] partitionValues;

    public PartitionData(Object[] partitionValues)
    {
        this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
    }

    @Override
    public int size()
    {
        return partitionValues.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass)
    {
        Object value = partitionValues[pos];

        if (javaClass == ByteBuffer.class && value instanceof byte[]) {
            value = ByteBuffer.wrap((byte[]) value);
        }

        if (value == null || javaClass.isInstance(value)) {
            return javaClass.cast(value);
        }

        throw new IllegalArgumentException(format("Wrong class [%s] for object class [%s]", javaClass.getName(), value.getClass().getName()));
    }

    @Override
    public <T> void set(int pos, T value)
    {
        partitionValues[pos] = value;
    }

    public String toJson()
    {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator generator = FACTORY.createGenerator(writer);
            generator.writeStartObject();
            generator.writeArrayFieldStart(PARTITION_VALUES_FIELD);
            for (Object value : partitionValues) {
                generator.writeObject(value);
            }
            generator.writeEndArray();
            generator.writeEndObject();
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("JSON conversion failed for PartitionData: " + Arrays.toString(partitionValues), e);
        }
    }

    public static PartitionData fromJson(String partitionDataAsJson, Type[] types)
    {
        if (partitionDataAsJson == null) {
            return null;
        }

        JsonNode jsonNode;
        try {
            jsonNode = MAPPER.readTree(partitionDataAsJson);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Conversion from JSON failed for PartitionData: " + partitionDataAsJson, e);
        }
        if (jsonNode.isNull()) {
            return null;
        }

        JsonNode partitionValues = jsonNode.get(PARTITION_VALUES_FIELD);
        Object[] objects = new Object[types.length];
        int index = 0;
        for (JsonNode partitionValue : partitionValues) {
            objects[index] = getValue(partitionValue, types[index]);
            index++;
        }
        return new PartitionData(objects);
    }

    public static Object getValue(JsonNode partitionValue, Type type)
    {
        if (partitionValue.isNull()) {
            return null;
        }
        switch (type.typeId()) {
            case BOOLEAN:
                return partitionValue.asBoolean();
            case INTEGER:
            case DATE:
                return partitionValue.asInt();
            case LONG:
            case TIMESTAMP:
            case TIME:
                return partitionValue.asLong();
            case FLOAT:
                return partitionValue.floatValue();
            case DOUBLE:
                return partitionValue.doubleValue();
            case STRING:
                return partitionValue.asText();
            case FIXED:
            case BINARY:
                try {
                    return partitionValue.binaryValue();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Failed during JSON conversion of " + partitionValue, e);
                }
            case DECIMAL:
                return partitionValue.decimalValue();
        }
        throw new UnsupportedOperationException("Type not supported as partition column: " + type);
    }
}
