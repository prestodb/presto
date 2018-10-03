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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.util.JsonUtil;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.netflix.iceberg.util.JsonUtil.factory;

// TODO this needs to part of Iceberg otherwise we could miss metrics that are newly added on iceberg side.
public class MetricsParser
{
    private static final String COLUMN_SIZES = "columnSizes";
    private static final String NULL_VALUE_COUNTS = "nullValueCounts";
    private static final String VALUE_COUNTS = "valueCounts";
    private static final String RECORD_COUNT = "recordCount";
    private static final String LOWER_BOUNDS = "lowerBounds";
    private static final String UPPER_BOUNDS = "upperBounds";
    private static final String KEY = "key";
    private static final String VALUE = "value";

    private MetricsParser()
    {
    }

    public static String toJson(Metrics metrics)
    {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator generator = factory().createGenerator(writer);
            generator.writeStartObject();
            writeMap(generator, metrics.columnSizes(), COLUMN_SIZES);
            writeMap(generator, metrics.nullValueCounts(), NULL_VALUE_COUNTS);
            writeMap(generator, metrics.valueCounts(), VALUE_COUNTS);
            generator.writeNumberField(RECORD_COUNT, metrics.recordCount());
            writeMap(generator, metrics.lowerBounds(), LOWER_BOUNDS);
            writeMap(generator, metrics.upperBounds(), UPPER_BOUNDS);
            generator.writeEndObject();
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new RuntimeIOException("Json conversion failed for metrics = " + metrics, e);
        }
    }

    public static void writeMap(JsonGenerator generator, Map<Integer, ?> map, String fieldName)
            throws IOException
    {
        generator.writeArrayFieldStart(fieldName);
        if (map != null) {
            for (Map.Entry<Integer, ?> entry : map.entrySet()) {
                generator.writeStartObject();
                generator.writeNumberField(KEY, entry.getKey());
                if (entry.getValue() instanceof ByteBuffer) {
                    generator.writeBinaryField(VALUE, ((ByteBuffer) entry.getValue()).array());
                }
                else {
                    generator.writeNumberField(VALUE, (Long) entry.getValue());
                }
                generator.writeEndObject();
            }
        }
        generator.writeEndArray();
    }

    public static <V> Map<Integer, V> readMap(JsonNode arrayNode, Class<V> clazz)
            throws IOException
    {
        Map<Integer, V> map = new HashMap();
        for (JsonNode jsonNode : arrayNode) {
            final int key = jsonNode.get(KEY).intValue();
            if (clazz.equals(ByteBuffer.class)) {
                map.put(key, (V) ByteBuffer.wrap(jsonNode.get(VALUE).binaryValue()));
            }
            else if ((clazz.equals(Long.class))) {
                map.put(key, (V) Long.valueOf(jsonNode.get(VALUE).longValue()));
            }
            else {
                throw new UnsupportedOperationException("class = " + clazz + " is unsupported");
            }
        }
        return map;
    }

    public static Metrics fromJson(String json)
    {
        try {
            final JsonNode jsonNode = JsonUtil.mapper().readTree(json);
            final Map<Integer, Long> columnSizes = readMap(jsonNode.get(COLUMN_SIZES), Long.class);
            final Map<Integer, Long> nullValueCounts = readMap(jsonNode.get(NULL_VALUE_COUNTS), Long.class);
            final Map<Integer, Long> valueCounts = readMap(jsonNode.get(VALUE_COUNTS), Long.class);
            final Long recordCount = jsonNode.get(RECORD_COUNT).asLong();
            final Map<Integer, ByteBuffer> lowerBounds = readMap(jsonNode.get(LOWER_BOUNDS), ByteBuffer.class);
            final Map<Integer, ByteBuffer> upperBounds = readMap(jsonNode.get(UPPER_BOUNDS), ByteBuffer.class);
            return new Metrics(recordCount, columnSizes, valueCounts, nullValueCounts, lowerBounds, upperBounds);
        }
        catch (IOException e) {
            throw new RuntimeIOException("Failed to fromIceberg to Metrics instance for json = " + json, e);
        }
    }
}
