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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Metrics;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMetricsWrapper
{
    private static final JsonCodec<MetricsWrapper> CODEC = jsonCodec(MetricsWrapper.class);

    @Test
    public void testRoundTrip()
    {
        Long recordCount = 123L;
        Map<Integer, Long> columnSizes = ImmutableMap.of(3, 321L, 5, 543L);
        Map<Integer, Long> valueCounts = ImmutableMap.of(7, 765L, 9, 987L);
        Map<Integer, Long> nullValueCounts = ImmutableMap.of(2, 234L, 4, 456L);
        Map<Integer, ByteBuffer> lowerBounds = ImmutableMap.of(13, ByteBuffer.wrap(new byte[] {0, 8, 9}));
        Map<Integer, ByteBuffer> upperBounds = ImmutableMap.of(17, ByteBuffer.wrap(new byte[] {5, 4, 0}));

        Metrics expected = new Metrics(recordCount, columnSizes, valueCounts, nullValueCounts, lowerBounds, upperBounds);

        Metrics actual = CODEC.fromJson(CODEC.toJson(new MetricsWrapper(expected))).metrics();

        assertEquals(actual.recordCount(), recordCount);
        assertEquals(actual.columnSizes(), columnSizes);
        assertEquals(actual.valueCounts(), valueCounts);
        assertEquals(actual.nullValueCounts(), nullValueCounts);
        assertEquals(actual.lowerBounds(), lowerBounds);
        assertEquals(actual.upperBounds(), upperBounds);
    }

    @Test
    public void testAllPropertiesHandled()
    {
        Set<String> properties = getJsonProperties(MetricsWrapper.class);
        for (Method method : Metrics.class.getMethods()) {
            if (method.getDeclaringClass().equals(Method.class)) {
                assertTrue(properties.contains(method.getName()), "Metrics method not in wrapper: " + method);
            }
        }
    }

    private static Set<String> getJsonProperties(Type type)
    {
        ObjectMapper mapper = new JsonObjectMapperProvider().get();
        return mapper.getSerializationConfig()
                .introspect(mapper.getTypeFactory().constructType(type))
                .findProperties()
                .stream()
                .map(BeanPropertyDefinition::getName)
                .collect(toImmutableSet());
    }
}
