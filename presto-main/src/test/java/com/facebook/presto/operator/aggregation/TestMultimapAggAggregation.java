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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.RowPageBuilder;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.MultimapAggregationFunction.NAME;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;

public class TestMultimapAggAggregation
{
    private static final MetadataManager metadata = createTestMetadataManager();

    @Test
    public void testSingleValueMap()
            throws Exception
    {
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0), VARCHAR, ImmutableList.of("a"));
        testMultimapAgg(VARCHAR, ImmutableList.of("a"), BIGINT, ImmutableList.of(1L));
    }

    @Test
    public void testMultiValueMap()
            throws Exception
    {
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0, 1.0, 1.0), VARCHAR, ImmutableList.of("a", "b", "c"));
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0, 1.0, 2.0), VARCHAR, ImmutableList.of("a", "b", "c"));
    }

    @Test
    public void testOrderValueMap()
            throws Exception
    {
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "a", "a"), BIGINT, ImmutableList.of(1L, 2L, 3L));
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "a", "a"), BIGINT, ImmutableList.of(2L, 1L, 3L));
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "a", "a"), BIGINT, ImmutableList.of(3L, 2L, 1L));
    }

    @Test
    public void testDuplicateValueMap()
            throws Exception
    {
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "a", "a"), BIGINT, ImmutableList.of(1L, 1L, 1L));
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "b", "a", "b", "c"), BIGINT, ImmutableList.of(1L, 1L, 1L, 1L, 1L));
    }

    @Test
    public void testNullMap()
            throws Exception
    {
        testMultimapAgg(DOUBLE, ImmutableList.<Double>of(), VARCHAR, ImmutableList.<String>of());
    }

    @Test
    public void testDoubleMapMultimap()
            throws Exception
    {
        Type mapType = new MapType(VARCHAR, BIGINT);
        List<Double> expectedKeys = ImmutableList.of(1.0, 2.0, 3.0);
        List<Map<String, Long>> expectedValues = ImmutableList.of(ImmutableMap.of("a", 1L), ImmutableMap.of("b", 2L, "c", 3L, "d", 4L), ImmutableMap.of("a", 1L));

        testMultimapAgg(DOUBLE, expectedKeys, mapType, expectedValues);
    }

    @Test
    public void testDoubleArrayMultimap()
            throws Exception
    {
        Type arrayType = new ArrayType(VARCHAR);
        List<Double> expectedKeys = ImmutableList.of(1.0, 2.0, 3.0);
        List<List<String>> expectedValues = ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("c"), ImmutableList.of("d", "e", "f"));

        testMultimapAgg(DOUBLE, expectedKeys, arrayType, expectedValues);
    }

    @Test
    public void testDoubleRowMap()
            throws Exception
    {
        RowType innerRowType = new RowType(ImmutableList.of(BIGINT, DOUBLE), Optional.of(ImmutableList.of("f1", "f2")));
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0, 2.0, 3.0), innerRowType, ImmutableList.of(ImmutableList.of(1L, 1.0), ImmutableList.of(2L, 2.0), ImmutableList.of(3L, 3.0)));
    }

    private static <K, V> void testMultimapAgg(Type keyType, List<K> expectedKeys, Type valueType, List<V> expectedValues)
    {
        checkState(expectedKeys.size() == expectedValues.size(), "expectedKeys and expectedValues should have equal size");

        MapType mapType = new MapType(keyType, new ArrayType(valueType));
        Signature signature = new Signature(NAME, AGGREGATE, mapType.getTypeSignature(), keyType.getTypeSignature(), valueType.getTypeSignature());
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(signature);

        Map<K, List<V>> map = new HashMap<>();
        for (int i = 0; i < expectedKeys.size(); i++) {
            if (!map.containsKey(expectedKeys.get(i))) {
                map.put(expectedKeys.get(i), new ArrayList<>());
            }
            map.get(expectedKeys.get(i)).add(expectedValues.get(i));
        }

        RowPageBuilder builder = RowPageBuilder.rowPageBuilder(keyType, valueType);
        for (int i = 0; i < expectedKeys.size(); i++) {
            builder.row(expectedKeys.get(i), expectedValues.get(i));
        }

        assertAggregation(aggFunc, map.isEmpty() ? null : map, builder.build().getBlocks());
    }
}
