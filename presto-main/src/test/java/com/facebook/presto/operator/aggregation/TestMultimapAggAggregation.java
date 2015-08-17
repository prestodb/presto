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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.MultimapAggregation.NAME;
import static com.facebook.presto.operator.scalar.TestingRowConstructor.testRowBigintBigint;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeUtils.appendToBlockBuilder;
import static com.google.common.base.Preconditions.checkState;

public class TestMultimapAggAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testSingleValueMap()
            throws Exception
    {
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0), VARCHAR, ImmutableList.of("a"));
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0, 2.0, 3.0), VARCHAR, ImmutableList.of("a", "b", "c"));

        testMultimapAgg(VARCHAR, ImmutableList.of("a"), BIGINT, ImmutableList.of(1L));
        testMultimapAgg(VARCHAR, ImmutableList.of("a", "b", "c"), BIGINT, ImmutableList.of(1L, 2L, 3L));
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
        testMultimapAgg(DOUBLE, ImmutableList.<Double>of(), createDoublesBlock(null, null, null),
                VARCHAR, ImmutableList.<String>of(), createStringsBlock("a", "b", "c"));
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0), createDoublesBlock(1.0, null, null),
                VARCHAR, ImmutableList.of("a"), createStringsBlock("a", "c", "c"));
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0, 2.0), createDoublesBlock(1.0, 2.0, null),
                VARCHAR, ImmutableList.of("a", "b"), createStringsBlock("a", "b", "c"));
        testMultimapAgg(DOUBLE, ImmutableList.of(1.0, 1.0, 2.0), createDoublesBlock(1.0, 1.0, 2.0, null, null),
                VARCHAR, ImmutableList.of("a", "b", "c"), createStringsBlock("a", "b", "c", "", ""));
    }

    @Test
    public void testDoubleMapMultimap()
            throws Exception
    {
        Type mapType = new MapType(VARCHAR, BIGINT);
        List<Double> expectedKeys = ImmutableList.of(1.0, 2.0, 3.0);
        List<Map<String, Long>> expectedValues = ImmutableList.of(ImmutableMap.of("a", 1L), ImmutableMap.of("b", 2L, "c", 3L, "d", 4L), ImmutableMap.of("a", 1L));

        test(DOUBLE, expectedKeys, createBlock(DOUBLE, expectedKeys),
                mapType, expectedValues, createBlock(mapType, expectedValues));
        test(DOUBLE, expectedKeys, createBlock(DOUBLE, expectedKeys),
                mapType, expectedValues, createBlock(mapType, expectedValues));
    }

    @Test
    public void testDoubleArrayMultimap()
            throws Exception
    {
        Type arrayType = new ArrayType(VARCHAR);
        List<Double> expectedKeys = ImmutableList.of(1.0, 2.0, 3.0);
        List<List<String>> expectedValues = ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("c"), ImmutableList.of("d", "e", "f"));

        test(DOUBLE, expectedKeys, createBlock(DOUBLE, expectedKeys),
                arrayType, expectedValues, createBlock(arrayType, expectedValues));
        test(DOUBLE, expectedKeys, createBlock(DOUBLE, expectedKeys),
                arrayType, expectedValues, createBlock(arrayType, expectedValues));
    }

    private <K, V> void testMultimapAgg(
            Type keyType, List<K> expectedKeys,
            Type valueType, List<V> expectedValues)
    {
        testMultimapAgg(keyType, expectedKeys, createBlock(keyType, expectedKeys), valueType, expectedValues, createBlock(valueType, expectedValues));
    }

    private <K, V> void testMultimapAgg(
            Type keyType, List<K> expectedKeys, Block inputKeyBlock,
            Type valueType, List<V> expectedValues, Block inputValueBlock)
    {
        checkState(expectedKeys.size() == expectedValues.size(), "ExpectedKeys and ExpectedValues should have equal size");
        checkState(inputKeyBlock.getPositionCount() == inputValueBlock.getPositionCount(), "Keys and values should have equal size");

        test(keyType, expectedKeys, inputKeyBlock, valueType, expectedValues, inputValueBlock);
    }

    private <E> Block createBlock(Type type, List<E> list)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), list.size());
        for (E e : list) {
            appendToBlockBuilder(type, e, blockBuilder);
        }
        return blockBuilder.build();
    }

    private <K, V> void test(Type keyType, List<K> expectedKeys, Block inputKeyBlock,
            Type valueType, List<V> expectedValues, Block intputValueBlock)
    {
        MapType mapType = new MapType(keyType, new ArrayType(valueType));
        Signature signature = new Signature(NAME, mapType.getTypeSignature().toString(), keyType.toString(), valueType.toString());
        InternalAggregationFunction aggFunc = metadata.getExactFunction(signature).getAggregationFunction();

        assertAggregation(
                aggFunc,
                1.0,
                createReturnMultimap(expectedKeys, expectedValues),
                new Page(inputKeyBlock, intputValueBlock));
    }

    private <K, V> Map<K, List<V>> createReturnMultimap(List<K> keys, List<V> values)
    {
        Map<K, List<V>> map = Maps.newHashMap();
        for (int i = 0; i < keys.size(); i++) {
            if (!map.containsKey(keys.get(i))) {
                map.put(keys.get(i), Lists.newArrayList());
            }
            map.get(keys.get(i)).add(values.get(i));
        }

        return map.isEmpty() ? null : map;
    }

    @Test
    public void testDoubleRowMap()
            throws Exception
    {
        RowType innerRowType = new RowType(ImmutableList.of(BIGINT, DOUBLE), Optional.of(ImmutableList.of("f1", "f2")));
        MapType mapType = new MapType(DOUBLE, new ArrayType(innerRowType));
        Signature signature = new Signature(NAME, mapType.getTypeSignature().toString(), DOUBLE.toString(), innerRowType.toString());
        InternalAggregationFunction aggFunc = metadata.getExactFunction(signature).getAggregationFunction();

        PageBuilder builder = new PageBuilder(ImmutableList.of(DOUBLE, innerRowType));

        builder.declarePosition();
        DOUBLE.writeDouble(builder.getBlockBuilder(0), 1.0);
        innerRowType.writeObject(builder.getBlockBuilder(1), testRowBigintBigint(1L, 1.0));

        builder.declarePosition();
        DOUBLE.writeDouble(builder.getBlockBuilder(0), 2.0);
        innerRowType.writeObject(builder.getBlockBuilder(1), testRowBigintBigint(2L, 2.0));

        builder.declarePosition();
        DOUBLE.writeDouble(builder.getBlockBuilder(0), 3.0);
        innerRowType.writeObject(builder.getBlockBuilder(1), testRowBigintBigint(3L, 3.0));

        assertAggregation(
                aggFunc,
                1.0,
                ImmutableMap.of(1.0, ImmutableList.of(ImmutableList.of(1L, 1.0)),
                        2.0, ImmutableList.of(ImmutableList.of(2L, 2.0)),
                        3.0, ImmutableList.of(ImmutableList.of(3L, 3.0))),
                builder.build());
    }
}
