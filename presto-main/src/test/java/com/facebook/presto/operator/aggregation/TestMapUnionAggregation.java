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
import com.facebook.presto.operator.aggregation.state.KeyValuePairStateSerializer;
import com.facebook.presto.operator.aggregation.state.KeyValuePairsStateFactory.SingleState;
import com.facebook.presto.operator.scalar.TestingRowConstructor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.util.StructuralTestUtil;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.aggregation;
import static com.facebook.presto.operator.aggregation.MapUnionAggregation.NAME;
import static com.facebook.presto.operator.aggregation.TestMapUnionAggregation.MapSerializer.serializeAsMap;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.of;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

/**
 * Tests {@link MapUnionAggregation}.
 */
public class TestMapUnionAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypesTestNG")
    @Test
    public void testSimpleMapsWithDuplicates()
            throws Exception
    {
        MapType mapType = new MapType(DOUBLE, VARCHAR);
        InternalAggregationFunction aggFunc =
                metadata.getExactFunction(
                        new Signature(NAME,
                                mapType.getTypeSignature().toString(), mapType.getTypeSignature().toString())
                ).getAggregationFunction();

        assertEquals(
                ImmutableMap.of(23.0, "aaa", 33.0, "bbb", 43.0, "ccc", 53.0, "ddd", 13.0, "eee"),
                aggregation(aggFunc, 1.0,
                        serializeAsMap(DOUBLE, of(23.0, 33.0), VARCHAR, of("aaa", "bbb")),
                        serializeAsMap(DOUBLE, of(43.0, 53.0, 13.0), VARCHAR, of("ccc", "ddd", "eee")))
        );

        assertEquals(
                ImmutableMap.of(23.0, "aaa", 33.0, "bbb", 43.0, "ccc", 53.0, "ddd", 13.0, "eee"),
                aggregation(
                        aggFunc,
                        1.0,
                        serializeAsMap(DOUBLE, of(23.0, 33.0), VARCHAR, of("aaa", "bbb")),
                        serializeAsMap(DOUBLE, of(43.0, 53.0, 13.0), VARCHAR, of("ccc", "ddd", "eee"))));

        mapType = new MapType(DOUBLE, BIGINT);
        aggFunc = metadata.getExactFunction(
                new Signature(NAME, mapType.getTypeSignature().toString(), mapType.getTypeSignature().toString()))
                .getAggregationFunction();

        assertEquals(
                ImmutableMap.of(1.0, 99L, 2.0, 99L, 3.0, 99L, 4.0, 44L),
                aggregation(
                        aggFunc,
                        1.0,
                        serializeAsMap(DOUBLE, of(1.0, 2.0, 3.0), BIGINT, of(99L, 99L, 99L)),
                        serializeAsMap(DOUBLE, of(1.0, 2.0, 4.0), BIGINT, of(44L, 44L, 44L))));

        mapType = new MapType(BOOLEAN, BIGINT);
        aggFunc = metadata.getExactFunction(
                new Signature(NAME, mapType.getTypeSignature().toString(), mapType.getTypeSignature().toString()))
                .getAggregationFunction();

        assertEquals(
                ImmutableMap.of(false, 12L, true, 13L),
                aggregation(
                        aggFunc,
                        1.0,
                        serializeAsMap(BOOLEAN, of(false), BIGINT, of(12L)),
                        serializeAsMap(BOOLEAN, of(true, false), BIGINT, of(13L, 33L))));
    }

    @Test
    public void testSimpleMapsWithNulls()
            throws Exception
    {
        MapType mapType = new MapType(DOUBLE, VARCHAR);
        InternalAggregationFunction aggFunc =
                metadata.getExactFunction(
                        new Signature(NAME,
                                mapType.getTypeSignature().toString(), mapType.getTypeSignature().toString())
                ).getAggregationFunction();
        assertEquals(
                new HashMap<Double, String>()
                {
                    {
                        put(23.0, "aaa");
                        put(13.0, "eee");
                        put(null, "bbb");
                    }
                },
                aggregation(
                        aggFunc,
                        1.0,

                        serializeAsMap(DOUBLE, asList(23.0, null), VARCHAR, of("aaa", "bbb")),
                        serializeAsMap(DOUBLE, asList(null, 13.0), VARCHAR, of("ccc", "eee"))));

        mapType = new MapType(VARCHAR, BIGINT);
        aggFunc = metadata.getExactFunction(
                new Signature(NAME, mapType.getTypeSignature().toString(), mapType.getTypeSignature().toString()))
                .getAggregationFunction();
        assertEquals(
                new HashMap<String, Long>()
                {
                    {
                        put("foo", 13L);
                        put("bar", 54L);
                        put(null, 12L);
                    }
                },
                aggregation(
                        aggFunc,
                        1.0,
                        serializeAsMap(VARCHAR, asList(null, "foo"), BIGINT, of(12L, 13L)),
                        serializeAsMap(VARCHAR, asList(null, "bar", null), BIGINT, of(44L, 54L, 64L))));

        mapType = new MapType(BOOLEAN, BIGINT);
        aggFunc = metadata.getExactFunction(
                new Signature(NAME, mapType.getTypeSignature().toString(), mapType.getTypeSignature().toString()))
                .getAggregationFunction();

        assertEquals(
                new HashMap<Boolean, Long>()
                {
                    {
                        put(true, 100L);
                        put(false, 300L);
                        put(null, 33L);
                    }
                },
                aggregation(
                        aggFunc,
                        1.0,
                        serializeAsMap(BOOLEAN, singletonList(null), BIGINT, singletonList(33L)),
                        serializeAsMap(BOOLEAN, asList(true, null, false), BIGINT, asList(100L, 200L, 300L))));
    }

    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypesTestNG")
    @Test
    public void testDoubleArrayMap()
            throws Exception
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        MapType mapType = new MapType(DOUBLE, arrayType);
        InternalAggregationFunction aggFunc = metadata.getExactFunction(
                new Signature(NAME, mapType.getTypeSignature().toString(), mapType.getTypeSignature().toString())
        ).getAggregationFunction();

        assertEquals(
                ImmutableMap.of(
                        1.0, of("a", "b"),
                        2.0, of("c", "d"),
                        3.0, of("e", "f"),
                        4.0, of("r", "s")),
                aggregation(
                        aggFunc,
                        1.0,
                        serializeAsMap(
                                DOUBLE, of(1.0, 2.0, 3.0),
                                arrayType, of(of("a", "b"), of("c", "d"), of("e", "f"))),
                        serializeAsMap(
                                DOUBLE, of(1.0, 4.0, 3.0),
                                arrayType, of(of("x", "y"), of("r", "s"), of("w", "z")))
                ));
    }

    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypesTestNG")
    @Test
    public void testDoubleMapMap()
            throws Exception
    {
        MapType innerMapType = new MapType(VARCHAR, VARCHAR);
        MapType mapType = new MapType(DOUBLE, innerMapType);
        InternalAggregationFunction aggFunc = metadata.getExactFunction(new Signature(NAME,
                mapType.getTypeSignature().toString(),
                mapType.getTypeSignature().toString())).getAggregationFunction();

        assertEquals(
                ImmutableMap.of(
                        1.0, ImmutableMap.of("a", "b"),
                        2.0, ImmutableMap.of("c", "d"),
                        3.0, ImmutableMap.of("e", "f")),
                aggregation(
                        aggFunc,
                        1.0, serializeAsMap(
                                DOUBLE, of(1.0, 2.0),
                                innerMapType,
                                of(ImmutableMap.of("a", "b"), ImmutableMap.of("c", "d"))),
                        serializeAsMap(
                                DOUBLE, of(3.0),
                                innerMapType,
                                of(ImmutableMap.of("e", "f")))
                ));
    }

    @Test
    public void testDoubleRowMap()
            throws Exception
    {
        RowType innerRowType = new RowType(of(BIGINT, DOUBLE), Optional.of(of("f1", "f2")));
        MapType mapType = new MapType(DOUBLE, innerRowType);
        InternalAggregationFunction aggFunc = metadata.getExactFunction(new Signature(NAME,
                        mapType.getTypeSignature().toString(),
                        mapType.getTypeSignature().toString())
        ).getAggregationFunction();

        assertEquals(
                ImmutableMap.of(
                        1.0, of(1L, 1.0),
                        2.0, of(2L, 2.0),
                        3.0, of(3L, 3.0),
                        4.0, of(4L, 4.0),
                        5.0, of(5L, 5.0)),
                aggregation(
                        aggFunc,
                        1.0,
                        serializeAsMap(
                                DOUBLE, of(1.0, 3.0),
                                innerRowType, of(of(1L, 1.0), of(3L, 3.0))),
                        serializeAsMap(
                                DOUBLE, of(4.0, 2.0, 5.0),
                                innerRowType, of(of(4L, 4.0), of(2L, 2.0), of(5L, 5.0)))
                ));
    }

    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypesTestNG")
    @Test
    public void testArrayDoubleMap()
            throws Exception
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        MapType mapType = new MapType(arrayType, DOUBLE);
        InternalAggregationFunction aggFunc = metadata.getExactFunction(new Signature(
                        NAME,
                        mapType.getTypeSignature().toString(),
                        mapType.getTypeSignature().toString())
        ).getAggregationFunction();

        assertEquals(
                ImmutableMap.of(
                        of("a", "b"), 1.0,
                        of("c", "d"), 2.0,
                        of("e", "f"), 3.0),
                aggregation(
                        aggFunc,
                        1.0,
                        serializeAsMap(
                                arrayType, of(of("a", "b"), of("e", "f")),
                                DOUBLE, of(1.0, 3.0)),
                        serializeAsMap(
                                arrayType, of(of("c", "d")),
                                DOUBLE, of(2.0))
                ));
    }

    abstract static class MapSerializer
    {
        static Page serializeAsMap(Type keyType, List<?> keys, Type valueType, List<?> values)
        {
            checkArgument(keys.size() == values.size(), "Length of keys and values should be the same");

            //Serialize the keys and values together as a KeyValuePairs type.
            KeyValuePairs keyValuePairs = new KeyValuePairs(keyType, valueType);
            Block keyBlock = serializeAsListOfX(keyType, keys);
            Block valueBlock = serializeAsListOfX(valueType, values);
            int size = keys.size();
            for (int i = 0; i < size; i++) {
                keyValuePairs.add(keyBlock, valueBlock, i, i);
            }

            SingleState singleState = new SingleState(keyType, valueType);
            singleState.set(keyValuePairs);

            PageBuilder builder = new PageBuilder(of(new MapType(keyType, valueType)));
            builder.declarePosition();
            new KeyValuePairStateSerializer(keyType, valueType, false)
                    .serialize(singleState, builder.getBlockBuilder(0));
            return builder.build();
        }

        @SuppressWarnings("unchecked")
        private static Block serializeAsListOfX(Type type, List<?> items)
        {
            Block valueBlock;
            if (type instanceof ArrayType) {
                valueBlock = serializeAsListOfArrays((ArrayType) type, (Iterable<List<?>>) items).getBlockBuilder(0);
            }
            else if (type instanceof MapType) {
                valueBlock = serializeAsListOfMaps((MapType) type, (Iterable<Map<?, ?>>) items).getBlockBuilder(0);
            }
            else if (type instanceof RowType) {
                valueBlock = serializeAsListOfRows((RowType) type, (Iterable<List<?>>) items).getBlockBuilder(0);
            }
            else {
                valueBlock = serializeAsListOfPrimitives(type, items);
            }
            return valueBlock;
        }

        @SuppressWarnings("unchecked")
        private static Block serializeAsListOfPrimitives(Type type, Iterable<?> items)
        {
            if (type == BIGINT) {
                return createLongsBlock((Iterable<Long>) items);
            }
            else if (type == DOUBLE) {
                return createDoublesBlock((Iterable<Double>) items);
            }
            else if (type == VARCHAR) {
                return createStringsBlock((Iterable<String>) items);
            }
            else if (type == BOOLEAN) {
                return createBooleansBlock((Iterable<Boolean>) items);
            }

            throw new IllegalArgumentException("Unsupported type [" + type.getDisplayName() + "]");
        }

        private static PageBuilder serializeAsListOfArrays(ArrayType arrayType, Iterable<List<?>> items)
        {
            PageBuilder builder = new PageBuilder(of(arrayType));
            for (List<?> subItems : items) {
                builder.declarePosition();
                arrayType.writeObject(
                        builder.getBlockBuilder(0),
                        StructuralTestUtil.arrayBlockOf(arrayType.getElementType(), (Object[]) subItems.toArray()));
            }
            return builder;
        }

        private static PageBuilder serializeAsListOfMaps(MapType mapType, Iterable<Map<?, ?>> items)
        {
            PageBuilder builder = new PageBuilder(of(mapType));
            for (Map<?, ?> subItems : items) {
                builder.declarePosition();
                mapType.writeObject(
                        builder.getBlockBuilder(0),
                        StructuralTestUtil.mapBlockOf(mapType.getKeyType(), mapType.getValueType(), subItems));
            }
            return builder;
        }

        private static PageBuilder serializeAsListOfRows(RowType rowType, Iterable<List<?>> items)
        {
            PageBuilder builder = new PageBuilder(of(rowType));
            for (List<?> subItems : items) {
                builder.declarePosition();
                Block block = TestingRowConstructor
                        .toStackRepresentation(rowType.getTypeParameters(), (Object[]) subItems.toArray());
                rowType.writeObject(builder.getBlockBuilder(0), block);
            }
            return builder;
        }
    }
}
