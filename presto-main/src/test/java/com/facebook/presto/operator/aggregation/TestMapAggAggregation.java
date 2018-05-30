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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createStringArraysBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.block.BlockAssertions.createTypedLongsBlock;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.OperatorAssertion.toRow;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.MapAggregationFunction.NAME;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.mapBlockOf;
import static com.facebook.presto.util.StructuralTestUtil.mapType;

public class TestMapAggAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testDuplicateKeysValues()
    {
        MapType mapType = mapType(DOUBLE, VARCHAR);
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, "a"),
                createDoublesBlock(1.0, 1.0, 1.0),
                createStringsBlock("a", "b", "c"));

        mapType = mapType(DOUBLE, INTEGER);
        aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.INTEGER)));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, 99, 2.0, 99, 3.0, 99),
                createDoublesBlock(1.0, 2.0, 3.0),
                createTypedLongsBlock(INTEGER, ImmutableList.of(99L, 99L, 99L)));
    }

    @Test
    public void testSimpleMaps()
    {
        MapType mapType = mapType(DOUBLE, VARCHAR);
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, "a", 2.0, "b", 3.0, "c"),
                createDoublesBlock(1.0, 2.0, 3.0),
                createStringsBlock("a", "b", "c"));

        mapType = mapType(DOUBLE, INTEGER);
        aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.INTEGER)));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, 3, 2.0, 2, 3.0, 1),
                createDoublesBlock(1.0, 2.0, 3.0),
                createTypedLongsBlock(INTEGER, ImmutableList.of(3L, 2L, 1L)));

        mapType = mapType(DOUBLE, BOOLEAN);
        aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType.getTypeSignature(),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, true, 2.0, false, 3.0, false),
                createDoublesBlock(1.0, 2.0, 3.0),
                createBooleansBlock(true, false, false));
    }

    @Test
    public void testNull()
    {
        InternalAggregationFunction doubleDouble = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature(NAME,
                        AGGREGATE,
                        mapType(DOUBLE, DOUBLE).getTypeSignature(),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.DOUBLE)));
        assertAggregation(
                doubleDouble,
                ImmutableMap.of(1.0, 2.0),
                createDoublesBlock(1.0, null, null),
                createDoublesBlock(2.0, 3.0, 4.0));

        assertAggregation(
                doubleDouble,
                null,
                createDoublesBlock(null, null, null),
                createDoublesBlock(2.0, 3.0, 4.0));

        Map<Double, Double> expected = new LinkedHashMap<>();
        expected.put(1.0, null);
        expected.put(2.0, null);
        expected.put(3.0, null);
        assertAggregation(
                doubleDouble,
                expected,
                createDoublesBlock(1.0, 2.0, 3.0),
                createDoublesBlock(null, null, null));
    }

    @Test
    public void testDoubleArrayMap()
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        MapType mapType = mapType(DOUBLE, arrayType);
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(new Signature(NAME,
                AGGREGATE,
                mapType.getTypeSignature(),
                parseTypeSignature(StandardTypes.DOUBLE),
                arrayType.getTypeSignature()));

        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, ImmutableList.of("a", "b"),
                        2.0, ImmutableList.of("c", "d"),
                        3.0, ImmutableList.of("e", "f")),
                createDoublesBlock(1.0, 2.0, 3.0),
                createStringArraysBlock(ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("c", "d"), ImmutableList.of("e", "f"))));
    }

    @Test
    public void testDoubleMapMap()
    {
        MapType innerMapType = mapType(VARCHAR, VARCHAR);
        MapType mapType = mapType(DOUBLE, innerMapType);
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(new Signature(NAME,
                AGGREGATE,
                mapType.getTypeSignature(),
                parseTypeSignature(StandardTypes.DOUBLE),
                innerMapType.getTypeSignature()));

        BlockBuilder builder = innerMapType.createBlockBuilder(null, 3);
        innerMapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("a", "b")));
        innerMapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("c", "d")));
        innerMapType.writeObject(builder, mapBlockOf(VARCHAR, VARCHAR, ImmutableMap.of("e", "f")));

        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, ImmutableMap.of("a", "b"),
                        2.0, ImmutableMap.of("c", "d"),
                        3.0, ImmutableMap.of("e", "f")),
                createDoublesBlock(1.0, 2.0, 3.0),
                builder.build());
    }

    @Test
    public void testDoubleRowMap()
    {
        RowType innerRowType = RowType.from(ImmutableList.of(
                RowType.field("f1", INTEGER),
                RowType.field("f2", DOUBLE)));
        MapType mapType = mapType(DOUBLE, innerRowType);
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(new Signature(NAME,
                AGGREGATE,
                mapType.getTypeSignature(),
                parseTypeSignature(StandardTypes.DOUBLE),
                innerRowType.getTypeSignature()));

        BlockBuilder builder = innerRowType.createBlockBuilder(null, 3);
        innerRowType.writeObject(builder, toRow(ImmutableList.of(INTEGER, DOUBLE), 1L, 1.0));
        innerRowType.writeObject(builder, toRow(ImmutableList.of(INTEGER, DOUBLE), 2L, 2.0));
        innerRowType.writeObject(builder, toRow(ImmutableList.of(INTEGER, DOUBLE), 3L, 3.0));

        assertAggregation(
                aggFunc,
                ImmutableMap.of(1.0, ImmutableList.of(1, 1.0),
                        2.0, ImmutableList.of(2, 2.0),
                        3.0, ImmutableList.of(3, 3.0)),
                createDoublesBlock(1.0, 2.0, 3.0),
                builder.build());
    }

    @Test
    public void testArrayDoubleMap()
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        MapType mapType = mapType(arrayType, DOUBLE);
        InternalAggregationFunction aggFunc = metadata.getFunctionRegistry().getAggregateFunctionImplementation(new Signature(
                NAME,
                AGGREGATE,
                mapType.getTypeSignature(),
                arrayType.getTypeSignature(),
                parseTypeSignature(StandardTypes.DOUBLE)));

        assertAggregation(
                aggFunc,
                ImmutableMap.of(
                        ImmutableList.of("a", "b"), 1.0,
                        ImmutableList.of("c", "d"), 2.0,
                        ImmutableList.of("e", "f"), 3.0),
                createStringArraysBlock(ImmutableList.of(ImmutableList.of("a", "b"), ImmutableList.of("c", "d"), ImmutableList.of("e", "f"))),
                createDoublesBlock(1.0, 2.0, 3.0));
    }
}
