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
package com.facebook.presto.operator.aggregation.minmaxby;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createShortDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestMinMaxByAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = metadata.getFunctionAndTypeManager();

    @Test
    public void testAllRegistered()
    {
        Set<Type> orderableTypes = getTypes().stream()
                .filter(Type::isOrderable)
                .collect(toImmutableSet());

        for (Type keyType : orderableTypes) {
            for (Type valueType : getTypes()) {
                if (StateCompiler.getSupportedFieldTypes().contains(valueType.getJavaType())) {
                    assertNotNull(getMinByAggregation(valueType, keyType));
                    assertNotNull(getMaxByAggregation(valueType, keyType));
                }
            }
        }
    }

    private static List<Type> getTypes()
    {
        List<Type> simpleTypes = metadata.getFunctionAndTypeManager().getTypes();
        return new ImmutableList.Builder<Type>()
                .addAll(simpleTypes)
                .add(VARCHAR)
                .add(createDecimalType(1))
                .add(RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR, DOUBLE)))
                .build();
    }

    @Test
    public void testMinUnknown()
    {
        JavaAggregationFunctionImplementation unknownKey = getMinByAggregation(UNKNOWN, DOUBLE);
        assertAggregation(
                unknownKey,
                null,
                createBooleansBlock(null, null),
                createDoublesBlock(1.0, 2.0));
        JavaAggregationFunctionImplementation unknownValue = getMinByAggregation(DOUBLE, UNKNOWN);
        assertAggregation(
                unknownValue,
                null,
                createDoublesBlock(1.0, 2.0),
                createBooleansBlock(null, null));
    }

    @Test
    public void testMaxUnknown()
    {
        JavaAggregationFunctionImplementation unknownKey = getMaxByAggregation(UNKNOWN, DOUBLE);
        assertAggregation(
                unknownKey,
                null,
                createBooleansBlock(null, null),
                createDoublesBlock(1.0, 2.0));
        JavaAggregationFunctionImplementation unknownValue = getMaxByAggregation(DOUBLE, UNKNOWN);
        assertAggregation(
                unknownValue,
                null,
                createDoublesBlock(1.0, 2.0),
                createBooleansBlock(null, null));
    }

    @Test
    public void testMinNull()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(DOUBLE, DOUBLE);
        assertAggregation(
                function,
                1.0,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                function,
                10.0,
                createDoublesBlock(10.0, 9.0, 8.0, 11.0),
                createDoublesBlock(1.0, null, 2.0, null));
    }

    @Test
    public void testMaxNull()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(DOUBLE, DOUBLE);
        assertAggregation(
                function,
                null,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                function,
                10.0,
                createDoublesBlock(8.0, 9.0, 10.0, 11.0),
                createDoublesBlock(-2.0, null, -1.0, null));
    }

    @Test
    public void testMinDoubleDouble()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(DOUBLE, DOUBLE);
        assertAggregation(
                function,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                function,
                3.0,
                createDoublesBlock(3.0, 2.0, 5.0, 3.0),
                createDoublesBlock(1.0, 1.5, 2.0, 4.0));
    }

    @Test
    public void testMaxDoubleDouble()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(DOUBLE, DOUBLE);
        assertAggregation(
                function,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                function,
                2.0,
                createDoublesBlock(3.0, 2.0, null),
                createDoublesBlock(1.0, 1.5, null));
    }

    @Test
    public void testMinDoubleVarchar()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                function,
                "z",
                createStringsBlock("z", "a", "x", "b"),
                createDoublesBlock(1.0, 2.0, 2.0, 3.0));

        assertAggregation(
                function,
                "a",
                createStringsBlock("zz", "hi", "bb", "a"),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0));
    }

    @Test
    public void testMaxDoubleVarchar()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(VARCHAR, DOUBLE);
        assertAggregation(
                function,
                "a",
                createStringsBlock("z", "a", null),
                createDoublesBlock(1.0, 2.0, null));

        assertAggregation(
                function,
                "hi",
                createStringsBlock("zz", "hi", null, "a"),
                createDoublesBlock(0.0, 1.0, null, -1.0));
    }

    @Test
    public void testMinLongLongArray()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(new ArrayType(BIGINT), BIGINT);
        assertAggregation(
                function,
                ImmutableList.of(8L, 9L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L))),
                createLongsBlock(1L, 2L, 2L, 3L));

        assertAggregation(
                function,
                ImmutableList.of(2L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L), ImmutableList.of(2L))),
                createLongsBlock(0L, 1L, 2L, -1L));
    }

    @Test
    public void testMinLongArrayLong()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(BIGINT, new ArrayType(BIGINT));
        assertAggregation(
                function,
                3L,
                createLongsBlock(1L, 2L, 2L, 3L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(1L, 1L))));

        assertAggregation(
                function,
                -1L,
                createLongsBlock(0L, 1L, 2L, -1L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(6L, 7L), ImmutableList.of(-1L, -3L), ImmutableList.of(-1L))));
    }

    @Test
    public void testMaxLongArrayLong()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(BIGINT, new ArrayType(BIGINT));
        assertAggregation(
                function,
                1L,
                createLongsBlock(1L, 2L, 2L, 3L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(1L, 1L))));

        assertAggregation(
                function,
                2L,
                createLongsBlock(0L, 1L, 2L, -1L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(-8L, 9L), ImmutableList.of(-6L, 7L), ImmutableList.of(-1L, -3L), ImmutableList.of(-1L))));
    }

    @Test
    public void testMaxLongLongArray()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(new ArrayType(BIGINT), BIGINT);
        assertAggregation(
                function,
                ImmutableList.of(1L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(1L, 2L), null)),
                createLongsBlock(1L, 2L, null));

        assertAggregation(
                function,
                ImmutableList.of(2L, 3L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(2L, 3L), null, asList(1L, 2L))),
                createLongsBlock(0L, 1L, null, -1L));
    }

    @Test
    public void testMinLongDecimalDecimal()
    {
        Type decimalType = createDecimalType(19, 1);
        JavaAggregationFunctionImplementation function = getMinByAggregation(decimalType, decimalType);
        assertAggregation(
                function,
                SqlDecimal.of("2.2"),
                createLongDecimalsBlock("1.1", "2.2", "3.3"),
                createLongDecimalsBlock("1.2", "1.0", "2.0"));
    }

    @Test
    public void testMaxLongDecimalDecimal()
    {
        Type decimalType = createDecimalType(19, 1);
        JavaAggregationFunctionImplementation function = getMaxByAggregation(decimalType, decimalType);
        assertAggregation(
                function,
                SqlDecimal.of("3.3"),
                createLongDecimalsBlock("1.1", "2.2", "3.3", "4.4"),
                createLongDecimalsBlock("1.2", "1.0", "2.0", "1.5"));
    }

    @Test
    public void testMinShortDecimalDecimal()
    {
        Type decimalType = createDecimalType(10, 1);
        JavaAggregationFunctionImplementation function = getMinByAggregation(decimalType, decimalType);
        assertAggregation(
                function,
                SqlDecimal.of("2.2"),
                createShortDecimalsBlock("1.1", "2.2", "3.3"),
                createShortDecimalsBlock("1.2", "1.0", "2.0"));
    }

    @Test
    public void testMaxShortDecimalDecimal()
    {
        Type decimalType = createDecimalType(10, 1);
        JavaAggregationFunctionImplementation function = getMaxByAggregation(decimalType, decimalType);
        assertAggregation(
                function,
                SqlDecimal.of("3.3"),
                createShortDecimalsBlock("1.1", "2.2", "3.3", "4.4"),
                createShortDecimalsBlock("1.2", "1.0", "2.0", "1.5"));
    }

    @Test
    public void testMinBooleanVarchar()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(VARCHAR, BOOLEAN);
        assertAggregation(
                function,
                "b",
                createStringsBlock("a", "b", "c"),
                createBooleansBlock(true, false, true));
    }

    @Test
    public void testMaxBooleanVarchar()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(VARCHAR, BOOLEAN);
        assertAggregation(
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createBooleansBlock(false, false, true));
    }

    @Test
    public void testMinIntegerVarchar()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(VARCHAR, INTEGER);
        assertAggregation(
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createIntsBlock(1, 2, 3));
    }

    @Test
    public void testMaxIntegerVarchar()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(VARCHAR, INTEGER);
        assertAggregation(
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createIntsBlock(1, 2, 3));
    }

    @Test
    public void testMinBooleanLongArray()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(new ArrayType(BIGINT), BOOLEAN);
        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, null)),
                createBooleansBlock(true, false, true));
    }

    @Test
    public void testMaxBooleanLongArray()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(new ArrayType(BIGINT), BOOLEAN);
        assertAggregation(
                function,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createBooleansBlock(false, false, true));
    }

    @Test
    public void testMinLongVarchar()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(VARCHAR, BIGINT);
        assertAggregation(
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createLongsBlock(1, 2, 3));
    }

    @Test
    public void testMaxLongVarchar()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(VARCHAR, BIGINT);
        assertAggregation(
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createLongsBlock(1, 2, 3));
    }

    @Test
    public void testMinDoubleLongArray()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(new ArrayType(BIGINT), DOUBLE);
        assertAggregation(
                function,
                asList(3L, 4L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(1.0, 2.0, 3.0));

        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(null, null, asList(2L, 2L))),
                createDoublesBlock(0.0, 1.0, 2.0));
    }

    @Test
    public void testMaxDoubleLongArray()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(new ArrayType(BIGINT), DOUBLE);
        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(1.0, 2.0, null));

        assertAggregation(
                function,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(0.0, 1.0, 2.0));
    }

    @Test
    public void testMinSliceLongArray()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(new ArrayType(BIGINT), VARCHAR);
        assertAggregation(
                function,
                asList(3L, 4L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(null, null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));
    }

    @Test
    public void testMaxSliceLongArray()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(new ArrayType(BIGINT), VARCHAR);
        assertAggregation(
                function,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, null)),
                createStringsBlock("a", "b", "c"));
    }

    @Test
    public void testMinLongArrayLongArray()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(new ArrayType(BIGINT), new ArrayType(BIGINT));
        assertAggregation(
                function,
                asList(1L, 2L),
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMaxLongArrayLongArray()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(new ArrayType(BIGINT), new ArrayType(BIGINT));
        assertAggregation(
                function,
                asList(3L, 3L),
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMinLongArraySlice()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(VARCHAR, new ArrayType(BIGINT));
        assertAggregation(
                function,
                "c",
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMaxLongArraySlice()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(VARCHAR, new ArrayType(BIGINT));
        assertAggregation(
                function,
                "a",
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMinUnknownSlice()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(VARCHAR, UNKNOWN);
        assertAggregation(
                function,
                null,
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMaxUnknownSlice()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(VARCHAR, UNKNOWN);
        assertAggregation(
                function,
                null,
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMinUnknownLongArray()
    {
        JavaAggregationFunctionImplementation function = getMinByAggregation(new ArrayType(BIGINT), UNKNOWN);
        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMaxUnknownLongArray()
    {
        JavaAggregationFunctionImplementation function = getMaxByAggregation(new ArrayType(BIGINT), UNKNOWN);
        assertAggregation(
                function,
                null,
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testLongAndBlockPositionValueStateSerialization()
    {
        Type mapType = mapType(VARCHAR, BOOLEAN);
        Map<String, Type> fieldMap = ImmutableMap.of("Key", BIGINT, "Value", mapType);
        AccumulatorStateFactory<LongAndBlockPositionValueState> factory = StateCompiler.generateStateFactory(LongAndBlockPositionValueState.class, fieldMap, new DynamicClassLoader(LongAndBlockPositionValueState.class.getClassLoader()));
        KeyAndBlockPositionValueStateSerializer<LongAndBlockPositionValueState> serializer = new LongAndBlockPositionStateSerializer(BIGINT, mapType);
        LongAndBlockPositionValueState singleState = factory.createSingleState();
        LongAndBlockPositionValueState deserializedState = factory.createSingleState();
        singleState.setFirst(2020);
        singleState.setFirstNull(false);

        BlockBuilder builder = RowType.anonymous(ImmutableList.of(BOOLEAN, BOOLEAN, BIGINT, mapType))
                .createBlockBuilder(null, 1);
        serializer.serialize(singleState, builder);

        Block rowBlock = builder.build();
        DictionaryBlock dictionaryBlock = (DictionaryBlock) rowBlock.getPositions(new int[]{0}, 0, 1);
        serializer.deserialize(dictionaryBlock, 0, deserializedState);

        assertEquals(deserializedState.isFirstNull(), singleState.isFirstNull());
        assertEquals(deserializedState.getFirst(), singleState.getFirst());
    }

    private JavaAggregationFunctionImplementation getMinByAggregation(Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction("min_by", fromTypes(arguments)));
    }

    private JavaAggregationFunctionImplementation getMaxByAggregation(Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction("max_by", fromTypes(arguments)));
    }
}
