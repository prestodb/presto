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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.groupedAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;

public class TestMinMaxByNAggregation
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    @Test
    public void testMaxDoubleDouble()
    {
        InternalAggregationFunction function = getMaxByAggregation(DOUBLE, DOUBLE, BIGINT);
        assertAggregation(
                function,
                Arrays.asList((Double) null),
                createDoublesBlock(1.0, null),
                createDoublesBlock(3.0, 5.0),
                createRLEBlock(1L, 2));

        assertAggregation(
                function,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null),
                createRLEBlock(1L, 2));

        assertAggregation(
                function,
                Arrays.asList(1.0),
                createDoublesBlock(null, 1.0, null, null),
                createDoublesBlock(null, 0.0, null, null),
                createRLEBlock(2L, 4));

        assertAggregation(
                function,
                Arrays.asList(1.0),
                createDoublesBlock(1.0),
                createDoublesBlock(0.0),
                createRLEBlock(2L, 1));

        assertAggregation(
                function,
                null,
                createDoublesBlock(),
                createDoublesBlock(),
                createRLEBlock(2L, 0));

        assertAggregation(
                function,
                ImmutableList.of(2.5),
                createDoublesBlock(2.5, 2.0, 5.0, 3.0),
                createDoublesBlock(4.0, 1.5, 2.0, 3.0),
                createRLEBlock(1L, 4));

        assertAggregation(
                function,
                ImmutableList.of(2.5, 3.0),
                createDoublesBlock(2.5, 2.0, 5.0, 3.0),
                createDoublesBlock(4.0, 1.5, 2.0, 3.0),
                createRLEBlock(2L, 4));
    }

    @Test
    public void testMinDoubleDouble()
    {
        InternalAggregationFunction function = getMinByAggregation(DOUBLE, DOUBLE, BIGINT);
        assertAggregation(
                function,
                Arrays.asList((Double) null),
                createDoublesBlock(1.0, null),
                createDoublesBlock(5.0, 3.0),
                createRLEBlock(1L, 2));

        assertAggregation(
                function,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null),
                createRLEBlock(1L, 2));

        assertAggregation(
                function,
                ImmutableList.of(2.0),
                createDoublesBlock(2.5, 2.0, 5.0, 3.0),
                createDoublesBlock(4.0, 1.5, 2.0, 3.0),
                createRLEBlock(1L, 4));

        assertAggregation(
                function,
                ImmutableList.of(2.0, 5.0),
                createDoublesBlock(2.5, 2.0, 5.0, 3.0),
                createDoublesBlock(4.0, 1.5, 2.0, 3.0),
                createRLEBlock(2L, 4));
    }

    @Test
    public void testMinDoubleVarchar()
    {
        InternalAggregationFunction function = getMinByAggregation(VARCHAR, DOUBLE, BIGINT);
        assertAggregation(
                function,
                ImmutableList.of("z", "a"),
                createStringsBlock("z", "a", "x", "b"),
                createDoublesBlock(1.0, 2.0, 2.0, 3.0),
                createRLEBlock(2L, 4));

        assertAggregation(
                function,
                ImmutableList.of("a", "zz"),
                createStringsBlock("zz", "hi", "bb", "a"),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0),
                createRLEBlock(2L, 4));

        assertAggregation(
                function,
                ImmutableList.of("a", "zz"),
                createStringsBlock("zz", "hi", null, "a"),
                createDoublesBlock(0.0, 1.0, null, -1.0),
                createRLEBlock(2L, 4));
    }

    @Test
    public void testMaxDoubleVarchar()
    {
        InternalAggregationFunction function = getMaxByAggregation(VARCHAR, DOUBLE, BIGINT);
        assertAggregation(
                function,
                ImmutableList.of("a", "z"),
                createStringsBlock("z", "a", null),
                createDoublesBlock(1.0, 2.0, null),
                createRLEBlock(2L, 3));

        assertAggregation(
                function,
                ImmutableList.of("bb", "hi"),
                createStringsBlock("zz", "hi", "bb", "a"),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0),
                createRLEBlock(2L, 4));

        assertAggregation(
                function,
                ImmutableList.of("hi", "zz"),
                createStringsBlock("zz", "hi", null, "a"),
                createDoublesBlock(0.0, 1.0, null, -1.0),
                createRLEBlock(2L, 4));
    }

    @Test
    public void testMinVarcharDouble()
    {
        InternalAggregationFunction function = getMinByAggregation(DOUBLE, VARCHAR, BIGINT);
        assertAggregation(
                function,
                ImmutableList.of(2.0, 3.0),
                createDoublesBlock(1.0, 2.0, 2.0, 3.0),
                createStringsBlock("z", "a", "x", "b"),
                createRLEBlock(2L, 4));

        assertAggregation(
                function,
                ImmutableList.of(-1.0, 2.0),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0),
                createStringsBlock("zz", "hi", "bb", "a"),
                createRLEBlock(2L, 4));

        assertAggregation(
                function,
                ImmutableList.of(-1.0, 1.0),
                createDoublesBlock(0.0, 1.0, null, -1.0),
                createStringsBlock("zz", "hi", null, "a"),
                createRLEBlock(2L, 4));
    }

    @Test
    public void testMaxVarcharDouble()
    {
        InternalAggregationFunction function = getMaxByAggregation(DOUBLE, VARCHAR, BIGINT);
        assertAggregation(
                function,
                ImmutableList.of(1.0, 2.0),
                createDoublesBlock(1.0, 2.0, null),
                createStringsBlock("z", "a", null),
                createRLEBlock(2L, 3));

        assertAggregation(
                function,
                ImmutableList.of(0.0, 1.0),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0),
                createStringsBlock("zz", "hi", "bb", "a"),
                createRLEBlock(2L, 4));

        assertAggregation(
                function,
                ImmutableList.of(0.0, 1.0),
                createDoublesBlock(0.0, 1.0, null, -1.0),
                createStringsBlock("zz", "hi", null, "a"),
                createRLEBlock(2L, 4));
    }

    @Test
    public void testMinVarcharArray()
    {
        InternalAggregationFunction function = getMinByAggregation(new ArrayType(BIGINT), VARCHAR, BIGINT);
        assertAggregation(
                function,
                ImmutableList.of(ImmutableList.of(2L, 3L), ImmutableList.of(4L, 5L)),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L), ImmutableList.of(3L, 4L), ImmutableList.of(4L, 5L))),
                createStringsBlock("z", "a", "x", "b"),
                createRLEBlock(2L, 4));
    }

    @Test
    public void testMaxVarcharArray()
    {
        InternalAggregationFunction function = getMaxByAggregation(new ArrayType(BIGINT), VARCHAR, BIGINT);
        assertAggregation(
                function,
                ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(3L, 4L)),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L), ImmutableList.of(3L, 4L), ImmutableList.of(4L, 5L))),
                createStringsBlock("z", "a", "x", "b"),
                createRLEBlock(2L, 4));
    }

    @Test
    public void testMinArrayVarchar()
    {
        InternalAggregationFunction function = getMinByAggregation(VARCHAR, new ArrayType(BIGINT), BIGINT);
        assertAggregation(
                function,
                ImmutableList.of("b", "x", "z"),
                createStringsBlock("z", "a", "x", "b"),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L), ImmutableList.of(0L, 3L), ImmutableList.of(0L, 2L))),
                createRLEBlock(3L, 4));
    }

    @Test
    public void testMaxArrayVarchar()
    {
        InternalAggregationFunction function = getMaxByAggregation(VARCHAR, new ArrayType(BIGINT), BIGINT);
        assertAggregation(
                function,
                ImmutableList.of("a", "z", "x"),
                createStringsBlock("z", "a", "x", "b"),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L), ImmutableList.of(0L, 3L), ImmutableList.of(0L, 2L))),
                createRLEBlock(3L, 4));
    }

    @Test
    public void testOutOfBound()
    {
        InternalAggregationFunction function = getMaxByAggregation(VARCHAR, BIGINT, BIGINT);
        try {
            groupedAggregation(function, new Page(createStringsBlock("z"), createLongsBlock(0), createLongsBlock(10001)));
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), "third argument of max_by/min_by must be less than or equal to 10000; found 10001");
        }
    }

    private InternalAggregationFunction getMaxByAggregation(Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(
                FUNCTION_AND_TYPE_MANAGER.lookupFunction("max_by", fromTypes(arguments)));
    }

    private InternalAggregationFunction getMinByAggregation(Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(
                FUNCTION_AND_TYPE_MANAGER.lookupFunction("min_by", fromTypes(arguments)));
    }
}
