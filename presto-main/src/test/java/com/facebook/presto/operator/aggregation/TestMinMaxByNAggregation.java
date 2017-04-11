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
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public class TestMinMaxByNAggregation
{
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testMaxDoubleDouble()
            throws Exception
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("max_by",
                        AGGREGATE,
                        parseTypeSignature("array(double)"),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.BIGINT)));
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
            throws Exception
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("min_by",
                        AGGREGATE,
                        parseTypeSignature("array(double)"),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.BIGINT)));
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
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("min_by",
                        AGGREGATE,
                        parseTypeSignature("array(varchar)"),
                        parseTypeSignature(StandardTypes.VARCHAR),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.BIGINT)));
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
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("max_by",
                        AGGREGATE,
                        parseTypeSignature("array(varchar)"),
                        parseTypeSignature(StandardTypes.VARCHAR),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.BIGINT)));
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
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("min_by",
                        AGGREGATE,
                        parseTypeSignature("array(double)"),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.VARCHAR),
                        parseTypeSignature(StandardTypes.BIGINT)));
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
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("max_by",
                        AGGREGATE,
                        parseTypeSignature("array(double)"),
                        parseTypeSignature(StandardTypes.DOUBLE),
                        parseTypeSignature(StandardTypes.VARCHAR),
                        parseTypeSignature(StandardTypes.BIGINT)));
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
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("min_by",
                        AGGREGATE,
                        parseTypeSignature("array(array(bigint))"),
                        parseTypeSignature("array(bigint)"),
                        parseTypeSignature(StandardTypes.VARCHAR),
                        parseTypeSignature(StandardTypes.BIGINT)));
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
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("max_by",
                        AGGREGATE,
                        parseTypeSignature("array(array(bigint))"),
                        parseTypeSignature("array(bigint)"),
                        parseTypeSignature(StandardTypes.VARCHAR),
                        parseTypeSignature(StandardTypes.BIGINT)));
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
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("min_by",
                        AGGREGATE,
                        parseTypeSignature("array(varchar)"),
                        parseTypeSignature(StandardTypes.VARCHAR),
                        parseTypeSignature("array(bigint)"),
                        parseTypeSignature(StandardTypes.BIGINT)));
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
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("max_by",
                        AGGREGATE,
                        parseTypeSignature("array(varchar)"),
                        parseTypeSignature(StandardTypes.VARCHAR),
                        parseTypeSignature("array(bigint)"),
                        parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                function,
                ImmutableList.of("a", "z", "x"),
                createStringsBlock("z", "a", "x", "b"),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L), ImmutableList.of(0L, 3L), ImmutableList.of(0L, 2L))),
                createRLEBlock(3L, 4));
    }
}
