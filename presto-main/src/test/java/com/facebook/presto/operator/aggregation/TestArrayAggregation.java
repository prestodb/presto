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
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.block.BlockAssertions.createTypedLongsBlock;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public class TestArrayAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testEmpty()
            throws Exception
    {
        InternalAggregationFunction bigIntAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("array_agg", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                bigIntAgg,
                null,
                createLongsBlock(new Long[] {}));
    }

    @Test
    public void testNullOnly()
            throws Exception
    {
        InternalAggregationFunction bigIntAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("array_agg", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                bigIntAgg,
                Arrays.asList(null, null, null),
                createLongsBlock(new Long[] {null, null, null}));
    }

    @Test
    public void testNullPartial()
            throws Exception
    {
        InternalAggregationFunction bigIntAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("array_agg", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                bigIntAgg,
                Arrays.asList(null, 2L, null, 3L, null),
                createLongsBlock(new Long[] {null, 2L, null, 3L, null}));
    }

    @Test
    public void testBoolean()
        throws Exception
    {
        InternalAggregationFunction booleanAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("array_agg", AGGREGATE, parseTypeSignature("array(boolean)"), parseTypeSignature(StandardTypes.BOOLEAN)));
        assertAggregation(
                booleanAgg,
                Arrays.asList(true, false),
                createBooleansBlock(new Boolean[] {true, false}));
    }

    @Test
    public void testBigInt()
        throws Exception
    {
        InternalAggregationFunction bigIntAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("array_agg", AGGREGATE, parseTypeSignature("array(bigint)"), parseTypeSignature(StandardTypes.BIGINT)));
        assertAggregation(
                bigIntAgg,
                Arrays.asList(2L, 1L, 2L),
                createLongsBlock(new Long[] {2L, 1L, 2L}));
    }

    @Test
    public void testVarchar()
        throws Exception
    {
        InternalAggregationFunction varcharAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("array_agg", AGGREGATE, parseTypeSignature("array(varchar)"), parseTypeSignature(StandardTypes.VARCHAR)));
        assertAggregation(
                varcharAgg,
                Arrays.asList("hello", "world"),
                createStringsBlock(new String[] {"hello", "world"}));
    }

    @Test
    public void testDate()
            throws Exception
    {
        InternalAggregationFunction varcharAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("array_agg", AGGREGATE, parseTypeSignature("array(date)"), parseTypeSignature(StandardTypes.DATE)));
        assertAggregation(
                varcharAgg,
                Arrays.asList(new SqlDate(1), new SqlDate(2), new SqlDate(4)),
                createTypedLongsBlock(DATE, ImmutableList.of(1L, 2L, 4L)));
    }

    @Test
    public void testArray()
            throws Exception
    {
        InternalAggregationFunction varcharAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("array_agg", AGGREGATE, parseTypeSignature("array(array(bigint))"), parseTypeSignature("array(bigint)")));

        assertAggregation(
                varcharAgg,
                Arrays.asList(Arrays.asList(1L), Arrays.asList(1L, 2L), Arrays.asList(1L, 2L, 3L)),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(1L, 2L), ImmutableList.of(1L, 2L, 3L))));
    }
}
