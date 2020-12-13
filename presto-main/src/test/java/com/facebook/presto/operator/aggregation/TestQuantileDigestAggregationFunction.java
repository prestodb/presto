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

import com.facebook.airlift.stats.QuantileDigest;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createSequenceBlockOfReal;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.StandardTypes.QDIGEST;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.doubleToSortableLong;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static com.facebook.presto.operator.aggregation.TestMergeQuantileDigestFunction.QDIGEST_EQUALITY;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;

public class TestQuantileDigestAggregationFunction
        extends TestStatisticalDigestAggregationFunction
{
    private static final double STANDARD_ERROR = 0.01;

    protected double getParameter()
    {
        return STANDARD_ERROR;
    }

    @Test
    public void testRealsWithWeights()
    {
        testAggregationReal(
                createBlockOfReals(1.0F, null, 2.0F, null, 3.0F, null, 4.0F, null, 5.0F, null),
                createRLEBlock(1, 10),
                0.01, 1.0F, 2.0F, 3.0F, 4.0F, 5.0F);
        testAggregationReal(
                createBlockOfReals(null, null, null, null, null),
                createRLEBlock(1, 5),
                NaN);
        testAggregationReal(
                createBlockOfReals(-1.0F, -2.0F, -3.0F, -4.0F, -5.0F, -6.0F, -7.0F, -8.0F, -9.0F, -10.0F),
                createRLEBlock(1, 10),
                0.01, -1.0F, -2.0F, -3.0F, -4.0F, -5.0F, -6.0F, -7.0F, -8.0F, -9.0F, -10.0F);
        testAggregationReal(
                createBlockOfReals(1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F),
                createRLEBlock(1, 10),
                0.01, 1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F, 10.0F);
        testAggregationReal(
                createBlockOfReals(),
                createRLEBlock(1, 0),
                NaN);
        testAggregationReal(
                createBlockOfReals(1.0F),
                createRLEBlock(1, 1),
                0.01, 1.0F);
        testAggregationReal(
                createSequenceBlockOfReal(-1000, 1000),
                createRLEBlock(1, 2000),
                0.01,
                Floats.toArray(LongStream.range(-1000, 1000).mapToObj(Float::new).collect(toImmutableList())));
    }

    @Test
    public void testBigintsWithWeight()
    {
        testAggregationBigint(
                createLongsBlock(1L, null, 2L, null, 3L, null, 4L, null, 5L, null),
                createRLEBlock(1, 10),
                0.01, 1, 2, 3, 4, 5);
        testAggregationBigint(
                createLongsBlock(null, null, null, null, null),
                createRLEBlock(1, 5),
                NaN);
        testAggregationBigint(
                createLongsBlock(-1, -2, -3, -4, -5, -6, -7, -8, -9, -10),
                createRLEBlock(1, 10),
                0.01, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10);
        testAggregationBigint(
                createLongsBlock(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                createRLEBlock(1, 10),
                0.01, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        testAggregationBigint(
                createLongsBlock(new int[] {}),
                createRLEBlock(1, 0),
                NaN);
        testAggregationBigint(
                createLongsBlock(1),
                createRLEBlock(1, 1),
                0.01, 1);
        testAggregationBigint(
                createLongSequenceBlock(-1000, 1000),
                createRLEBlock(1, 2000),
                0.01,
                LongStream.range(-1000, 1000).toArray());
    }

    @Override
    protected InternalAggregationFunction getAggregationFunction(Type... type)
    {
        FunctionAndTypeManager functionAndTypeManager = METADATA.getFunctionAndTypeManager();
        return functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("qdigest_agg", fromTypes(type)));
    }

    private void testAggregationBigint(Block inputBlock, Block weightsBlock, double maxError, long... inputs)
    {
        // Test without weights and accuracy
        testAggregationBigints(
                getAggregationFunction(BIGINT),
                new Page(inputBlock),
                maxError,
                inputs);

        // Test with weights and without accuracy
        testAggregationBigints(
                getAggregationFunction(BIGINT, BIGINT),
                new Page(inputBlock, weightsBlock),
                maxError,
                inputs);
        // Test with weights and accuracy
        testAggregationBigints(
                getAggregationFunction(BIGINT, BIGINT, DOUBLE),
                new Page(inputBlock, weightsBlock, createRLEBlock(maxError, inputBlock.getPositionCount())),
                maxError,
                inputs);
    }

    private void testAggregationReal(Block longsBlock, Block weightsBlock, double maxError, float... inputs)
    {
        // Test without weights and accuracy
        testAggregationReal(
                getAggregationFunction(REAL),
                new Page(longsBlock),
                maxError,
                inputs);
        // Test with weights and without accuracy
        testAggregationReal(
                getAggregationFunction(REAL, BIGINT),
                new Page(longsBlock, weightsBlock),
                maxError,
                inputs);
        // Test with weights and accuracy
        testAggregationReal(
                getAggregationFunction(REAL, BIGINT, DOUBLE),
                new Page(longsBlock, weightsBlock, createRLEBlock(maxError, longsBlock.getPositionCount())),
                maxError,
                inputs);
    }

    private void testAggregationBigints(InternalAggregationFunction function, Page page, double maxError, long... inputs)
    {
        // aggregate level
        assertAggregation(function,
                QDIGEST_EQUALITY,
                "test multiple positions",
                page,
                getExpectedValueLongs(maxError, inputs));

        // test scalars
        List<Long> rows = Arrays.stream(inputs).sorted().boxed().collect(Collectors.toList());

        SqlVarbinary returned = (SqlVarbinary) AggregationTestUtils.aggregation(function, page);
        assertPercentileWithinError(QDIGEST, StandardTypes.BIGINT, returned, maxError, rows, 0.1, 0.5, 0.9, 0.99);
    }

    private void testAggregationReal(InternalAggregationFunction function, Page page, double maxError, float... inputs)
    {
        assertAggregation(function,
                QDIGEST_EQUALITY,
                "test multiple positions",
                page,
                getExpectedValuesFloats(maxError, inputs));

        // test scalars
        List<Double> rows = Floats.asList(inputs).stream().sorted().map(Float::doubleValue).collect(Collectors.toList());

        SqlVarbinary returned = (SqlVarbinary) AggregationTestUtils.aggregation(function, page);
        assertPercentileWithinError(QDIGEST, StandardTypes.REAL, returned, maxError, rows, 0.1, 0.5, 0.9, 0.99);
    }

    @Override
    protected void testAggregationDoubles(InternalAggregationFunction function, Page page, double maxError, double... inputs)
    {
        assertAggregation(function,
                QDIGEST_EQUALITY,
                "test multiple positions",
                page,
                getExpectedValueDoubles(maxError, inputs));

        // test scalars
        List<Double> rows = Doubles.asList(inputs).stream().sorted().collect(Collectors.toList());

        SqlVarbinary returned = (SqlVarbinary) AggregationTestUtils.aggregation(function, page);
        assertPercentileWithinError(QDIGEST, StandardTypes.DOUBLE, returned, maxError, rows, 0.1, 0.5, 0.9, 0.99);
    }

    private Object getExpectedValueLongs(double maxError, long... values)
    {
        if (values.length == 0) {
            return null;
        }
        QuantileDigest qdigest = new QuantileDigest(maxError);
        Arrays.stream(values).forEach(qdigest::add);
        return new SqlVarbinary(qdigest.serialize().getBytes());
    }

    @Override
    protected Object getExpectedValueDoubles(double maxError, double... values)
    {
        if (values.length == 0) {
            return null;
        }
        QuantileDigest qdigest = new QuantileDigest(maxError);
        Arrays.stream(values).forEach(value -> qdigest.add(doubleToSortableLong(value)));
        return new SqlVarbinary(qdigest.serialize().getBytes());
    }

    private Object getExpectedValuesFloats(double maxError, float... values)
    {
        if (values.length == 0) {
            return null;
        }
        QuantileDigest qdigest = new QuantileDigest(maxError);
        Floats.asList(values).forEach(value -> qdigest.add(floatToSortableInt(value)));
        return new SqlVarbinary(qdigest.serialize().getBytes());
    }
}
