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

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.facebook.presto.block.BlockAssertions.createDoubleSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.Integer.min;
import static org.testng.Assert.assertTrue;

public abstract class TestStatisticalDigestApproximatePercentileAggregation
        extends AbstractTestFunctions
{
    private static final List<Double> quantiles = ImmutableList.of(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.95, 0.99, 0.995, 0.999, 0.9995, 0.9999);
    private static final double STANDARD_ERROR = 0.01;
    private static final double STANDARD_COMPRESSION_FACTOR = 100;
    private final double parameter;

    protected TestStatisticalDigestApproximatePercentileAggregation(FeaturesConfig featuresConfig)
    {
        super(featuresConfig);

        switch (config.getStatisticalDigestImplementation()) {
            case TDIGEST:
                parameter = STANDARD_COMPRESSION_FACTOR;
                break;
            case QDIGEST:
                parameter = STANDARD_ERROR;
                break;
            default:
                throw new IllegalArgumentException("Expected a statistical digest");
        }
    }

    @DataProvider(name = "distributions")
    public static Object[][] getDistributions()
    {
        return new Object[][] {
                new Object[] {new NormalDistribution(200_000, 7500)},
                new Object[] {new UniformRealDistribution(-5000, 5000)},
                new Object[] {new ChiSquaredDistribution(4)}};
    }

    /*
    Tests aggregations for normal and uniform distributions with 1_000_000 elements
    Disabled because it takes around 15-30s per distribution to run
     */
    @Test(dataProvider = "distributions", enabled = false)
    public void testDoublePartialStep(AbstractRealDistribution distribution)
    {
        final double[] inputs = getDistributionValues(distribution, 1_000_000);
        final Double[] objectInputs = new Double[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            objectInputs[i] = Double.valueOf(inputs[i]);
        }

        for (double quantile : quantiles) {
            testAggregationDouble(
                    createDoublesBlock(objectInputs),
                    createLongRepeatBlock(1, 1_000_000),
                    createRLEBlock(quantile, 1_000_000),
                    STANDARD_ERROR,
                    quantile,
                    parameter,
                    inputs);
        }
    }

    @Test
    public void testDoublePartialStep()
    {
        // regular approx_percentile
        testAggregationDouble(
                createDoublesBlock(null, null),
                BlockAssertions.createRLEBlock(1, 2),
                createRLEBlock(0.5, 2),
                STANDARD_ERROR,
                0.5,
                parameter);

        testAggregationDouble(
                createDoublesBlock(null, 1.0),
                BlockAssertions.createRLEBlock(1, 2),
                createRLEBlock(0.2, 2),
                STANDARD_ERROR,
                0.2,
                parameter,
                1.0);

        testAggregationDouble(
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                BlockAssertions.createRLEBlock(1, 4),
                createRLEBlock(0.1, 4),
                STANDARD_ERROR,
                0.1,
                parameter,
                1.0, 2.0, 3.0);

        testAggregationDouble(
                createDoublesBlock(1.0, 2.0, 3.0),
                BlockAssertions.createRLEBlock(1, 3),
                createRLEBlock(0.7, 3),
                STANDARD_ERROR,
                0.7,
                parameter,
                1.0, 2.0, 3.0);

        testAggregationDouble(
                createDoublesBlock(1.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 3.0, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                BlockAssertions.createRLEBlock(1, 21),
                createRLEBlock(0.9, 21),
                STANDARD_ERROR,
                0.9,
                parameter,
                1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0, 4.0, 5.0, 6.0, 7.0);

        // array of approx_percentile
        testArrayAggregationDouble(
                createDoublesBlock(null, null),
                BlockAssertions.createRLEBlock(1, 2),
                createRLEBlock(quantiles, 2),
                STANDARD_ERROR,
                quantiles);

        testArrayAggregationDouble(
                createDoublesBlock(null, 1.0),
                BlockAssertions.createRLEBlock(1, 2),
                createRLEBlock(quantiles, 2),
                STANDARD_ERROR,
                quantiles,
                1.0);

        testArrayAggregationDouble(
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                BlockAssertions.createRLEBlock(1, 4),
                createRLEBlock(quantiles, 4),
                STANDARD_ERROR,
                quantiles,
                1.0, 2.0, 3.0);

        testArrayAggregationDouble(
                createDoublesBlock(1.0, 2.0, 3.0),
                BlockAssertions.createRLEBlock(1, 3),
                createRLEBlock(quantiles, 3),
                STANDARD_ERROR,
                quantiles,
                1.0, 2.0, 3.0);

        testArrayAggregationDouble(
                createDoublesBlock(1.0, null, 2.0, null, 2.0, null, 2.0, null, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0, 7.0),
                BlockAssertions.createRLEBlock(1, 18),
                createRLEBlock(quantiles, 18),
                STANDARD_ERROR,
                quantiles,
                1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0, 5.0, 6.0, 7.0, 7.0);

        testArrayAggregationDouble(
                createDoubleSequenceBlock(0, 10000),
                createLongRepeatBlock(1, 10000),
                createRLEBlock(quantiles, 10000),
                STANDARD_ERROR,
                quantiles,
                LongStream.range(0, 10000).asDoubleStream().toArray());
    }

    private static double[] getDistributionValues(AbstractRealDistribution distribution, int size)
    {
        double[] values = new double[size];
        for (int i = 0; i < size; i++) {
            values[i] = distribution.sample();
        }
        return values;
    }

    private void testAggregationDouble(Block doublesBlock, Block weightsBlock, Block percentiles, double error, double quantile, double parameter, double... inputs)
    {
        // Test without weights and accuracy
        testAggregationDoubles(
                getInternalAggregationFunction(2),
                new Page(doublesBlock, percentiles),
                error,
                ImmutableList.of(quantile),
                inputs);
        // Test with weights and without accuracy
        testAggregationDoubles(
                getInternalAggregationFunction(3),
                new Page(doublesBlock, weightsBlock, percentiles),
                error,
                ImmutableList.of(quantile),
                inputs);
        // Test with weights and accuracy
        testAggregationDoubles(
                getInternalAggregationFunction(4),
                new Page(doublesBlock, weightsBlock, percentiles, BlockAssertions.createRLEBlock(parameter, doublesBlock.getPositionCount())),
                error,
                ImmutableList.of(quantile),
                inputs);
    }

    private void testArrayAggregationDouble(Block doublesBlock, Block weightsBlock, Block percentiles, double error, List<Double> quantiles, double... inputs)
    {
        // Test without weights and without accuracy/compression
        testAggregationDoubles(
                getArrayInternalAggregationFunction(2),
                new Page(doublesBlock, percentiles),
                error,
                quantiles,
                inputs);
        // Test with weights and without accuracy/compression
        testAggregationDoubles(
                getArrayInternalAggregationFunction(3),
                new Page(doublesBlock, weightsBlock, percentiles),
                error,
                quantiles,
                inputs);
    }

    abstract InternalAggregationFunction getInternalAggregationFunction(int arity);

    abstract InternalAggregationFunction getArrayInternalAggregationFunction(int arity);

    private void testAggregationDoubles(InternalAggregationFunction function, Page page, double error, List<Double> quantiles, double... inputs)
    {
        // test scalars
        List<Double> rows = Arrays.stream(inputs).sorted().boxed().collect(Collectors.toList());
        Object aggregation = AggregationTestUtils.aggregation(function, page);

        final List<Double> returned;
        if (aggregation == null) {
            returned = new ArrayList<>();
        }
        else {
            if (aggregation instanceof Double) {
                returned = ImmutableList.of((Double) aggregation);
            }
            else if (aggregation instanceof List) {
                returned = (List<Double>) aggregation;
            }
            else {
                throw new IllegalArgumentException("Aggregation object is invalid");
            }
        }
        assertPercentilesWithinError(returned, error, quantiles, rows);
    }

    private void assertPercentilesWithinError(List<Double> result, double error, List<Double> quantiles, List<? extends Number> rows)
    {
        if (rows.isEmpty()) {
            // Nothing to assert except that the digest is empty
            return;
        }

        // Test each quantile individually (value_at_quantile)
        for (int i = 0; i < result.size(); i++) {
            assertPercentileWithinError(result.get(i), error, quantiles.get(i), rows);
        }
    }

    private void assertPercentileWithinError(Double result, double error, double percentile, List<? extends Number> rows)
    {
        double lowerBound = Math.max(0, percentile - error);
        double upperBound = Math.min(1, percentile + error);

        double min = rows.get((int) (rows.size() * lowerBound)).doubleValue();
        double max = rows.get(min(rows.size() - 1, (int) (rows.size() * upperBound))).doubleValue();
        // Check that the chosen quantile is within the upper and lower bound of the error
        assertTrue(result >= min);
        assertTrue(result <= max);
    }

    protected static InternalAggregationFunction getAggregation(FunctionManager functionManager, Type... arguments)
    {
        return functionManager.getAggregateFunctionImplementation(functionManager.lookupFunction("approx_percentile", fromTypes(arguments)));
    }

    protected static RunLengthEncodedBlock createRLEBlock(double percentile, int positionCount)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 1);
        DOUBLE.writeDouble(blockBuilder, percentile);
        return new RunLengthEncodedBlock(blockBuilder.build(), positionCount);
    }

    protected static RunLengthEncodedBlock createRLEBlock(Iterable<Double> percentiles, int positionCount)
    {
        BlockBuilder rleBlockBuilder = new ArrayType(DOUBLE).createBlockBuilder(null, 1);
        BlockBuilder arrayBlockBuilder = rleBlockBuilder.beginBlockEntry();

        for (double percentile : percentiles) {
            DOUBLE.writeDouble(arrayBlockBuilder, percentile);
        }

        rleBlockBuilder.closeEntry();

        return new RunLengthEncodedBlock(rleBlockBuilder.build(), positionCount);
    }
}
