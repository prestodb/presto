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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.google.common.base.Joiner;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;

import static com.facebook.presto.block.BlockAssertions.createDoubleSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.lang.String.format;

public abstract class TestStatisticalDigestAggregationFunction
        extends AbstractTestFunctions
{
    static final Joiner ARRAY_JOINER = Joiner.on(",");
    protected static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testDoublesWithWeights()
    {
        testAggregationDouble(
                createDoublesBlock(1.0, null, 2.0, null, 3.0, null, 4.0, null, 5.0, null),
                createRLEBlock(1, 10),
                getParameter(),
                1.0, 2.0, 3.0, 4.0, 5.0);
        testAggregationDouble(
                createDoublesBlock(null, null, null, null, null),
                createRLEBlock(1, 5),
                NaN);
        testAggregationDouble(
                createDoublesBlock(-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0),
                createRLEBlock(1, 10),
                getParameter(),
                -1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0);
        testAggregationDouble(
                createDoublesBlock(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0),
                createRLEBlock(1, 10),
                getParameter(),
                1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        testAggregationDouble(
                createDoublesBlock(),
                createRLEBlock(1, 0),
                NaN);
        testAggregationDouble(
                createDoublesBlock(1.0),
                createRLEBlock(1, 1),
                getParameter(),
                1.0);
        testAggregationDouble(
                createDoubleSequenceBlock(-1000, 1000),
                createRLEBlock(1, 2000),
                getParameter(),
                LongStream.range(-1000, 1000).asDoubleStream().toArray());
    }

    protected abstract InternalAggregationFunction getAggregationFunction(Type... type);

    private void testAggregationDouble(Block longsBlock, Block weightsBlock, double parameter, double... inputs)
    {
        // Test without weights and accuracy
        testAggregationDoubles(
                getAggregationFunction(DOUBLE),
                new Page(longsBlock),
                parameter,
                inputs);
        // Test with weights and without accuracy
        testAggregationDoubles(
                getAggregationFunction(DOUBLE, BIGINT),
                new Page(longsBlock, weightsBlock),
                parameter,
                inputs);
        // Test with weights and accuracy
        testAggregationDoubles(
                getAggregationFunction(DOUBLE, BIGINT, DOUBLE),
                new Page(longsBlock, weightsBlock, createRLEBlock(parameter, longsBlock.getPositionCount())),
                parameter,
                inputs);
    }

    abstract double getParameter();

    abstract void testAggregationDoubles(InternalAggregationFunction function, Page page, double maxError, double... inputs);

    abstract Object getExpectedValueDoubles(double maxError, double... values);

    void assertPercentileWithinError(String method, String type, SqlVarbinary binary, double error, List<? extends Number> rows, double... percentiles)
    {
        if (rows.isEmpty()) {
            // Nothing to assert except that the digest is empty
            return;
        }

        // Test each quantile individually (value_at_quantile)
        for (double percentile : percentiles) {
            assertPercentileWithinError(method, type, binary, error, rows, percentile);
        }

        // Test all the quantiles (values_at_quantiles)
        assertPercentilesWithinError(method, type, binary, error, rows, percentiles);
    }

    private void assertPercentileWithinError(String method, String type, SqlVarbinary binary, double error, List<? extends Number> rows, double percentile)
    {
        Number lowerBound = getLowerBoundValue(error, rows, percentile);
        Number upperBound = getUpperBoundValue(error, rows, percentile);

        // Check that the chosen quantile is within the upper and lower bound of the error
        functionAssertions.assertFunction(
                format("value_at_quantile(CAST(X'%s' AS %s(%s)), %s) >= %s", binary.toString().replaceAll("\\s+", " "), method,
                        type, percentile, lowerBound),
                BOOLEAN,
                true);
        functionAssertions.assertFunction(
                format("value_at_quantile(CAST(X'%s' AS %s(%s)), %s) <= %s", binary.toString().replaceAll("\\s+", " "),
                       method, type, percentile, upperBound),
                BOOLEAN,
                true);
    }

    private void assertPercentilesWithinError(String method, String type, SqlVarbinary binary, double error, List<? extends Number> rows, double[] percentiles)
    {
        List<Double> boxedPercentiles = Arrays.stream(percentiles).sorted().boxed().collect(toImmutableList());
        List<Number> lowerBounds = boxedPercentiles.stream().map(percentile -> getLowerBoundValue(error, rows, percentile)).collect(toImmutableList());
        List<Number> upperBounds = boxedPercentiles.stream().map(percentile -> getUpperBoundValue(error, rows, percentile)).collect(toImmutableList());

        // Ensure that the lower bound of each item in the distribution is not greater than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(values_at_quantiles(CAST(X'%s' AS %s(%s)), ARRAY[%s]), ARRAY[%s], (value, lowerbound) -> value >= lowerbound)",
                        binary.toString().replaceAll("\\s+", " "),
                        method,
                        type,
                        ARRAY_JOINER.join(boxedPercentiles),
                        ARRAY_JOINER.join(lowerBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(percentiles.length, true));

        // Ensure that the upper bound of each item in the distribution is not less than the chosen quantiles
        functionAssertions.assertFunction(
                format(
                        "zip_with(values_at_quantiles(CAST(X'%s' AS %s(%s)), ARRAY[%s]), ARRAY[%s], (value, upperbound) -> value <= upperbound)",
                        binary.toString().replaceAll("\\s+", " "),
                        method,
                        type,
                        ARRAY_JOINER.join(boxedPercentiles),
                        ARRAY_JOINER.join(upperBounds)),
                METADATA.getType(parseTypeSignature("array(boolean)")),
                Collections.nCopies(percentiles.length, true));
    }

    private Number getLowerBoundValue(double error, List<? extends Number> rows, double percentile)
    {
        int medianIndex = (int) (rows.size() * percentile);
        int marginOfError = (int) (rows.size() * error / 2);
        return rows.get(max(medianIndex - marginOfError, 0));
    }

    private Number getUpperBoundValue(double error, List<? extends Number> rows, double percentile)
    {
        int medianIndex = (int) (rows.size() * percentile);
        int marginOfError = (int) (rows.size() * error / 2);
        return rows.get(min(medianIndex + marginOfError, rows.size() - 1));
    }
}
