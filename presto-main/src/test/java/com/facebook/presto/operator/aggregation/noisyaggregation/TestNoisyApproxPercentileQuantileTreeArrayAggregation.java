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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.TestingSeededRandomizationStrategy;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;

public class TestNoisyApproxPercentileQuantileTreeArrayAggregation
        extends AbstractTestQuantileTreeAggregation
{
    @Test
    public void testPrivateMedian()
    {
        JavaAggregationFunctionImplementation percentileAgg = getFunction("noisy_approx_percentile_qtree", DOUBLE, new ArrayType(DOUBLE), DOUBLE, DOUBLE, DOUBLE, DOUBLE);

        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        List<Double> probabilities = ImmutableList.of(0.5);
        double epsilon = 5;
        double delta = 1e-6;
        double lower = 50;
        double upper = 150;
        List<Double> expected = ImmutableList.of(100.0);

        assertAggregation(
                percentileAgg,
                getComparator(15), // since this test is random, we allow 15 standard deviations in error
                "private median is within 15 SD of expectation",
                getPage(numbers, probabilities, epsilon, delta, lower, upper),
                expected);
    }

    /**
     * T181507692
     */
    @Test(enabled = false)
    public void testPrivateMultipleQuantiles()
    {
        JavaAggregationFunctionImplementation percentileAgg = getFunction("noisy_approx_percentile_qtree", DOUBLE, new ArrayType(DOUBLE), DOUBLE, DOUBLE, DOUBLE, DOUBLE);

        Double[] numbers = generateNormal(10000, 100, 1, new TestingSeededRandomizationStrategy(1));
        List<Double> probabilities = ImmutableList.of(0.25, 0.5, 0.75);
        double epsilon = 5;
        double delta = 1e-6;
        double lower = 50;
        double upper = 150;
        List<Double> expected = ImmutableList.of(99.3255, 100.0, 100.6745);

        assertAggregation(
                percentileAgg,
                getComparator(15), // since this test is random, we allow 15 standard deviations in error
                "private multiple quantiles are within 15 SD of expectation",
                getPage(numbers, probabilities, epsilon, delta, lower, upper),
                expected);
    }

    @Test
    public void testNonPrivateMedian()
    {
        JavaAggregationFunctionImplementation percentileAgg = getFunction("noisy_approx_percentile_qtree", DOUBLE, new ArrayType(DOUBLE), DOUBLE, DOUBLE, DOUBLE, DOUBLE);

        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        List<Double> probabilities = ImmutableList.of(0.5);
        double epsilon = Double.POSITIVE_INFINITY;
        double delta = 1;
        double lower = 50;
        double upper = 150;
        List<Double> expected = ImmutableList.of(100.0);

        assertAggregation(
                percentileAgg,
                getComparator(0.1), // this test is seeded, so it should always return the same value
                "non-private median is accurate to within 0.1 SD",
                getPage(numbers, probabilities, epsilon, delta, lower, upper),
                expected);
    }

    @Test
    public void testNonPrivateFirstQuartile()
    {
        JavaAggregationFunctionImplementation percentileAgg = getFunction("noisy_approx_percentile_qtree", DOUBLE, new ArrayType(DOUBLE), DOUBLE, DOUBLE, DOUBLE, DOUBLE);

        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        List<Double> probabilities = ImmutableList.of(0.25);
        double epsilon = Double.POSITIVE_INFINITY;
        double delta = 1;
        double lower = 50;
        double upper = 150;
        List<Double> expected = ImmutableList.of(100.0 - 0.6745);

        assertAggregation(
                percentileAgg,
                getComparator(0.1), // this test is seeded, so it should always return the same value
                "non-private 0.25-th quantile is accurate to within 0.1 SD",
                getPage(numbers, probabilities, epsilon, delta, lower, upper),
                expected);
    }

    private static Page getPage(Double[] values, List<Double> probabilities, double epsilon, double delta, double lower, double upper)
    {
        return new Page(
                createDoublesBlock(values),
                createRLEProbabilityBlock(probabilities, values.length),
                createRLEBlock(epsilon, values.length),
                createRLEBlock(delta, values.length),
                createRLEBlock(lower, values.length),
                createRLEBlock(upper, values.length));
    }

    public static BiFunction<Object, Object, Boolean> getComparator(double tolerance)
    {
        return (x, y) -> {
            List<Double> xArray = Collections.unmodifiableList((List<? extends Double>) x);
            List<Double> yArray = Collections.unmodifiableList((List<? extends Double>) y);
            for (int i = 0; i < xArray.size(); i++) {
                double difference = Math.abs(xArray.get(i) - yArray.get(i));
                if (difference > tolerance) {
                    return false;
                }
            }
            return true;
        };
    }

    public static RunLengthEncodedBlock createRLEProbabilityBlock(Iterable<Double> values, int positionCount)
    {
        BlockBuilder rleBlockBuilder = new ArrayType(DOUBLE).createBlockBuilder(null, 1);
        BlockBuilder arrayBlockBuilder = rleBlockBuilder.beginBlockEntry();
        for (double value : values) {
            DOUBLE.writeDouble(arrayBlockBuilder, value);
        }
        rleBlockBuilder.closeEntry();

        return new RunLengthEncodedBlock(rleBlockBuilder.build(), positionCount);
    }
}
