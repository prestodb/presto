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
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.TestingSeededRandomizationStrategy;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_BIN_COUNT;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_BRANCHING_FACTOR;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_SKETCH_DEPTH;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_SKETCH_WIDTH;

public class TestNoisyQuantileTreeAggregation
        extends AbstractTestQuantileTreeAggregation
{
    @Test
    public void testNonPrivate()
    {
        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        double epsilon = Double.POSITIVE_INFINITY;
        double delta = 1;
        double lower = 50;
        double upper = 150;
        int binCount = 16384; // 2^14 = 4^7
        int branchingFactor = 2;
        int sketchDepth = 5;
        int sketchWidth = 500;

        assertAggregation(
                getAggFunction(),
                getQuantileComparator(0), // without privacy, the quantiles should match exactly
                "non-private tree matches quantiles",
                getPage(numbers, epsilon, delta, lower, upper, binCount, branchingFactor, sketchDepth, sketchWidth),
                buildReferenceTree(numbers, lower, upper, binCount, branchingFactor, sketchDepth, sketchWidth));
    }

    @Test
    public void testNonPrivateDefaultSketch()
    {
        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        double epsilon = Double.POSITIVE_INFINITY;
        double delta = 1;
        double lower = 50;
        double upper = 150;
        int binCount = 16384; // 2^14 = 4^7
        int branchingFactor = 2;

        assertAggregation(
                getAggFunctionDefaultSketch(),
                getQuantileComparator(0), // without privacy, the quantiles should match exactly
                "non-private tree matches quantiles",
                getPage(numbers, epsilon, delta, lower, upper, binCount, branchingFactor),
                buildReferenceTree(numbers, lower, upper, binCount, branchingFactor));
    }

    @Test
    public void testNonPrivateDefaultSketchBranchingFactor()
    {
        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        double epsilon = Double.POSITIVE_INFINITY;
        double delta = 1;
        double lower = 50;
        double upper = 150;
        int binCount = 16384; // 2^14

        assertAggregation(
                getAggFunctionDefaultSketchBranchingFactor(),
                getQuantileComparator(0), // without privacy, the quantiles should match exactly
                "non-private tree matches quantiles",
                getPage(numbers, epsilon, delta, lower, upper, binCount),
                buildReferenceTree(numbers, lower, upper, binCount));
    }

    @Test
    public void testNonPrivateDefaultSketchBranchingFactorBinCount()
    {
        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        double epsilon = Double.POSITIVE_INFINITY;
        double delta = 1;
        double lower = 50;
        double upper = 150;

        assertAggregation(
                getAggFunctionDefaultSketchBranchingFactorBinCount(),
                getQuantileComparator(0), // without privacy, the quantiles should match exactly
                "non-private tree matches quantiles",
                getPage(numbers, epsilon, delta, lower, upper),
                buildReferenceTree(numbers, lower, upper));
    }

    @Test
    public void testPrivate()
    {
        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        double epsilon = 8; // low privacy = low noise
        double delta = 0.001;
        double lower = 50;
        double upper = 150;
        int binCount = 1024; // 2^10 = 4^5
        int branchingFactor = 2;
        int sketchDepth = 1;
        int sketchWidth = 10_000; // do not sketch

        // With privacy, the quantiles won't match exactly but should be close.
        // The tolerance is set wide here, since the test is random.
        assertAggregation(
                getAggFunction(),
                getQuantileComparator(25),
                "private tree quantiles match (to within 25 SD)",
                getPage(numbers, epsilon, delta, lower, upper, binCount, branchingFactor, sketchDepth, sketchWidth),
                buildReferenceTree(numbers, lower, upper, binCount, branchingFactor, sketchDepth, sketchWidth));
    }

    @Test
    public void testPrivateDefaultSketch()
    {
        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        double epsilon = 8; // low privacy = low noise
        double delta = 0.001;
        double lower = 50;
        double upper = 150;
        int binCount = 1024; // 2^10 = 4^5
        int branchingFactor = 2;

        // With privacy, the quantiles won't match exactly but should be close.
        // The tolerance is set wide here, since the test is random.
        assertAggregation(
                getAggFunctionDefaultSketch(),
                getQuantileComparator(25),
                "private tree quantiles match (to within 25 SD)",
                getPage(numbers, epsilon, delta, lower, upper, binCount, branchingFactor),
                buildReferenceTree(numbers, lower, upper, binCount, branchingFactor));
    }

    @Test
    public void testPrivateDefaultSketchBranchingFactor()
    {
        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        double epsilon = 8; // low privacy = low noise
        double delta = 0.001;
        double lower = 50;
        double upper = 150;
        int binCount = 1024; // 2^10

        // With privacy, the quantiles won't match exactly but should be close.
        // The tolerance is set wide here, since the test is random.
        assertAggregation(
                getAggFunctionDefaultSketchBranchingFactor(),
                getQuantileComparator(25),
                "private tree quantiles match (to within 25 SD)",
                getPage(numbers, epsilon, delta, lower, upper, binCount),
                buildReferenceTree(numbers, lower, upper, binCount));
    }

    @Test
    public void testPrivateDefaultSketchBranchingFactorBinCount()
    {
        Double[] numbers = generateNormal(1000, 100, 1, new TestingSeededRandomizationStrategy(1));
        double epsilon = 8; // low privacy = low noise
        double delta = 0.001;
        double lower = 50;
        double upper = 150;

        // With privacy, the quantiles won't match exactly but should be close.
        // The tolerance is set wide here, since the test is random.
        assertAggregation(
                getAggFunctionDefaultSketchBranchingFactorBinCount(),
                getQuantileComparator(25),
                "private tree quantiles match (to within 25 SD)",
                getPage(numbers, epsilon, delta, lower, upper),
                buildReferenceTree(numbers, lower, upper));
    }

    private JavaAggregationFunctionImplementation getAggFunction()
    {
        return getFunction("noisy_qtree_agg", DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INTEGER, INTEGER, INTEGER, INTEGER);
    }

    private JavaAggregationFunctionImplementation getAggFunctionDefaultSketch()
    {
        return getFunction("noisy_qtree_agg", DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INTEGER, INTEGER);
    }

    private JavaAggregationFunctionImplementation getAggFunctionDefaultSketchBranchingFactor()
    {
        return getFunction("noisy_qtree_agg", DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INTEGER);
    }

    private JavaAggregationFunctionImplementation getAggFunctionDefaultSketchBranchingFactorBinCount()
    {
        return getFunction("noisy_qtree_agg", DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE);
    }

    private static Page getPage(Double[] values, double epsilon, double delta, double lower, double upper)
    {
        return new Page(
                createDoublesBlock(values),
                createRLEBlock(epsilon, values.length),
                createRLEBlock(delta, values.length),
                createRLEBlock(lower, values.length),
                createRLEBlock(upper, values.length));
    }

    private static Page getPage(Double[] values, double epsilon, double delta, double lower, double upper, long binCount)
    {
        return new Page(
                createDoublesBlock(values),
                createRLEBlock(epsilon, values.length),
                createRLEBlock(delta, values.length),
                createRLEBlock(lower, values.length),
                createRLEBlock(upper, values.length),
                createRLEBlock(binCount, values.length));
    }

    private static Page getPage(Double[] values, double epsilon, double delta, double lower, double upper, long binCount, long branchingFactor)
    {
        return new Page(
                createDoublesBlock(values),
                createRLEBlock(epsilon, values.length),
                createRLEBlock(delta, values.length),
                createRLEBlock(lower, values.length),
                createRLEBlock(upper, values.length),
                createRLEBlock(binCount, values.length),
                createRLEBlock(branchingFactor, values.length));
    }

    private static Page getPage(Double[] values, double epsilon, double delta, double lower, double upper, long binCount, long branchingFactor, long sketchDepth, long sketchWidth)
    {
        return new Page(
                createDoublesBlock(values),
                createRLEBlock(epsilon, values.length),
                createRLEBlock(delta, values.length),
                createRLEBlock(lower, values.length),
                createRLEBlock(upper, values.length),
                createRLEBlock(binCount, values.length),
                createRLEBlock(branchingFactor, values.length),
                createRLEBlock(sketchDepth, values.length),
                createRLEBlock(sketchWidth, values.length));
    }

    private static SqlVarbinary buildReferenceTree(Double[] numbers, double lower, double upper)
    {
        return buildReferenceTree(numbers, lower, upper, DEFAULT_BIN_COUNT, DEFAULT_BRANCHING_FACTOR, DEFAULT_SKETCH_DEPTH, DEFAULT_SKETCH_WIDTH);
    }

    private static SqlVarbinary buildReferenceTree(Double[] numbers, double lower, double upper, int binCount)
    {
        return buildReferenceTree(numbers, lower, upper, binCount, DEFAULT_BRANCHING_FACTOR, DEFAULT_SKETCH_DEPTH, DEFAULT_SKETCH_WIDTH);
    }

    private static SqlVarbinary buildReferenceTree(Double[] numbers, double lower, double upper, int binCount, int branchingFactor)
    {
        return buildReferenceTree(numbers, lower, upper, binCount, branchingFactor, DEFAULT_SKETCH_DEPTH, DEFAULT_SKETCH_WIDTH);
    }

    private static SqlVarbinary buildReferenceTree(Double[] numbers, double lower, double upper, int binCount, int branchingFactor, int sketchDepth, int sketchWidth)
    {
        QuantileTree tree = new QuantileTree(lower, upper, binCount, branchingFactor, sketchDepth, sketchWidth);

        for (double number : numbers) {
            tree.add(number);
        }

        return new SqlVarbinary(tree.serialize().getBytes());
    }
}
