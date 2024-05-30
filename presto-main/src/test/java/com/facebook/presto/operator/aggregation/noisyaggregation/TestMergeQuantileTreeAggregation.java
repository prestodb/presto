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
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.RandomizationStrategy;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.TestingSeededRandomizationStrategy;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.type.QuantileTreeType;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_BIN_COUNT;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_BRANCHING_FACTOR;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_SKETCH_DEPTH;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_SKETCH_WIDTH;
import static org.testng.Assert.assertThrows;

public class TestMergeQuantileTreeAggregation
        extends AbstractTestQuantileTreeAggregation
{
    @Test
    public void testMergeOne()
    {
        JavaAggregationFunctionImplementation mergeAgg = getFunction("merge", QuantileTreeType.QUANTILE_TREE_TYPE);

        int n = 1_000;
        double mean = 100;
        double standardDeviation = 10;
        double rho = 0.5;
        double lower = 50;
        double upper = 150;
        QuantileTree tree = generateNormalSketch(n, mean, standardDeviation, lower, upper, rho, new TestingSeededRandomizationStrategy(1));

        assertAggregation(
                mergeAgg,
                getQuantileComparator(0), // quantiles should match exactly (merge is deterministic)
                "quantiles of merged tree = quantiles of original tree (single tree)",
                getPage(tree),
                new SqlVarbinary(tree.serialize().getBytes()));
    }

    @Test
    public void testMergeSeveral()
    {
        JavaAggregationFunctionImplementation mergeAgg = getFunction("merge", QuantileTreeType.QUANTILE_TREE_TYPE);

        int numSketches = 20;
        int nPerSketch = 1_000;
        double mean = 0;
        double standardDeviation = 1;
        double rho = 0.5;
        double lower = -10;
        double upper = 10;

        QuantileTree[] trees = new QuantileTree[numSketches];
        for (int i = 0; i < numSketches; i++) {
            trees[i] = generateNormalSketch(nPerSketch, mean, standardDeviation, lower, upper, rho, new TestingSeededRandomizationStrategy(i));
        }

        assertAggregation(
                mergeAgg,
                getQuantileComparator(0), // quantiles should match exactly (merge is deterministic)
                "quantiles of merged tree = quantiles of manually merged tree",
                getPage(trees),
                buildMergedSketch(trees));
    }

    @Test
    public void testMergeIncompatible()
    {
        JavaAggregationFunctionImplementation mergeAgg = getFunction("merge", QuantileTreeType.QUANTILE_TREE_TYPE);

        QuantileTree[] trees = new QuantileTree[2];
        // Note: These trees are incompatible because their bounds are different (0-123 vs. 123-456).
        trees[0] = generateNormalSketch(100, 100, 10, 0, 123, 0.5, new TestingSeededRandomizationStrategy(1));
        trees[1] = generateNormalSketch(100, 200, 10, 123, 456, 0.5, new TestingSeededRandomizationStrategy(2));

        assertThrows(PrestoException.class, () -> {
            assertAggregation(
                    mergeAgg,
                    getQuantileComparator(Double.POSITIVE_INFINITY), // doesn't matter
                    null,
                    getPage(trees),
                    null);
        });
    }

    private static QuantileTree generateNormalSketch(int n, double mean, double standardDeviation, double lower, double upper, double rho, RandomizationStrategy randomizationStrategy)
    {
        QuantileTree tree = new QuantileTree(lower, upper,
                DEFAULT_BIN_COUNT, DEFAULT_BRANCHING_FACTOR, DEFAULT_SKETCH_DEPTH, DEFAULT_SKETCH_WIDTH);
        for (double number : generateNormal(n, mean, standardDeviation, randomizationStrategy)) {
            tree.add(number);
        }
        if (rho != QuantileTree.NON_PRIVATE_RHO) {
            tree.enablePrivacy(rho, randomizationStrategy);
        }
        return tree;
    }

    private static Page getPage(QuantileTree... trees)
    {
        BlockBuilder builder = QuantileTreeType.QUANTILE_TREE_TYPE.createBlockBuilder(null, trees.length);
        for (QuantileTree tree : trees) {
            QuantileTreeType.QUANTILE_TREE_TYPE.writeSlice(builder, tree.serialize());
        }
        return new Page(builder.build());
    }

    private static SqlVarbinary buildMergedSketch(QuantileTree... trees)
    {
        if (trees.length == 0) {
            return null;
        }

        QuantileTree merged = QuantileTree.deserialize(trees[0].serialize()); // roundabout clone
        for (int i = 1; i < trees.length; i++) {
            merged.merge(trees[i]);
        }

        return new SqlVarbinary(merged.serialize().getBytes());
    }
}
