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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.RandomizationStrategy;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SecureRandomizationStrategy;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.QuantileTreeType;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

public final class QuantileTreeScalarFunctions
{
    private QuantileTreeScalarFunctions() {}

    @ScalarFunction(value = "cardinality")
    @Description("Returns the approximate number of items in a QuantileTree sketch")
    @SqlType(StandardTypes.BIGINT)
    public static long cardinality(@SqlType(QuantileTreeType.NAME) Slice serializedSketch)
    {
        double cardinality = QuantileTree.deserialize(serializedSketch).cardinality();
        return Math.max(0, Math.round(cardinality));
    }

    @ScalarFunction(value = "noisy_empty_qtree", deterministic = false)
    @Description("Creates an empty QuantileTree sketch object")
    @SqlType(QuantileTreeType.NAME)
    public static Slice emptyQuantileTree(
            @SqlType(StandardTypes.DOUBLE) double epsilon,
            @SqlType(StandardTypes.DOUBLE) double delta,
            @SqlType(StandardTypes.DOUBLE) double lower,
            @SqlType(StandardTypes.DOUBLE) double upper)
    {
        return emptyQuantileTree(epsilon, delta, lower, upper, new SecureRandomizationStrategy());
    }

    /**
     * For mocking purposes, expose a version of the emptyQuantileTree function that accepts a RandomizationStrategy
     */
    @VisibleForTesting
    static Slice emptyQuantileTree(double epsilon, double delta, double lower, double upper, RandomizationStrategy randomizationStrategy)
    {
        QuantileTree tree = new QuantileTree(lower, upper,
                QuantileTreeAggregationUtils.DEFAULT_BIN_COUNT,
                QuantileTreeAggregationUtils.DEFAULT_BRANCHING_FACTOR,
                QuantileTreeAggregationUtils.DEFAULT_SKETCH_WIDTH,
                QuantileTreeAggregationUtils.DEFAULT_SKETCH_DEPTH);
        tree.enablePrivacy(QuantileTree.getRhoForEpsilonDelta(epsilon, delta), randomizationStrategy);

        return tree.serialize();
    }

    @ScalarFunction(value = "ensure_noise_qtree", deterministic = false)
    @Description("Adds noise to a QuantileTree sketch to ensure a given level of noise")
    @SqlType(QuantileTreeType.NAME)
    public static Slice ensureNoise(
            @SqlType(QuantileTreeType.NAME) Slice serializedSketch,
            @SqlType(StandardTypes.DOUBLE) double epsilon,
            @SqlType(StandardTypes.DOUBLE) double delta)
    {
        QuantileTree tree = QuantileTree.deserialize(serializedSketch);
        tree.enablePrivacy(QuantileTree.getRhoForEpsilonDelta(epsilon, delta), new SecureRandomizationStrategy());
        return tree.serialize();
    }

    @ScalarFunction(value = "value_at_quantile")
    @Description("Returns the approximate value from the quantile tree corresponding to a cumulative probability between 0 and 1")
    @SqlType(StandardTypes.DOUBLE)
    public static double valueAtQuantile(@SqlType(QuantileTreeType.NAME) Slice serializedSketch, @SqlType(StandardTypes.DOUBLE) double probability)
    {
        return QuantileTree.deserialize(serializedSketch).quantile(probability);
    }

    @ScalarFunction(value = "values_at_quantiles")
    @Description("Returns the approximate values from the quantile tree corresponding to cumulative probabilities between 0 and 1")
    @SqlType("array(double)")
    public static Block valuesAtQuantiles(@SqlType(QuantileTreeType.NAME) Slice serializedSketch, @SqlType("array(double)") Block probabilityArrayBlock)
    {
        QuantileTree deserializedQuantileTree = QuantileTree.deserialize(serializedSketch);
        BlockBuilder output = DOUBLE.createBlockBuilder(null, probabilityArrayBlock.getPositionCount());
        for (int i = 0; i < probabilityArrayBlock.getPositionCount(); i++) {
            double probability = DOUBLE.getDouble(probabilityArrayBlock, i);
            DOUBLE.writeDouble(output, deserializedQuantileTree.quantile(probability));
        }
        return output.build();
    }
}
