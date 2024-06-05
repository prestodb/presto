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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_BIN_COUNT;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_BRANCHING_FACTOR;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_SKETCH_DEPTH;
import static com.facebook.presto.operator.aggregation.noisyaggregation.QuantileTreeAggregationUtils.DEFAULT_SKETCH_WIDTH;

/**
 * noisy_approx_percentile_qtree returns noisy estimates of percentiles from a multiset of values.
 * Like all noisy aggregations, it does not achieve a true DP guarantee,
 * as it returns NULL in the absence of data. Its DP-like privacy guarantee holds at the row-level under
 * unbounded-neighbors (add/remove) semantics. This corresponds to user-level privacy in the case when a user
 * contributes at most one row. To achieve user-level privacy when users contribute more than one row, divide
 * the privacy budget accordingly.
 */
@AggregationFunction(value = "noisy_approx_percentile_qtree")
public final class NoisyApproxPercentileQuantileTreeAggregation
{
    private NoisyApproxPercentileQuantileTreeAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState QuantileTreeState state,
            @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.DOUBLE) double probability,
            @SqlType(StandardTypes.DOUBLE) double epsilon,
            @SqlType(StandardTypes.DOUBLE) double delta,
            @SqlType(StandardTypes.DOUBLE) double lower,
            @SqlType(StandardTypes.DOUBLE) double upper)
    {
        QuantileTreeAggregationUtils.inputValue(state, value, probability, epsilon, delta, lower, upper,
                DEFAULT_BIN_COUNT, DEFAULT_BRANCHING_FACTOR, DEFAULT_SKETCH_DEPTH, DEFAULT_SKETCH_WIDTH);
    }

    @CombineFunction
    public static void combineStates(@AggregationState QuantileTreeState state, @AggregationState QuantileTreeState otherState)
    {
        QuantileTreeAggregationUtils.combineStates(state, otherState);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void evaluateFinal(@AggregationState QuantileTreeState state, BlockBuilder out)
    {
        if (state.getQuantileTree() == null) {
            out.appendNull();
        }
        else {
            QuantileTreeAggregationUtils.enablePrivacy(state);
            double probability = state.getProbabilities().get(0);
            double quantile = state.getQuantileTree().quantile(probability);
            DoubleType.DOUBLE.writeDouble(out, quantile);
        }
    }
}
