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
package com.facebook.presto.operator.aggregation.differentialentropy;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static java.util.Locale.ENGLISH;

@AggregationFunction("differential_entropy")
@Description("Computes differential entropy based on random-variable samples")
public final class DifferentialEntropyAggregation
{
    private DifferentialEntropyAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState DifferentialEntropyState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.DOUBLE) double min,
            @SqlType(StandardTypes.DOUBLE) double max)
    {
        DifferentialEntropyStateStrategy strategy = DifferentialEntropyStateStrategy.getStrategy(
                state.getStrategy(),
                size,
                sample,
                weight,
                method.toStringUtf8().toLowerCase(ENGLISH),
                min,
                max);
        state.setStrategy(strategy);
        strategy.add(sample, weight);
    }

    @InputFunction
    public static void input(
            @AggregationState DifferentialEntropyState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        DifferentialEntropyStateStrategy strategy = DifferentialEntropyStateStrategy.getStrategy(
                state.getStrategy(),
                size,
                sample,
                weight);
        state.setStrategy(strategy);
        strategy.add(sample, weight);
    }

    @InputFunction
    public static void input(
            @AggregationState DifferentialEntropyState state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample)
    {
        DifferentialEntropyStateStrategy strategy = DifferentialEntropyStateStrategy.getStrategy(
                state.getStrategy(),
                size,
                sample);
        state.setStrategy(strategy);
        strategy.add(sample);
    }

    @CombineFunction
    public static void combine(
            @AggregationState DifferentialEntropyState state,
            @AggregationState DifferentialEntropyState otherState)
    {
        DifferentialEntropyStateStrategy strategy = state.getStrategy();
        DifferentialEntropyStateStrategy otherStrategy = otherState.getStrategy();
        if (strategy == null && otherStrategy != null) {
            state.setStrategy(otherStrategy);
            return;
        }
        if (otherStrategy == null) {
            return;
        }
        DifferentialEntropyStateStrategy.combine(strategy, otherStrategy);
    }

    @OutputFunction("double")
    public static void output(@AggregationState DifferentialEntropyState state, BlockBuilder out)
    {
        DifferentialEntropyStateStrategy strategy = state.getStrategy();
        DOUBLE.writeDouble(out, strategy == null ? Double.NaN : strategy.calculateEntropy());
    }
}
