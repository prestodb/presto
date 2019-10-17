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
package com.facebook.presto.operator.aggregation.discreteentropy;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static java.util.Locale.ENGLISH;

@AggregationFunction("discrete_entropy")
@Description("Computes discrete entropy based on random-variable samples")
public final class DiscreteEntropyAggregation
{
    private DiscreteEntropyAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.INTEGER) int sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        DiscreteEntropyStateStrategy strategy = DiscreteEntropyStateStrategy.getStrategy(
                state.getStrategy(),
                weight,
                method.toStringUtf8().toLowerCase(ENGLISH));
        strategy.add(sample, weight);
        state.setStrategy(strategy);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.BIGINT) long sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        input(state, Long.valueOf(sample).hashCode(), weight, method);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.BOOLEAN) boolean sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        input(state, sample ? 1 : 0, weight, method);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        input(state, Double.valueOf(sample).hashCode(), weight, method);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.VARCHAR) Slice sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        input(state, sample.toStringUtf8().hashCode(), weight, method);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.INTEGER) int sample,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        DiscreteEntropyStateStrategy strategy = DiscreteEntropyStateStrategy.getStrategy(
                state.getStrategy(),
                method.toStringUtf8().toLowerCase(ENGLISH));
        strategy.add(sample);
        state.setStrategy(strategy);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.BIGINT) long sample,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        input(state, Long.valueOf(sample).hashCode(), method);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.BOOLEAN) boolean sample,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        input(state, sample ? 1 : 0, method);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        input(state, Double.valueOf(sample).hashCode(), method);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.VARCHAR) Slice sample,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        input(state, sample.toStringUtf8().hashCode(), method);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.INTEGER) int sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        DiscreteEntropyStateStrategy strategy = DiscreteEntropyStateStrategy.getStrategy(
                state.getStrategy(),
                weight);
        strategy.add(sample, weight);
        state.setStrategy(strategy);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.BIGINT) long sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        input(state, Long.valueOf(sample).hashCode(), weight);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.BOOLEAN) boolean sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        input(state, sample ? 1 : 0, weight);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        input(state, Double.valueOf(sample).hashCode(), weight);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.VARCHAR) Slice sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        input(state, sample.toStringUtf8().hashCode(), weight);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.INTEGER) int sample)
    {
        DiscreteEntropyStateStrategy strategy = DiscreteEntropyStateStrategy.getStrategy(
                state.getStrategy());
        strategy.add(sample);
        state.setStrategy(strategy);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.BIGINT) long sample)
    {
        input(state, Long.valueOf(sample).hashCode());
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.BOOLEAN) boolean sample)
    {
        input(state, sample ? 1 : 0);
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.DOUBLE) double sample)
    {
        input(state, Double.valueOf(sample).hashCode());
    }

    @InputFunction
    public static void input(
            @AggregationState DiscreteEntropyState state,
            @SqlType(StandardTypes.VARCHAR) Slice sample)
    {
        input(state, sample.toStringUtf8().hashCode());
    }

    @CombineFunction
    public static void combine(
            @AggregationState DiscreteEntropyState state,
            @AggregationState DiscreteEntropyState otherState)
    {
        DiscreteEntropyStateStrategy strategy = state.getStrategy();
        DiscreteEntropyStateStrategy otherStrategy = otherState.getStrategy();
        if (strategy == null && otherStrategy != null) {
            state.setStrategy(otherStrategy);
            return;
        }
        if (otherStrategy == null) {
            return;
        }
        DiscreteEntropyStateStrategy.combine(strategy, otherStrategy);
        state.setStrategy(strategy);
    }

    @OutputFunction("double")
    public static void output(@AggregationState DiscreteEntropyState state, BlockBuilder out)
    {
        DiscreteEntropyStateStrategy strategy = state.getStrategy();
        double result = strategy == null ? 0.0 : strategy.calculateEntropy();
        DOUBLE.writeDouble(out, result);
    }
}
