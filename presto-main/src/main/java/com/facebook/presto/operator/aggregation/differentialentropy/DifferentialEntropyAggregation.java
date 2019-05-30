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

@AggregationFunction("differential_entropy")
@Description("Computes differential entropy based on random-variable samples")
public final class DifferentialEntropyAggregation
{
    private DifferentialEntropyAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState State state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.DOUBLE) double arg0,
            @SqlType(StandardTypes.DOUBLE) double arg1)
    {
        input(
                state,
                size,
                sample,
                Double.valueOf(weight),
                method,
                Double.valueOf(arg0),
                Double.valueOf(arg1));
    }

    @InputFunction
    public static void input(
            @AggregationState State state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight,
            @SqlType(StandardTypes.VARCHAR) Slice method)
    {
        input(
                state,
                size,
                sample,
                Double.valueOf(weight),
                method,
                (Double) null,
                (Double) null);
    }

    @InputFunction
    public static void input(
            @AggregationState State state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        input(
                state,
                size,
                sample,
                Double.valueOf(weight),
                null,
                (Double) null,
                (Double) null);
    }

    @InputFunction
    public static void input(
            @AggregationState State state,
            @SqlType(StandardTypes.BIGINT) long size,
            @SqlType(StandardTypes.DOUBLE) double sample)
    {
        input(
                state,
                size,
                sample,
                null,
                null,
                null,
                null);
    }

    protected static void input(
            State state,
            long size,
            double sample,
            Double weight,
            Slice method,
            Double arg0,
            Double arg1)
    {
        final String requestedMethod = method == null ? null : method.toStringUtf8();
        if (state.getStrategy() == null) {
            state.setStrategy(StateSerializer.create(
                    size, requestedMethod, arg0, arg1));
        }
        if (weight == null) {
            weight = Double.valueOf(1.0);
        }
        final StateStrategy strategy = state.getStrategy();
        StateSerializer.validate(requestedMethod, strategy);
        strategy.validateParams(size, sample, weight, arg0, arg1);
        strategy.add(sample, weight);
    }

    @CombineFunction
    public static void combine(
            @AggregationState State state,
            @AggregationState State otherState)
    {
        final StateStrategy strategy = state.getStrategy();
        final StateStrategy otherStrategy = otherState.getStrategy();
        if (strategy == null && otherStrategy != null) {
            state.setStrategy(otherStrategy.clone());
            return;
        }
        if (strategy != null && otherStrategy != null) {
            StateSerializer.combine(strategy, otherStrategy);
        }
    }

    @OutputFunction("double")
    public static void output(@AggregationState State state, BlockBuilder out)
    {
        final StateStrategy strategy = state.getStrategy();
        if (strategy == null) {
            DOUBLE.writeDouble(out, 0.0);
            return;
        }
        DOUBLE.writeDouble(
                out,
                strategy == null ? 0.0 : strategy.calculateEntropy());
    }
}
