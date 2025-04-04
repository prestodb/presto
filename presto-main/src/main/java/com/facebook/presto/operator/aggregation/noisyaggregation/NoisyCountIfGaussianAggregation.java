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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountAggregationUtils.combineStates;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountAggregationUtils.updateState;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountAggregationUtils.writeNoisyCountOutput;

@AggregationFunction(value = "noisy_count_if_gaussian", isCalledOnNullInput = true)
public final class NoisyCountIfGaussianAggregation
{
    private NoisyCountIfGaussianAggregation() {}

    @InputFunction
    public static void input(@AggregationState NoisyCountState state,
            @SqlType(StandardTypes.BOOLEAN) boolean value,
            @SqlType(StandardTypes.DOUBLE) double noiseScale)
    {
        if (value) {
            updateState(state, noiseScale, null);
        }
    }

    @InputFunction
    public static void input(@AggregationState NoisyCountState state,
            @SqlType(StandardTypes.BOOLEAN) boolean value,
            @SqlType(StandardTypes.DOUBLE) double noiseScale,
            @SqlType(StandardTypes.BIGINT) long randomSeed)
    {
        if (value) {
            updateState(state, noiseScale, randomSeed);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState NoisyCountState state, @AggregationState NoisyCountState otherState)
    {
        combineStates(state, otherState);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState NoisyCountState state, BlockBuilder out)
    {
        writeNoisyCountOutput(state, out);
    }
}
