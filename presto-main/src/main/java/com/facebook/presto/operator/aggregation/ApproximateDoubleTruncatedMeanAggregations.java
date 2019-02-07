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

import com.facebook.presto.operator.aggregation.state.DigestAndPercentileArrayState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.operator.aggregation.ApproximateLongTruncatedMeanAggregations.quantileBoundsToArray;
import static com.facebook.presto.operator.scalar.QuantileDigestFunctions.DOUBLE_MIDDLE_FUNCTION;

@AggregationFunction("approx_truncated_mean")
public final class ApproximateDoubleTruncatedMeanAggregations
{
    private ApproximateDoubleTruncatedMeanAggregations() {}

    @InputFunction
    public static void input(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile, @SqlType(StandardTypes.DOUBLE) double upperQuantile)
    {
        ApproximateDoublePercentileArrayAggregations.input(state, value, quantileBoundsToArray(lowerQuantile, upperQuantile));
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile, @SqlType(StandardTypes.DOUBLE) double upperQuantile)
    {
        ApproximateDoublePercentileArrayAggregations.weightedInput(state, value, weight, quantileBoundsToArray(lowerQuantile, upperQuantile));
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile, @SqlType(StandardTypes.DOUBLE) double upperQuantile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        ApproximateDoublePercentileArrayAggregations.weightedInput(state, value, weight, quantileBoundsToArray(lowerQuantile, upperQuantile), accuracy);
    }

    @CombineFunction
    public static void combine(@AggregationState DigestAndPercentileArrayState state, DigestAndPercentileArrayState otherState)
    {
        ApproximateDoublePercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState DigestAndPercentileArrayState state, BlockBuilder out)
    {
        ApproximateLongTruncatedMeanAggregations.digestStateToTruncatedMean(state, out, DOUBLE_MIDDLE_FUNCTION);
    }
}
