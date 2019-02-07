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
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static com.facebook.presto.operator.scalar.QuantileDigestFunctions.digestToTruncatedMean;
import static com.facebook.presto.operator.scalar.QuantileDigestFunctions.validateQuantileRange;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("approx_truncated_mean")
public final class ApproximateLongTruncatedMeanAggregations
{
    private ApproximateLongTruncatedMeanAggregations() {}

    @InputFunction
    public static void input(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile, @SqlType(StandardTypes.DOUBLE) double upperQuantile)
    {
        ApproximateLongPercentileArrayAggregations.input(state, value, quantileBoundsToArray(lowerQuantile, upperQuantile));
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile, @SqlType(StandardTypes.DOUBLE) double upperQuantile)
    {
        ApproximateLongPercentileArrayAggregations.weightedInput(state, value, weight, quantileBoundsToArray(lowerQuantile, upperQuantile));
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile, @SqlType(StandardTypes.DOUBLE) double upperQuantile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        ApproximateLongPercentileArrayAggregations.weightedInput(state, value, weight, quantileBoundsToArray(lowerQuantile, upperQuantile), accuracy);
    }

    @CombineFunction
    public static void combine(@AggregationState DigestAndPercentileArrayState state, DigestAndPercentileArrayState otherState)
    {
        ApproximateLongPercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState DigestAndPercentileArrayState state, BlockBuilder out)
    {
        digestStateToTruncatedMean(state, out, QuantileDigest.MiddleFunction.DEFAULT);
    }

    public static void digestStateToTruncatedMean(@AggregationState DigestAndPercentileArrayState state, BlockBuilder out, QuantileDigest.MiddleFunction middleFunction)
    {
        QuantileDigest digest = state.getDigest();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            List<Long> bounds = digest.getQuantiles(state.getPercentiles());
            Double mean = digestToTruncatedMean(digest, bounds, middleFunction);
            if (mean == null) {
                out.appendNull();
            }
            else {
                DOUBLE.writeDouble(out, mean);
            }
        }
    }

    public static BlockBuilder quantileBoundsToArray(double lowerQuantile, double upperQuantile)
    {
        validateQuantileRange(lowerQuantile, upperQuantile);
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 2);
        DOUBLE.writeDouble(blockBuilder, lowerQuantile);
        DOUBLE.writeDouble(blockBuilder, upperQuantile);
        return blockBuilder;
    }
}
