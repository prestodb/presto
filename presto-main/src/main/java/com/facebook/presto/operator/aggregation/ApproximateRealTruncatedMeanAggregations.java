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

import static com.facebook.presto.operator.scalar.QuantileDigestFunctions.REAL_MIDDLE_FUNCTION;
import static com.facebook.presto.operator.scalar.QuantileDigestFunctions.digestToTruncatedMean;
import static com.facebook.presto.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;

@AggregationFunction("approx_truncated_mean")
public final class ApproximateRealTruncatedMeanAggregations
{
    private ApproximateRealTruncatedMeanAggregations() {}

    @InputFunction
    public static void input(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.REAL) long value,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile, @SqlType(StandardTypes.DOUBLE) double upperQuantile)
    {
        ApproximateRealPercentileArrayAggregations.input(state, value,
                ApproximateLongTruncatedMeanAggregations.quantileBoundsToArray(lowerQuantile, upperQuantile));
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile, @SqlType(StandardTypes.DOUBLE) double upperQuantile)
    {
        ApproximateRealPercentileArrayAggregations.weightedInput(state, value, weight,
                ApproximateLongTruncatedMeanAggregations.quantileBoundsToArray(lowerQuantile, upperQuantile));
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile, @SqlType(StandardTypes.DOUBLE) double upperQuantile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        ApproximateRealPercentileArrayAggregations.weightedInput(state, value, weight,
                ApproximateLongTruncatedMeanAggregations.quantileBoundsToArray(lowerQuantile, upperQuantile), accuracy);
    }

    @CombineFunction
    public static void combine(@AggregationState DigestAndPercentileArrayState state, DigestAndPercentileArrayState otherState)
    {
        ApproximateRealPercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.REAL)
    public static void output(@AggregationState DigestAndPercentileArrayState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            List<Long> bounds = digest.getQuantiles(state.getPercentiles());
            Double mean = digestToTruncatedMean(digest, bounds, REAL_MIDDLE_FUNCTION);
            if (mean == null) {
                out.appendNull();
            }
            else {
                REAL.writeLong(out, floatToRawIntBits(mean.floatValue()));
            }
        }
    }
}
