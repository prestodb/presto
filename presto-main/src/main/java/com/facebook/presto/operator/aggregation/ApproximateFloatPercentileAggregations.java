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

import com.facebook.presto.operator.aggregation.state.DigestAndPercentileState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.stats.QuantileDigest;

import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.sortableIntToFloat;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("approx_percentile")
public class ApproximateFloatPercentileAggregations
{
    private ApproximateFloatPercentileAggregations() {}

    @InputFunction
    public static void input(DigestAndPercentileState state, @SqlType(StandardTypes.FLOAT) long value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateLongPercentileAggregations.input(state, floatToSortableInt(intBitsToFloat((int) value)), percentile);
    }

    @InputFunction
    public static void weightedInput(DigestAndPercentileState state, @SqlType(StandardTypes.FLOAT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        ApproximateLongPercentileAggregations.weightedInput(state, floatToSortableInt(intBitsToFloat((int) value)), weight, percentile);
    }

    @InputFunction
    public static void weightedInput(DigestAndPercentileState state, @SqlType(StandardTypes.FLOAT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        ApproximateLongPercentileAggregations.weightedInput(state, floatToSortableInt(intBitsToFloat((int) value)), weight, percentile, accuracy);
    }

    @CombineFunction
    public static void combine(DigestAndPercentileState state, DigestAndPercentileState otherState)
    {
        ApproximateLongPercentileAggregations.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.FLOAT)
    public static void output(DigestAndPercentileState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            FLOAT.writeLong(out, floatToRawIntBits(sortableIntToFloat((int) digest.getQuantile(percentile))));
        }
    }
}
