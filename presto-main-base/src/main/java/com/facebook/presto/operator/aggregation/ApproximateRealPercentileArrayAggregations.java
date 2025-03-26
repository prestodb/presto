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

import com.facebook.airlift.stats.QuantileDigest;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.DigestAndPercentileArrayState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import java.util.List;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.sortableIntToFloat;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("approx_percentile")
public class ApproximateRealPercentileArrayAggregations
{
    private ApproximateRealPercentileArrayAggregations() {}

    @InputFunction
    public static void input(
            @AggregationState DigestAndPercentileArrayState state,
            @SqlType(StandardTypes.REAL) long value,
            @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongPercentileArrayAggregations.input(state, floatToSortableInt(intBitsToFloat((int) value)), percentilesArrayBlock);
    }

    @InputFunction
    public static void input(
            @AggregationState DigestAndPercentileArrayState state,
            @SqlType(StandardTypes.REAL) long value,
            @SqlType("array(double)") Block percentilesArrayBlock,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        ApproximateLongPercentileArrayAggregations.input(state, floatToSortableInt(intBitsToFloat((int) value)), percentilesArrayBlock, accuracy);
    }

    @InputFunction
    public static void weightedInput(
            @AggregationState DigestAndPercentileArrayState state,
            @SqlType(StandardTypes.REAL) long value,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongPercentileArrayAggregations.weightedInput(state, floatToSortableInt(intBitsToFloat((int) value)), weight, percentilesArrayBlock);
    }

    @InputFunction
    public static void weightedInput(
            @AggregationState DigestAndPercentileArrayState state,
            @SqlType(StandardTypes.REAL) long value,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType("array(double)") Block percentilesArrayBlock,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        ApproximateLongPercentileArrayAggregations.weightedInput(state, floatToSortableInt(intBitsToFloat((int) value)), weight, percentilesArrayBlock, accuracy);
    }

    @CombineFunction
    public static void combine(@AggregationState DigestAndPercentileArrayState state, @AggregationState DigestAndPercentileArrayState otherState)
    {
        ApproximateLongPercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction("array(real)")
    public static void output(@AggregationState DigestAndPercentileArrayState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();

        if (percentiles == null || digest == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();

        for (int i = 0; i < percentiles.size(); i++) {
            Double percentile = percentiles.get(i);
            REAL.writeLong(blockBuilder, floatToRawIntBits(sortableIntToFloat((int) digest.getQuantile(percentile))));
        }

        out.closeEntry();
    }
}
