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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;

@AggregationFunction("approx_percentile")
public final class ApproximateDoublePercentileArrayAggregations
{
    public static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION = new AggregationCompiler().generateAggregationFunction(ApproximateDoublePercentileArrayAggregations.class, new ArrayType(DOUBLE), ImmutableList.<Type>of(DOUBLE, new ArrayType(DOUBLE)));

    private ApproximateDoublePercentileArrayAggregations() {}

    @InputFunction
    public static void arrayInput(DigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array<double>") Block quantilesArrayBlock)
    {
       ApproximateLongPercentileArrayAggregations.arrayInput(state, doubleToSortableLong(value), quantilesArrayBlock);
    }

    @CombineFunction
    public static void combine(DigestAndPercentileArrayState state, DigestAndPercentileArrayState otherState)
    {
        ApproximateLongPercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction("array<double>")
    public static void output(DigestAndPercentileArrayState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();

        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), percentiles.size());
        for (int i = 0; i < percentiles.size(); i++) {
            Double percentile = percentiles.get(i);
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            try {
                DOUBLE.writeDouble(blockBuilder, longToDouble(digest.getQuantile(percentile)));
            }
            catch (NullPointerException e) {
                // no-op
            }
        }

        new ArrayType(DOUBLE).writeObject(out, blockBuilder.build());
    }

    // TODO: raghavsethi: copied code.. what's the best way to re-use what's in ApproximateDoublePercentileAggregations?
    private static double longToDouble(long value)
    {
        if (value < 0) {
            value ^= 0x7fffffffffffffffL;
        }

        return Double.longBitsToDouble(value);
    }

    private static long doubleToSortableLong(double value)
    {
        long result = Double.doubleToRawLongBits(value);

        if (result < 0) {
            result ^= 0x7fffffffffffffffL;
        }

        return result;
    }
}
