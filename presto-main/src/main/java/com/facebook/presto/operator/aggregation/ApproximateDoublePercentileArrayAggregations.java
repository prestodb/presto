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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static com.facebook.presto.operator.aggregation.LongDoubleConverterUtil.doubleToSortableLong;
import static com.facebook.presto.operator.aggregation.LongDoubleConverterUtil.sortableLongToDouble;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.testing.AggregationTestUtils.generateInternalAggregationFunction;

@AggregationFunction("approx_percentile")
public final class ApproximateDoublePercentileArrayAggregations
{
    public static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION =
            generateInternalAggregationFunction(
                    ApproximateDoublePercentileArrayAggregations.class,
                    new ArrayType(DOUBLE).getTypeSignature(),
                    ImmutableList.of(DOUBLE.getTypeSignature(), new ArrayType(DOUBLE).getTypeSignature()));
    public static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION =
            generateInternalAggregationFunction(
                    ApproximateDoublePercentileArrayAggregations.class,
                    new ArrayType(DOUBLE).getTypeSignature(),
                    ImmutableList.of(DOUBLE.getTypeSignature(), BIGINT.getTypeSignature(), new ArrayType(DOUBLE).getTypeSignature()));

    private ApproximateDoublePercentileArrayAggregations() {}

    @InputFunction
    public static void input(DigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongPercentileArrayAggregations.input(state, doubleToSortableLong(value), percentilesArrayBlock);
    }

    @InputFunction
    public static void weightedInput(DigestAndPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongPercentileArrayAggregations.weightedInput(state, doubleToSortableLong(value), weight, percentilesArrayBlock);
    }

    @CombineFunction
    public static void combine(DigestAndPercentileArrayState state, DigestAndPercentileArrayState otherState)
    {
        ApproximateLongPercentileArrayAggregations.combine(state, otherState);
    }

    @OutputFunction("array(double)")
    public static void output(DigestAndPercentileArrayState state, BlockBuilder out)
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
            DOUBLE.writeDouble(blockBuilder, sortableLongToDouble(digest.getQuantile(percentile)));
        }

        out.closeEntry();
    }
}
