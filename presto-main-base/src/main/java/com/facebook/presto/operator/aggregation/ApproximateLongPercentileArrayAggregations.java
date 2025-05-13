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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.DEFAULT_ACCURACY;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.DEFAULT_WEIGHT;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.checkAccuracy;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.checkWeight;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

@AggregationFunction("approx_percentile")
public final class ApproximateLongPercentileArrayAggregations
{
    private ApproximateLongPercentileArrayAggregations() {}

    @InputFunction
    public static void input(
            @AggregationState DigestAndPercentileArrayState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("array(double)") Block percentilesArrayBlock)
    {
        addInput(state, value, DEFAULT_WEIGHT, percentilesArrayBlock, DEFAULT_ACCURACY);
    }

    @InputFunction
    public static void input(
            @AggregationState DigestAndPercentileArrayState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("array(double)") Block percentilesArrayBlock,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        addInput(state, value, DEFAULT_WEIGHT, percentilesArrayBlock, accuracy);
    }

    @InputFunction
    public static void weightedInput(
            @AggregationState DigestAndPercentileArrayState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType("array(double)") Block percentilesArrayBlock)
    {
        checkWeight(weight);
        addInput(state, value, weight, percentilesArrayBlock, DEFAULT_ACCURACY);
    }

    @InputFunction
    public static void weightedInput(
            @AggregationState DigestAndPercentileArrayState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType("array(double)") Block percentilesArrayBlock,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        checkWeight(weight);
        addInput(state, value, weight, percentilesArrayBlock, accuracy);
    }

    private static void addInput(
            @AggregationState DigestAndPercentileArrayState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType("array(double)") Block percentilesArrayBlock,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        initializePercentilesArray(state, percentilesArrayBlock);

        QuantileDigest digest = state.getDigest();
        if (state.getDigest() == null) {
            checkAccuracy(accuracy);
            digest = new QuantileDigest(accuracy);
            state.setDigest(digest);
        }
        else {
            state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        }

        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
    }

    @CombineFunction
    public static void combine(@AggregationState DigestAndPercentileArrayState state, DigestAndPercentileArrayState otherState)
    {
        QuantileDigest otherDigest = otherState.getDigest();
        QuantileDigest digest = state.getDigest();

        if (digest == null) {
            state.setDigest(otherDigest);
            state.addMemoryUsage(otherDigest.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
            digest.merge(otherDigest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }

        state.setPercentiles(otherState.getPercentiles());
    }

    @OutputFunction("array(bigint)")
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
            BIGINT.writeLong(blockBuilder, digest.getQuantile(percentile));
        }

        out.closeEntry();
    }

    private static void initializePercentilesArray(@AggregationState DigestAndPercentileArrayState state, Block percentilesArrayBlock)
    {
        if (state.getPercentiles() == null) {
            ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();

            for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
                checkCondition(!percentilesArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "Percentile cannot be null");
                double percentile = DOUBLE.getDouble(percentilesArrayBlock, i);
                checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
                percentilesListBuilder.add(percentile);
            }

            state.setPercentiles(percentilesListBuilder.build());
        }
    }
}
