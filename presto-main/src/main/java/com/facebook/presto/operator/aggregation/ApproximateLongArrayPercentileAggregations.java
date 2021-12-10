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
import com.facebook.presto.operator.aggregation.state.ApproxPercentileState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.DEFAULT_ACCURACY;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.DEFAULT_WEIGHT;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.checkAccuracy;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.checkWeight;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;

@AggregationFunction("approx_percentile")
public final class ApproximateLongArrayPercentileAggregations
{
    private ApproximateLongArrayPercentileAggregations() {}

    @InputFunction
    public static void input(
            @AggregationState ApproxPercentileState state,
            @SqlType("array(bigint)") Block valuesArrayBlock,
            @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        addInput(state, valuesArrayBlock, DEFAULT_WEIGHT, percentile, DEFAULT_ACCURACY);
    }

    @InputFunction
    public static void input(
            @AggregationState ApproxPercentileState state,
            @SqlType("array(bigint)") Block valuesArrayBlock,
            @SqlType(StandardTypes.DOUBLE) double percentile,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        addInput(state, valuesArrayBlock, DEFAULT_WEIGHT, percentile, accuracy);
    }

    @InputFunction
    public static void weightedInput(
            @AggregationState ApproxPercentileState state,
            @SqlType("array(bigint)") Block valuesArrayBlock,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        checkWeight(weight);
        addInput(state, valuesArrayBlock, weight, percentile, DEFAULT_ACCURACY);
    }

    @InputFunction
    public static void weightedInput(
            @AggregationState ApproxPercentileState state,
            @SqlType("array(bigint)") Block valuesArrayBlock,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double percentile,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        checkWeight(weight);
        addInput(state, valuesArrayBlock, weight, percentile, accuracy);
    }

    private static void addInput(
            @AggregationState ApproxPercentileState state,
            @SqlType("array(bigint)") Block valuesArrayBlock,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double percentile,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        initializeQuantileDigestArray(state, valuesArrayBlock, accuracy);
        state.addDigests(valuesArrayBlock, weight);
        state.setPercentile(percentile);
    }

    @CombineFunction
    public static void combine(@AggregationState ApproxPercentileState state, ApproxPercentileState otherState)
    {
        QuantileDigest[] otherDigests = otherState.getDigests();
        QuantileDigest[] digests = state.getDigests();

        if (digests == null) {
            state.setDigests(otherDigests);
            state.addMemoryUsage(state.estimatedInMemorySizeInBytes());
        }
        else {
            checkState(otherDigests != null, "The digests to be combined cannot be null");
            checkState(otherDigests.length == digests.length, "The sizes of inputs cannot be different");

            for (int i = 0; i < digests.length; i++) {
                state.addMemoryUsage(-digests[i].estimatedInMemorySizeInBytes());
                digests[i].merge(otherDigests[i]);
                state.addMemoryUsage(digests[i].estimatedInMemorySizeInBytes());
            }
        }

        state.setPercentile(otherState.getPercentile());
    }

    @OutputFunction("array(bigint)")
    public static void output(@AggregationState ApproxPercentileState state, BlockBuilder out)
    {
        QuantileDigest[] digests = state.getDigests();
        double percentile = state.getPercentile();

        if (digests == null) {
            out.appendNull();
        }
        else {
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");

            BlockBuilder blockBuilder = out.beginBlockEntry();
            for (QuantileDigest digest : digests) {
                BIGINT.writeLong(blockBuilder, digest.getQuantile(percentile));
            }
            out.closeEntry();
        }
    }

    private static void initializeQuantileDigestArray(@AggregationState ApproxPercentileState state, Block valuesArrayBlock, double accuracy)
    {
        if (state.getDigests() == null) {
            checkAccuracy(accuracy);
            int length = valuesArrayBlock.getPositionCount();
            QuantileDigest[] digests = new QuantileDigest[length];
            for (int i = 0; i < length; i++) {
                digests[i] = new QuantileDigest(accuracy);
            }
            state.setDigests(digests);
        }
    }
}
