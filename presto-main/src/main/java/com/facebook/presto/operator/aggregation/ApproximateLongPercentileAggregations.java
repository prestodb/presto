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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.DigestAndPercentileState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;

@AggregationFunction("approx_percentile")
public final class ApproximateLongPercentileAggregations
{
    static final double DEFAULT_ACCURACY = 0.01;
    static final long DEFAULT_WEIGHT = 1;

    private ApproximateLongPercentileAggregations() {}

    @InputFunction
    public static void input(
            @AggregationState DigestAndPercentileState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        addInput(state, value, DEFAULT_WEIGHT, percentile, DEFAULT_ACCURACY);
    }

    @InputFunction
    public static void input(
            @AggregationState DigestAndPercentileState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.DOUBLE) double percentile,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        addInput(state, value, DEFAULT_WEIGHT, percentile, accuracy);
    }

    @InputFunction
    public static void weightedInput(
            @AggregationState DigestAndPercentileState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        checkWeight(weight);
        addInput(state, value, weight, percentile, DEFAULT_ACCURACY);
    }

    @InputFunction
    public static void weightedInput(
            @AggregationState DigestAndPercentileState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double percentile,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        checkWeight(weight);
        addInput(state, value, weight, percentile, accuracy);
    }

    private static void addInput(
            @AggregationState DigestAndPercentileState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long weight,
            @SqlType(StandardTypes.DOUBLE) double percentile,
            @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
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

        // use last percentile
        state.setPercentile(percentile);
    }

    @CombineFunction
    public static void combine(@AggregationState DigestAndPercentileState state, DigestAndPercentileState otherState)
    {
        QuantileDigest input = otherState.getDigest();

        QuantileDigest previous = state.getDigest();
        if (previous == null) {
            state.setDigest(input);
            state.addMemoryUsage(input.estimatedInMemorySizeInBytes());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySizeInBytes());
            previous.merge(input);
            state.addMemoryUsage(previous.estimatedInMemorySizeInBytes());
        }
        state.setPercentile(otherState.getPercentile());
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState DigestAndPercentileState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        double percentile = state.getPercentile();
        if (digest == null || digest.getCount() == 0.0) {
            out.appendNull();
        }
        else {
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            BIGINT.writeLong(out, digest.getQuantile(percentile));
        }
    }

    static void checkAccuracy(double accuracy)
    {
        checkCondition(0 < accuracy && accuracy < 1, INVALID_FUNCTION_ARGUMENT, "Percentile accuracy must be strictly between 0 and 1");
    }

    static void checkWeight(long weight)
    {
        checkCondition(weight > 0, INVALID_FUNCTION_ARGUMENT, "percentile weight must be > 0");
    }
}
