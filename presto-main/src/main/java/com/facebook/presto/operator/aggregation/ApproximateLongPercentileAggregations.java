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

import com.facebook.presto.operator.aggregation.state.QuantileDigestAndPercentileState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.LongArrayBlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileArrayAggregations.initializeDigest;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileArrayAggregations.initializePercentilesArray;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;

@AggregationFunction("approx_percentile")
public final class ApproximateLongPercentileAggregations
{
    private static final double STANDARD_ERROR = 0.01;

    private ApproximateLongPercentileAggregations() {}

    @InputFunction
    public static void input(@AggregationState QuantileDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        initializePercentilesArray(state, createPercentileBlock(percentile));
        initializeDigest(state, STANDARD_ERROR);

        QuantileDigest digest = state.getDigest();

        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());

        // use last percentile
        state.setPercentiles(ImmutableList.of(percentile));
    }

    @InputFunction
    public static void weightedInput(@AggregationState QuantileDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        checkWeight(weight);
        initializePercentilesArray(state, createPercentileBlock(percentile));
        initializeDigest(state, STANDARD_ERROR);

        QuantileDigest digest = state.getDigest();

        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());

        // use last percentile
        state.setPercentiles(ImmutableList.of(percentile));
    }

    @InputFunction
    public static void weightedInput(@AggregationState QuantileDigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        checkWeight(weight);
        initializePercentilesArray(state, createPercentileBlock(percentile));
        initializeDigest(state, accuracy);

        QuantileDigest digest = state.getDigest();

        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());

        // use last percentile
        state.setPercentiles(ImmutableList.of(percentile));
    }

    private static Block createPercentileBlock(double percentile)
    {
        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 1);
        DOUBLE.writeDouble(blockBuilder, percentile);
        return blockBuilder.build();
    }

    @CombineFunction
    public static void combine(@AggregationState QuantileDigestAndPercentileState state, QuantileDigestAndPercentileState otherState)
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
        state.setPercentiles(otherState.getPercentiles());
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState QuantileDigestAndPercentileState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();

        if (percentiles == null || digest == null || digest.getCount() == 0) {
            out.appendNull();
            return;
        }

        checkState(percentiles.get(0) != -1.0, "Percentile is missing");
        checkCondition(0 <= percentiles.get(0) && percentiles.get(0) <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
        BIGINT.writeLong(out, digest.getQuantile(percentiles.get(0)));
    }

    private static void checkWeight(long weight)
    {
        checkCondition(weight > 0, INVALID_FUNCTION_ARGUMENT, "percentile weight must be > 0");
    }
}
