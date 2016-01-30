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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.QuantileDigest;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;

@AggregationFunction("approx_percentile")
public final class ApproximateLongPercentileAggregations
{
    public static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_AGGREGATION = new AggregationCompiler().generateAggregationFunction(ApproximateLongPercentileAggregations.class, BIGINT, ImmutableList.<Type>of(BIGINT, DOUBLE));
    public static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION = new AggregationCompiler().generateAggregationFunction(ApproximateLongPercentileAggregations.class, BIGINT, ImmutableList.<Type>of(BIGINT, BIGINT, DOUBLE));
    public static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_ACCURACY = new AggregationCompiler().generateAggregationFunction(ApproximateLongPercentileAggregations.class, BIGINT, ImmutableList.<Type>of(BIGINT, BIGINT, DOUBLE, DOUBLE));

    private ApproximateLongPercentileAggregations() {}

    @InputFunction
    public static void input(DigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        QuantileDigest digest = state.getDigest();

        if (digest == null) {
            digest = new QuantileDigest(0.01);
            state.setDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }

        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());

        // use last percentile
        state.setPercentile(percentile);
    }

    @InputFunction
    public static void weightedInput(DigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        checkWeight(weight);

        QuantileDigest digest = state.getDigest();

        if (digest == null) {
            digest = new QuantileDigest(0.01);
            state.setDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }

        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());

        // use last percentile
        state.setPercentile(percentile);
    }

    @InputFunction
    public static void weightedInput(DigestAndPercentileState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.DOUBLE) double accuracy)
    {
        checkWeight(weight);

        QuantileDigest digest = state.getDigest();

        if (digest == null) {
            if (accuracy > 0 && accuracy < 1) {
                digest = new QuantileDigest(accuracy);
            }
            else {
                throw new IllegalArgumentException("Percentile accuracy must be strictly between 0 and 1");
            }
            state.setDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }

        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());

        // use last percentile
        state.setPercentile(percentile);
    }

    @CombineFunction
    public static void combine(DigestAndPercentileState state, DigestAndPercentileState otherState)
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
            BIGINT.writeLong(out, digest.getQuantile(percentile));
        }
    }

    private static void checkWeight(long weight)
    {
        checkCondition(weight > 0, INVALID_FUNCTION_ARGUMENT, "percentile weight must be > 0");
    }
}
