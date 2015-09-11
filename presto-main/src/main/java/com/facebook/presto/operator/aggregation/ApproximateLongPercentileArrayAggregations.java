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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.stats.QuantileDigest;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;

@AggregationFunction("approx_percentile")
public final class ApproximateLongPercentileArrayAggregations
{
    public static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION = new AggregationCompiler().generateAggregationFunction(ApproximateLongPercentileArrayAggregations.class, new ArrayType(BIGINT), ImmutableList.<Type>of(BIGINT, new ArrayType(DOUBLE)));

    private ApproximateLongPercentileArrayAggregations() {}

    @InputFunction
    public static void arrayInput(DigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType("array<double>") Block quantilesArrayBlock)
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

        List<Double> percentiles = new ArrayList<>(quantilesArrayBlock.getPositionCount());

        for (int i = 0; i < quantilesArrayBlock.getPositionCount(); i++) {
            if (quantilesArrayBlock.isNull(i)) {
                continue;
            }
            try {
                percentiles.add(DOUBLE.getDouble(quantilesArrayBlock, i));
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }

        state.setPercentiles(percentiles);
    }

    @CombineFunction
    public static void combine(DigestAndPercentileArrayState state, DigestAndPercentileArrayState otherState)
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

    @OutputFunction("array<bigint>")
    public static void output(DigestAndPercentileArrayState state, BlockBuilder out)
    {
        QuantileDigest digest = state.getDigest();
        List<Double> percentiles = state.getPercentiles();

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), percentiles.size());
        for (int i = 0; i < percentiles.size(); i++) {
            Double percentile = percentiles.get(i);
            checkState(percentile != -1.0, "Percentile is missing");
            checkCondition(0 <= percentile && percentile <= 1, INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
            try {
                BIGINT.writeLong(blockBuilder, digest.getQuantile(percentile));
            }
            catch (NullPointerException e) {
                // no-op
            }
        }

        new ArrayType(BIGINT).writeObject(out, blockBuilder.build());
    }
}
