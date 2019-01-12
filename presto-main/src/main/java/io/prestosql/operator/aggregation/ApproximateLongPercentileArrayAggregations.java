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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.stats.QuantileDigest;
import io.prestosql.operator.aggregation.state.DigestAndPercentileArrayState;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.List;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.util.Failures.checkCondition;

@AggregationFunction("approx_percentile")
public final class ApproximateLongPercentileArrayAggregations
{
    private ApproximateLongPercentileArrayAggregations() {}

    @InputFunction
    public static void input(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        initializePercentilesArray(state, percentilesArrayBlock);
        initializeDigest(state);

        QuantileDigest digest = state.getDigest();
        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        initializePercentilesArray(state, percentilesArrayBlock);
        initializeDigest(state);

        QuantileDigest digest = state.getDigest();
        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
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

    private static void initializeDigest(@AggregationState DigestAndPercentileArrayState state)
    {
        QuantileDigest digest = state.getDigest();
        if (digest == null) {
            digest = new QuantileDigest(0.01);
            state.setDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }
    }
}
