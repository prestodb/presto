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

import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.sortableIntToFloat;
import static io.prestosql.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction("approx_percentile")
public class ApproximateRealPercentileArrayAggregations
{
    private ApproximateRealPercentileArrayAggregations() {}

    @InputFunction
    public static void input(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.REAL) long value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongPercentileArrayAggregations.input(state, floatToSortableInt(intBitsToFloat((int) value)), percentilesArrayBlock);
    }

    @InputFunction
    public static void weightedInput(@AggregationState DigestAndPercentileArrayState state, @SqlType(StandardTypes.REAL) long value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        ApproximateLongPercentileArrayAggregations.weightedInput(state, floatToSortableInt(intBitsToFloat((int) value)), weight, percentilesArrayBlock);
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
