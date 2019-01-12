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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.sortableIntToFloat;
import static io.prestosql.operator.aggregation.FloatingPointBitsConverterUtil.sortableLongToDouble;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.Float.floatToRawIntBits;

public final class QuantileDigestFunctions
{
    public static final double DEFAULT_ACCURACY = 0.01;
    public static final long DEFAULT_WEIGHT = 1L;

    private QuantileDigestFunctions() {}

    @ScalarFunction("value_at_quantile")
    @Description("Given an input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType(StandardTypes.DOUBLE)
    public static double valueAtQuantileDouble(@SqlType("qdigest(double)") Slice input, @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        return sortableLongToDouble(valueAtQuantileBigint(input, quantile));
    }

    @ScalarFunction("value_at_quantile")
    @Description("Given an input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType(StandardTypes.REAL)
    public static long valueAtQuantileReal(@SqlType("qdigest(real)") Slice input, @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        return floatToRawIntBits(sortableIntToFloat((int) valueAtQuantileBigint(input, quantile)));
    }

    @ScalarFunction("value_at_quantile")
    @Description("Given an input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType(StandardTypes.BIGINT)
    public static long valueAtQuantileBigint(@SqlType("qdigest(bigint)") Slice input, @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        return new QuantileDigest(input).getQuantile(quantile);
    }

    @ScalarFunction("values_at_quantiles")
    @Description("For each input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType("array(double)")
    public static Block valuesAtQuantilesDouble(@SqlType("qdigest(double)") Slice input, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        QuantileDigest digest = new QuantileDigest(input);
        BlockBuilder output = DOUBLE.createBlockBuilder(null, percentilesArrayBlock.getPositionCount());
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            DOUBLE.writeDouble(output, sortableLongToDouble(digest.getQuantile(DOUBLE.getDouble(percentilesArrayBlock, i))));
        }
        return output.build();
    }

    @ScalarFunction("values_at_quantiles")
    @Description("For each input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType("array(real)")
    public static Block valuesAtQuantilesReal(@SqlType("qdigest(real)") Slice input, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        QuantileDigest digest = new QuantileDigest(input);
        BlockBuilder output = REAL.createBlockBuilder(null, percentilesArrayBlock.getPositionCount());
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            REAL.writeLong(output, floatToRawIntBits(sortableIntToFloat((int) digest.getQuantile(DOUBLE.getDouble(percentilesArrayBlock, i)))));
        }
        return output.build();
    }

    @ScalarFunction("values_at_quantiles")
    @Description("For each input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the qdigest is qn.")
    @SqlType("array(bigint)")
    public static Block valuesAtQuantilesBigint(@SqlType("qdigest(bigint)") Slice input, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        QuantileDigest digest = new QuantileDigest(input);
        BlockBuilder output = BIGINT.createBlockBuilder(null, percentilesArrayBlock.getPositionCount());
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            BIGINT.writeLong(output, digest.getQuantile(DOUBLE.getDouble(percentilesArrayBlock, i)));
        }
        return output.build();
    }

    public static double verifyAccuracy(double accuracy)
    {
        checkCondition(accuracy > 0 && accuracy < 1, INVALID_FUNCTION_ARGUMENT, "Percentile accuracy must be exclusively between 0 and 1, was %s", accuracy);
        return accuracy;
    }

    public static long verifyWeight(long weight)
    {
        checkCondition(weight > 0, INVALID_FUNCTION_ARGUMENT, "Percentile weight must be > 0, was %s", weight);
        return weight;
    }
}
