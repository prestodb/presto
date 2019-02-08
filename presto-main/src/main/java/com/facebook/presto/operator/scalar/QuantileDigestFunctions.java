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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.stats.QuantileDigest;

import java.util.List;

import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.sortableIntToFloat;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.sortableLongToDouble;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Float.floatToRawIntBits;

public final class QuantileDigestFunctions
{
    public static final double DEFAULT_ACCURACY = 0.01;
    public static final long DEFAULT_WEIGHT = 1L;

    public static final QuantileDigest.MiddleFunction DOUBLE_MIDDLE_FUNCTION = (long lowerBound, long upperBound) -> {
        // There is an edge case when dealing with digests built from large numbers either side of 0:
        // The internal nodes of the digest have a huge spread in values, and some empty nodes
        // have an upper bound which is Long.MAX_VALUE larger than its lower bound.
        // Although these empty nodes do not end up in the calculation (since count = 0), the MiddleFunction
        // is nonetheless called. If unchecked, sortableLongToDouble will return NaN and break the calculation,
        // so we must catch it and return 0.
        // To verify this, build a digest with values: (-2.0, -1.0, 1.0, 2.0)
        if ((upperBound - lowerBound) == Long.MAX_VALUE) {
            return 0;
        }
        // Underflow is safer than overflow, so halve first
        return sortableLongToDouble(lowerBound) / 2.0D + sortableLongToDouble(upperBound) / 2.0D;
    };

    public static final QuantileDigest.MiddleFunction REAL_MIDDLE_FUNCTION = (long lowerBound, long upperBound) -> {
        if ((upperBound - lowerBound) == Integer.MAX_VALUE) {
            return 0;
        }
        return ((double) sortableIntToFloat((int) lowerBound) + (double) sortableIntToFloat((int) upperBound)) / 2.0D;
    };

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

    @SqlNullable
    @ScalarFunction("truncated_mean")
    @Description("The approx arithmetic mean, estimated from qdigest, of values in the quantile range bounded by lowerQuantile and upperQuantile.")
    @SqlType(StandardTypes.DOUBLE)
    public static Double truncatedMeanDouble(
            @SqlType("qdigest(double)") Slice input,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile,
            @SqlType(StandardTypes.DOUBLE) double upperQuantile)
    {
        validateQuantileRange(lowerQuantile, upperQuantile);
        QuantileDigest digest = new QuantileDigest(input);
        List<Long> bounds = digest.getQuantiles(ImmutableList.of(lowerQuantile, upperQuantile));
        return digestToTruncatedMean(digest, bounds, DOUBLE_MIDDLE_FUNCTION);
    }

    @SqlNullable
    @ScalarFunction("truncated_mean")
    @Description("The approx arithmetic mean, estimated from qdigest, of values in the quantile range bounded by lowerQuantile and upperQuantile.")
    @SqlType(StandardTypes.REAL)
    public static Long truncatedMeanReal(
            @SqlType("qdigest(real)") Slice input,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile,
            @SqlType(StandardTypes.DOUBLE) double upperQuantile)
    {
        validateQuantileRange(lowerQuantile, upperQuantile);
        QuantileDigest digest = new QuantileDigest(input);
        List<Long> bounds = digest.getQuantiles(ImmutableList.of(lowerQuantile, upperQuantile));
        Double mean = digestToTruncatedMean(digest, bounds, REAL_MIDDLE_FUNCTION);
        if (mean == null) {
            return null;
        }
        return (long) floatToRawIntBits(mean.floatValue());
    }

    @SqlNullable
    @ScalarFunction("truncated_mean")
    @Description("The approx arithmetic mean, estimated from qdigest, of values in the quantile range bounded by lowerQuantile and upperQuantile.")
    @SqlType(StandardTypes.DOUBLE)
    public static Double truncatedMeanBigint(
            @SqlType("qdigest(bigint)") Slice input,
            @SqlType(StandardTypes.DOUBLE) double lowerQuantile,
            @SqlType(StandardTypes.DOUBLE) double upperQuantile)
    {
        validateQuantileRange(lowerQuantile, upperQuantile);
        QuantileDigest digest = new QuantileDigest(input);
        List<Long> bounds = digest.getQuantiles(ImmutableList.of(lowerQuantile, upperQuantile));
        return digestToTruncatedMean(digest, bounds, QuantileDigest.MiddleFunction.DEFAULT);
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

    public static void validateQuantileRange(double lowerQuantile, double upperQuantile)
    {
        checkCondition(0 <= lowerQuantile && lowerQuantile <= 1, INVALID_FUNCTION_ARGUMENT, "lowerQuantile must be between 0 and 1");
        checkCondition(0 <= upperQuantile && upperQuantile <= 1, INVALID_FUNCTION_ARGUMENT, "upperQuantile must be between 0 and 1");
        checkCondition(lowerQuantile < upperQuantile, INVALID_FUNCTION_ARGUMENT, "lowerQuantile must be strictly less than upperQuantile");
    }

    /**
     * A histogram made from two quantiles returns two Buckets:
     * 1) values below the lower bound (exclusive)
     * 2) values between the lower bound (inclusive) and the upper bound (exclusive)
     * <p>
     * The truncated mean comes from the second Bucket
     * If the lower and upper bounds are the same, the Bucket is empty so we return null
     * NB: even if the quantiles are different, the corresponding values that constitute the histogram bounds
     * may be the same, e.g. in the set(1,5,9), quantiles 0.49 and 0.5 both resolve to the value 5.
     */
    public static Double digestToTruncatedMean(QuantileDigest digest, List<Long> bounds, QuantileDigest.MiddleFunction middleFunction)
    {
        if (bounds.get(0).equals(bounds.get(1))) {
            return null;
        }
        return digest.getHistogram(bounds, middleFunction).get(1).getMean();
    }
}
