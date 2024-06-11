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

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.facebook.airlift.stats.cardinality.PrivateLpcaSketch;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarFunctionConstantStats;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.operator.aggregation.ApproximateSetAggregation.DEFAULT_STANDARD_ERROR;
import static com.facebook.presto.operator.aggregation.HyperLogLogUtils.standardErrorToBuckets;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.PropagateSourceStats.ROW_COUNT;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.EXPERIMENTAL;
import static com.facebook.presto.util.Failures.checkCondition;

public final class HyperLogLogFunctions
{
    private HyperLogLogFunctions() {}

    @ScalarFunction
    @Description("compute the cardinality of a HyperLogLog instance")
    @SqlType(StandardTypes.BIGINT)
    @ScalarFunctionConstantStats(avgRowSize = 8.0, nullFraction = 0.0, minValue = 0)
    public static long cardinality(@ScalarPropagateSourceStats(distinctValueCount = ROW_COUNT, maxValue = ROW_COUNT) @SqlType(StandardTypes.HYPER_LOG_LOG) Slice serializedHll)
    {
        return HyperLogLog.newInstance(serializedHll).cardinality();
    }

    @ScalarFunction(visibility = EXPERIMENTAL)
    @Description("compute the noisy cardinality of a HyperLogLog instance")
    @SqlType(StandardTypes.BIGINT)
    public static long noisyCardinality(@SqlType(StandardTypes.HYPER_LOG_LOG) Slice serializedHll, @SqlType(StandardTypes.DOUBLE) double epsilon)
    {
        checkCondition(epsilon > 0, INVALID_FUNCTION_ARGUMENT, "Epsilon must be greater than 0");
        PrivateLpcaSketch privacySketch = new PrivateLpcaSketch(HyperLogLog.newInstance(serializedHll), 0.1 * epsilon, 0.9 * epsilon);
        return privacySketch.cardinality();
    }

    @ScalarFunction
    @Description("an empty HyperLogLog instance")
    @SqlType(StandardTypes.HYPER_LOG_LOG)
    public static Slice emptyApproxSet()
    {
        return HyperLogLog.newInstance(standardErrorToBuckets(DEFAULT_STANDARD_ERROR)).serialize();
    }

    @ScalarFunction
    @Description("an empty HyperLogLog instance with the specified max standard error")
    @SqlType(StandardTypes.HYPER_LOG_LOG)
    public static Slice emptyApproxSet(@SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        return HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError)).serialize();
    }

    @ScalarFunction("merge_hll")
    @Description("merge the contents of an array of HyperLogLogs")
    @SqlType(StandardTypes.HYPER_LOG_LOG)
    @SqlNullable
    public static Slice scalarMerge(@SqlType("array(HyperLogLog)") Block block)
    {
        if (block.getPositionCount() == 0) {
            return null;
        }

        HyperLogLog merged = null;
        int firstNonNullIndex = 0;

        while (firstNonNullIndex < block.getPositionCount() && block.isNull(firstNonNullIndex)) {
            firstNonNullIndex++;
        }

        if (firstNonNullIndex == block.getPositionCount()) {
            return null;
        }

        Slice initialSlice = block.getSlice(firstNonNullIndex, 0, block.getSliceLength(firstNonNullIndex));
        merged = HyperLogLog.newInstance(initialSlice);

        for (int i = firstNonNullIndex + 1; i < block.getPositionCount(); i++) {
            Slice currentSlice = block.getSlice(i, 0, block.getSliceLength(i));
            if (!block.isNull(i)) {
                merged.mergeWith(HyperLogLog.newInstance(currentSlice));
            }
        }

        return merged.serialize();
    }
}
