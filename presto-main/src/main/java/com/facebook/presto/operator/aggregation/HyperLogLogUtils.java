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

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.facebook.presto.operator.aggregation.state.HyperLogLogState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationState;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

public final class HyperLogLogUtils
{
    private static final double LOWEST_MAX_STANDARD_ERROR = 0.0040625;
    private static final double HIGHEST_MAX_STANDARD_ERROR = 0.26000;

    private HyperLogLogUtils()
    {
    }

    public static int standardErrorToBuckets(double maxStandardError)
    {
        checkCondition(maxStandardError >= LOWEST_MAX_STANDARD_ERROR && maxStandardError <= HIGHEST_MAX_STANDARD_ERROR,
                INVALID_FUNCTION_ARGUMENT,
                "Max standard error must be in [%s, %s]: %s", LOWEST_MAX_STANDARD_ERROR, HIGHEST_MAX_STANDARD_ERROR, maxStandardError);
        return log2Ceiling((int) Math.ceil(1.0816 / (maxStandardError * maxStandardError)));
    }

    public static HyperLogLog getOrCreateHyperLogLog(HyperLogLogState state, double maxStandardError)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    public static void mergeState(@AggregationState HyperLogLogState state, HyperLogLog input)
    {
        HyperLogLog previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(input);
            state.addMemoryUsage(input.estimatedInMemorySize());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySize());
            try {
                // Throws if the bucket counts are different.
                // We currently cannot access these counts from HLL so we must
                // catch and rethrow a more useful exception
                previous.mergeWith(input);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
            }
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    private static int log2Ceiling(int value)
    {
        return Integer.highestOneBit(value - 1) << 1;
    }
}
