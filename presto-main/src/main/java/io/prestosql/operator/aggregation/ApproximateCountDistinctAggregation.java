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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;
import io.prestosql.operator.aggregation.state.HyperLogLogState;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Failures.internalError;

@AggregationFunction("approx_distinct")
public final class ApproximateCountDistinctAggregation
{
    private static final double LOWEST_MAX_STANDARD_ERROR = 0.0040625;
    private static final double HIGHEST_MAX_STANDARD_ERROR = 0.26000;

    private ApproximateCountDistinctAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState HyperLogLogState state,
            @BlockPosition @SqlType("unknown") Block block,
            @BlockIndex int index,
            @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        // do nothing
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") long value,
            @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        long hash;
        try {
            hash = (long) methodHandle.invokeExact(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        hll.addHash(hash);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") double value,
            @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        long hash;
        try {
            hash = (long) methodHandle.invokeExact(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        hll.addHash(hash);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") Slice value,
            @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        long hash;
        try {
            hash = (long) methodHandle.invokeExact(value);
        }
        catch (Throwable t) {
            throw internalError(t);
        }
        hll.addHash(hash);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    private static HyperLogLog getOrCreateHyperLogLog(HyperLogLogState state, double maxStandardError)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    @VisibleForTesting
    static int standardErrorToBuckets(double maxStandardError)
    {
        checkCondition(maxStandardError >= LOWEST_MAX_STANDARD_ERROR && maxStandardError <= HIGHEST_MAX_STANDARD_ERROR,
                INVALID_FUNCTION_ARGUMENT,
                "Max standard error must be in [%s, %s]: %s", LOWEST_MAX_STANDARD_ERROR, HIGHEST_MAX_STANDARD_ERROR, maxStandardError);
        return log2Ceiling((int) Math.ceil(1.0816 / (maxStandardError * maxStandardError)));
    }

    private static int log2Ceiling(int value)
    {
        return Integer.highestOneBit(value - 1) << 1;
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        HyperLogLog input = otherState.getHyperLogLog();

        HyperLogLog previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(input);
            state.addMemoryUsage(input.estimatedInMemorySize());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySize());
            previous.mergeWith(input);
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        HyperLogLog hyperLogLog = state.getHyperLogLog();
        if (hyperLogLog == null) {
            BIGINT.writeLong(out, 0);
        }
        else {
            BIGINT.writeLong(out, hyperLogLog.cardinality());
        }
    }
}
