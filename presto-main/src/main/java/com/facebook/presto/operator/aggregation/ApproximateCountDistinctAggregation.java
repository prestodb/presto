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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.BooleanDistinctState;
import com.facebook.presto.operator.aggregation.state.HyperLogLogState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.util.Failures.internalError;

@AggregationFunction("approx_distinct")
public final class ApproximateCountDistinctAggregation
{
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
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") long value,
            @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = HyperLogLogUtils.getOrCreateHyperLogLog(state, maxStandardError);
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
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") double value,
            @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = HyperLogLogUtils.getOrCreateHyperLogLog(state, maxStandardError);
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
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") Slice value,
            @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = HyperLogLogUtils.getOrCreateHyperLogLog(state, maxStandardError);
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
    public static void input(BooleanDistinctState state, @SqlType(StandardTypes.BOOLEAN) boolean value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        state.setByte((byte) (state.getByte() | (value ? 1 : 2)));
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        HyperLogLogUtils.mergeState(state, otherState.getHyperLogLog());
    }

    @CombineFunction
    public static void combineState(BooleanDistinctState state, BooleanDistinctState otherState)
    {
        state.setByte((byte) (state.getByte() | otherState.getByte()));
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

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(BooleanDistinctState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, Integer.bitCount(state.getByte()));
    }
}
