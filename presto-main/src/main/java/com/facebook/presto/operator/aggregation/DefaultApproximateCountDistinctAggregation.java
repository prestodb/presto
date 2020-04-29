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

@AggregationFunction("approx_distinct")
public final class DefaultApproximateCountDistinctAggregation
{
    private static final double DEFAULT_STANDARD_ERROR = 0.023;

    private DefaultApproximateCountDistinctAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState HyperLogLogState state,
            @BlockPosition @SqlType("unknown") Block block,
            @BlockIndex int index)
    {
        // do nothing
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") long value)
    {
        ApproximateCountDistinctAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") double value)
    {
        ApproximateCountDistinctAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") Slice value)
    {
        ApproximateCountDistinctAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(BooleanDistinctState state, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        ApproximateCountDistinctAggregation.input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        ApproximateCountDistinctAggregation.combineState(state, otherState);
    }

    @CombineFunction
    public static void combineState(BooleanDistinctState state, BooleanDistinctState otherState)
    {
        ApproximateCountDistinctAggregation.combineState(state, otherState);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        ApproximateCountDistinctAggregation.evaluateFinal(state, out);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(BooleanDistinctState state, BlockBuilder out)
    {
        ApproximateCountDistinctAggregation.evaluateFinal(state, out);
    }
}
