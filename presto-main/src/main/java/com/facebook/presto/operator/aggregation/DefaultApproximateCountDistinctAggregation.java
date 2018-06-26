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

import com.facebook.presto.operator.aggregation.state.HyperLogLogState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.XX_HASH_64;

@AggregationFunction("approx_distinct")
public final class DefaultApproximateCountDistinctAggregation
{
    private static final double DEFAULT_STANDARD_ERROR = 0.023;

    private DefaultApproximateCountDistinctAggregation() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") long value)
    {
        ApproximateCountDistinctAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") double value)
    {
        ApproximateCountDistinctAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = XX_HASH_64, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle methodHandle,
            @AggregationState HyperLogLogState state,
            @SqlType("T") Slice value)
    {
        ApproximateCountDistinctAggregation.input(methodHandle, state, value, DEFAULT_STANDARD_ERROR);
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        ApproximateCountDistinctAggregation.combineState(state, otherState);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        ApproximateCountDistinctAggregation.evaluateFinal(state, out);
    }
}
