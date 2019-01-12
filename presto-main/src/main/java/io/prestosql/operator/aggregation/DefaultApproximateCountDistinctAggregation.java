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

import io.airlift.slice.Slice;
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

import static io.prestosql.spi.function.OperatorType.XX_HASH_64;

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
