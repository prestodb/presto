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

import com.facebook.presto.operator.aggregation.state.BlockState;
import com.facebook.presto.operator.aggregation.state.BlockStateSerializer;
import com.facebook.presto.operator.aggregation.state.NullableBooleanState;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.AggregationStateSerializerFactory;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.operator.aggregation.MinMaxHelper.combineStateWithState;
import static com.facebook.presto.operator.aggregation.MinMaxHelper.combineStateWithValue;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;

@AggregationFunction("min")
@Description("Returns the minimum value of the argument")
public class MinAggregationFunction
{
    private MinAggregationFunction()
    {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableDoubleState state,
            @SqlType("T") double value)
    {
        combineStateWithValue(methodHandle, state, value);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableLongState state,
            @SqlType("T") long value)
    {
        combineStateWithValue(methodHandle, state, value);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState SliceState state,
            @SqlType("T") Slice value)
    {
        combineStateWithValue(methodHandle, state, value);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableBooleanState state,
            @SqlType("T") boolean value)
    {
        combineStateWithValue(methodHandle, state, value);
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState BlockState state,
            @SqlType("T") Block value)
    {
        combineStateWithValue(methodHandle, state, value);
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableLongState state,
            @AggregationState NullableLongState otherState)
    {
        combineStateWithState(methodHandle, state, otherState);
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableDoubleState state,
            @AggregationState NullableDoubleState otherState)
    {
        combineStateWithState(methodHandle, state, otherState);
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableBooleanState state,
            @AggregationState NullableBooleanState otherState)
    {
        combineStateWithState(methodHandle, state, otherState);
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState SliceState state,
            @AggregationState SliceState otherState)
    {
        combineStateWithState(methodHandle, state, otherState);
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState BlockState state,
            @AggregationState BlockState otherState)
    {
        combineStateWithState(methodHandle, state, otherState);
    }

    @OutputFunction("T")
    @TypeParameter("T")
    public static void output(
            @TypeParameter("T") Type type,
            @AggregationState NullableLongState state,
            BlockBuilder out)
    {
        NullableLongState.write(type, state, out);
    }

    @OutputFunction("T")
    @TypeParameter("T")
    public static void output(
            @TypeParameter("T") Type type,
            @AggregationState NullableDoubleState state,
            BlockBuilder out)
    {
        NullableDoubleState.write(type, state, out);
    }

    @OutputFunction("T")
    @TypeParameter("T")
    public static void output(
            @TypeParameter("T") Type type,
            @AggregationState NullableBooleanState state,
            BlockBuilder out)
    {
        NullableBooleanState.write(type, state, out);
    }

    @OutputFunction("T")
    @TypeParameter("T")
    public static void output(
            @TypeParameter("T") Type type,
            @AggregationState SliceState state,
            BlockBuilder out)
    {
        SliceState.write(type, state, out);
    }

    @OutputFunction("T")
    @TypeParameter("T")
    public static void output(
            @TypeParameter("T") Type type,
            @AggregationState BlockState state,
            BlockBuilder out)
    {
        BlockState.write(type, state, out);
    }

    @AggregationStateSerializerFactory(BlockState.class)
    @TypeParameter("T")
    public static AccumulatorStateSerializer<?> getStateSerializer(@TypeParameter("T") Type type)
    {
        return new BlockStateSerializer(type);
    }
}
