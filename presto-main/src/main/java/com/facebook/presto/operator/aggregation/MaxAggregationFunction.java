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
import com.facebook.presto.operator.aggregation.state.NullableBooleanState;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;

@AggregationFunction("max")
@Description("Returns the maximum value of the argument")
public class MaxAggregationFunction
{
    private MaxAggregationFunction()
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableDoubleState state,
            @SqlType("T") double value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(value);
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(value, state.getDouble())) {
                state.setDouble(value);
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableLongState state,
            @SqlType("T") long value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(value);
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(value, state.getLong())) {
                state.setLong(value);
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState SliceState state,
            @SqlType("T") Slice value)
    {
        if (state.getSlice() == null) {
            state.setSlice(value);
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(value, state.getSlice())) {
                state.setSlice(value);
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableBooleanState state,
            @SqlType("T") boolean value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setBoolean(value);
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(value, state.getBoolean())) {
                state.setBoolean(value);
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState BlockState state,
            @SqlType("T") Block value)
    {
        if (state.getBlock() == null) {
            state.setBlock(value);
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(value, state.getBlock())) {
                state.setBlock(value);
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableLongState state,
            @AggregationState NullableLongState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(otherState.getLong());
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(otherState.getLong(), state.getLong())) {
                state.setLong(otherState.getLong());
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableDoubleState state,
            @AggregationState NullableDoubleState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(otherState.getDouble());
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(otherState.getDouble(), state.getDouble())) {
                state.setDouble(otherState.getDouble());
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState NullableBooleanState state,
            @AggregationState NullableBooleanState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setBoolean(otherState.getBoolean());
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(otherState.getBoolean(), state.getBoolean())) {
                state.setBoolean(otherState.getBoolean());
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState SliceState state,
            @AggregationState SliceState otherState)
    {
        if (state.getSlice() == null) {
            state.setSlice(otherState.getSlice());
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(otherState.getSlice(), state.getSlice())) {
                state.setSlice(otherState.getSlice());
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @CombineFunction
    public static void combine(
            @OperatorDependency(operator = GREATER_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle methodHandle,
            @AggregationState BlockState state,
            @AggregationState BlockState otherState)
    {
        if (state.getBlock() == null) {
            state.setBlock(otherState.getBlock());
            return;
        }
        try {
            if ((boolean) methodHandle.invokeExact(otherState.getBlock(), state.getBlock())) {
                state.setBlock(otherState.getBlock());
            }
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
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
}
