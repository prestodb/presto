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

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.BlockState;
import com.facebook.presto.operator.aggregation.state.BlockStateSerializer;
import com.facebook.presto.operator.aggregation.state.NullableBooleanState;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public abstract class AbstractMinMaxAggregationFunction
        extends SqlAggregationFunction
{
    private static final MethodHandle LONG_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "input", MethodHandle.class, NullableLongState.class, long.class);
    private static final MethodHandle DOUBLE_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "input", MethodHandle.class, NullableDoubleState.class, double.class);
    private static final MethodHandle SLICE_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "input", MethodHandle.class, SliceState.class, Slice.class);
    private static final MethodHandle BOOLEAN_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "input", MethodHandle.class, NullableBooleanState.class, boolean.class);
    private static final MethodHandle BLOCK_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "input", MethodHandle.class, BlockState.class, Block.class);

    private static final MethodHandle LONG_OUTPUT_FUNCTION = methodHandle(NullableLongState.class, "write", Type.class, NullableLongState.class, BlockBuilder.class);
    private static final MethodHandle DOUBLE_OUTPUT_FUNCTION = methodHandle(NullableDoubleState.class, "write", Type.class, NullableDoubleState.class, BlockBuilder.class);
    private static final MethodHandle SLICE_OUTPUT_FUNCTION = methodHandle(SliceState.class, "write", Type.class, SliceState.class, BlockBuilder.class);
    private static final MethodHandle BOOLEAN_OUTPUT_FUNCTION = methodHandle(NullableBooleanState.class, "write", Type.class, NullableBooleanState.class, BlockBuilder.class);
    private static final MethodHandle BLOCK_OUTPUT_FUNCTION = methodHandle(BlockState.class, "write", Type.class, BlockState.class, BlockBuilder.class);

    private final OperatorType operatorType;

    private final StateCompiler compiler = new StateCompiler();

    protected AbstractMinMaxAggregationFunction(String name, OperatorType operatorType)
    {
        super(name,
                ImmutableList.of(orderableTypeParameter("E")),
                ImmutableList.of(),
                parseTypeSignature("E"),
                ImmutableList.of(parseTypeSignature("E")));
        requireNonNull(operatorType);
        this.operatorType = operatorType;
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("E");
        MethodHandle compareMethodHandle = functionRegistry.getScalarFunctionImplementation(internalOperator(operatorType, BOOLEAN, ImmutableList.of(type, type))).getMethodHandle();
        return generateAggregation(type, compareMethodHandle);
    }

    protected InternalAggregationFunction generateAggregation(Type type, MethodHandle compareMethodHandle)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AbstractMinMaxAggregationFunction.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(type);

        MethodHandle inputFunction;
        MethodHandle outputFunction;
        Class<? extends AccumulatorState> stateInterface;
        AccumulatorStateSerializer<?> stateSerializer;

        if (type.getJavaType() == long.class) {
            stateInterface = NullableLongState.class;
            stateSerializer = compiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = LONG_INPUT_FUNCTION;
            outputFunction = LONG_OUTPUT_FUNCTION;
        }
        else if (type.getJavaType() == double.class) {
            stateInterface = NullableDoubleState.class;
            stateSerializer = compiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = DOUBLE_INPUT_FUNCTION;
            outputFunction = DOUBLE_OUTPUT_FUNCTION;
        }
        else if (type.getJavaType() == Slice.class) {
            stateInterface = SliceState.class;
            stateSerializer = compiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = SLICE_INPUT_FUNCTION;
            outputFunction = SLICE_OUTPUT_FUNCTION;
        }
        else if (type.getJavaType() == boolean.class) {
            stateInterface = NullableBooleanState.class;
            stateSerializer = compiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = BOOLEAN_INPUT_FUNCTION;
            outputFunction = BOOLEAN_OUTPUT_FUNCTION;
        }
        else {
            stateInterface = BlockState.class;
            stateSerializer = new BlockStateSerializer(type);
            inputFunction = BLOCK_INPUT_FUNCTION;
            outputFunction = BLOCK_OUTPUT_FUNCTION;
        }

        inputFunction = inputFunction.bindTo(compareMethodHandle);
        outputFunction = outputFunction.bindTo(type);

        AccumulatorStateFactory<?> stateFactory = compiler.generateStateFactory(stateInterface, classLoader);

        Type intermediateType = stateSerializer.getSerializedType();
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getName(), type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createParameterMetadata(type),
                inputFunction,
                createParameterMetadata(intermediateType),
                inputFunction,
                null,
                outputFunction,
                stateInterface,
                stateSerializer,
                stateFactory,
                type,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getName(), inputTypes, intermediateType, type, true, false, factory);
    }

    private static List<ParameterMetadata> createParameterMetadata(Type type)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, type));
    }

    public static void input(MethodHandle methodHandle, NullableDoubleState state, double value)
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

    public static void input(MethodHandle methodHandle, NullableLongState state, long value)
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

    public static void input(MethodHandle methodHandle, SliceState state, Slice value)
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

    public static void input(MethodHandle methodHandle, NullableBooleanState state, boolean value)
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

    public static void input(MethodHandle methodHandle, BlockState state, Block value)
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
}
