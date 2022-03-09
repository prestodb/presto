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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.state.BlockPositionState;
import com.facebook.presto.operator.aggregation.state.BlockPositionStateSerializer;
import com.facebook.presto.operator.aggregation.state.NullableBooleanState;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.orderableTypeParameter;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.Failures.internalError;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public abstract class AbstractMinMaxAggregationFunction
        extends SqlAggregationFunction
{
    private static final MethodHandle LONG_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "input", MethodHandle.class, NullableLongState.class, long.class);
    private static final MethodHandle DOUBLE_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "input", MethodHandle.class, NullableDoubleState.class, double.class);
    private static final MethodHandle BOOLEAN_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "input", MethodHandle.class, NullableBooleanState.class, boolean.class);
    private static final MethodHandle BLOCK_POSITION_MIN_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "minInput", Type.class, BlockPositionState.class, Block.class, int.class);
    private static final MethodHandle BLOCK_POSITION_MAX_INPUT_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "maxInput", Type.class, BlockPositionState.class, Block.class, int.class);

    private static final MethodHandle LONG_OUTPUT_FUNCTION = methodHandle(NullableLongState.class, "write", Type.class, NullableLongState.class, BlockBuilder.class);
    private static final MethodHandle DOUBLE_OUTPUT_FUNCTION = methodHandle(NullableDoubleState.class, "write", Type.class, NullableDoubleState.class, BlockBuilder.class);
    private static final MethodHandle BOOLEAN_OUTPUT_FUNCTION = methodHandle(NullableBooleanState.class, "write", Type.class, NullableBooleanState.class, BlockBuilder.class);
    private static final MethodHandle BLOCK_POSITION_OUTPUT_FUNCTION = methodHandle(BlockPositionState.class, "write", Type.class, BlockPositionState.class, BlockBuilder.class);

    private static final MethodHandle LONG_COMBINE_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "combine", MethodHandle.class, NullableLongState.class, NullableLongState.class);
    private static final MethodHandle DOUBLE_COMBINE_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "combine", MethodHandle.class, NullableDoubleState.class, NullableDoubleState.class);
    private static final MethodHandle BOOLEAN_COMBINE_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "combine", MethodHandle.class, NullableBooleanState.class, NullableBooleanState.class);
    private static final MethodHandle BLOCK_POSITION_MIN_COMBINE_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "minCombine", Type.class, BlockPositionState.class, BlockPositionState.class);
    private static final MethodHandle BLOCK_POSITION_MAX_COMBINE_FUNCTION = methodHandle(AbstractMinMaxAggregationFunction.class, "maxCombine", Type.class, BlockPositionState.class, BlockPositionState.class);

    private final OperatorType operatorType;
    private final boolean min;

    protected AbstractMinMaxAggregationFunction(String name, boolean min)
    {
        super(name,
                ImmutableList.of(orderableTypeParameter("E")),
                ImmutableList.of(),
                parseTypeSignature("E"),
                ImmutableList.of(parseTypeSignature("E")));
        this.min = min;
        this.operatorType = min ? LESS_THAN : GREATER_THAN;
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = boundVariables.getTypeVariable("E");
        MethodHandle compareMethodHandle = functionAndTypeManager.getJavaScalarFunctionImplementation(
                functionAndTypeManager.resolveOperator(operatorType, fromTypes(type, type))).getMethodHandle();
        return generateAggregation(type, compareMethodHandle);
    }

    protected InternalAggregationFunction generateAggregation(Type type, MethodHandle compareMethodHandle)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AbstractMinMaxAggregationFunction.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(type);

        MethodHandle inputFunction;
        MethodHandle combineFunction;
        MethodHandle outputFunction;
        Class<? extends AccumulatorState> stateInterface;
        AccumulatorStateSerializer<?> stateSerializer;

        if (type.getJavaType() == long.class) {
            stateInterface = NullableLongState.class;
            stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = LONG_INPUT_FUNCTION.bindTo(compareMethodHandle);
            combineFunction = LONG_COMBINE_FUNCTION.bindTo(compareMethodHandle);
            outputFunction = LONG_OUTPUT_FUNCTION.bindTo(type);
        }
        else if (type.getJavaType() == double.class) {
            stateInterface = NullableDoubleState.class;
            stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = DOUBLE_INPUT_FUNCTION.bindTo(compareMethodHandle);
            combineFunction = DOUBLE_COMBINE_FUNCTION.bindTo(compareMethodHandle);
            outputFunction = DOUBLE_OUTPUT_FUNCTION.bindTo(type);
        }
        else if (type.getJavaType() == boolean.class) {
            stateInterface = NullableBooleanState.class;
            stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);
            inputFunction = BOOLEAN_INPUT_FUNCTION.bindTo(compareMethodHandle);
            combineFunction = BOOLEAN_COMBINE_FUNCTION.bindTo(compareMethodHandle);
            outputFunction = BOOLEAN_OUTPUT_FUNCTION.bindTo(type);
        }
        else {
            // native container type is Slice or Block
            stateInterface = BlockPositionState.class;
            stateSerializer = new BlockPositionStateSerializer(type);
            inputFunction = min ? BLOCK_POSITION_MIN_INPUT_FUNCTION.bindTo(type) : BLOCK_POSITION_MAX_INPUT_FUNCTION.bindTo(type);
            combineFunction = min ? BLOCK_POSITION_MIN_COMBINE_FUNCTION.bindTo(type) : BLOCK_POSITION_MAX_COMBINE_FUNCTION.bindTo(type);
            outputFunction = BLOCK_POSITION_OUTPUT_FUNCTION.bindTo(type);
        }

        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateInterface, classLoader);

        Type intermediateType = stateSerializer.getSerializedType();
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getNameSuffix(), type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createParameterMetadata(type),
                inputFunction,
                combineFunction,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getNameSuffix(), inputTypes, ImmutableList.of(intermediateType), type, true, false, factory);
    }

    private static List<ParameterMetadata> createParameterMetadata(Type type)
    {
        if (type.getJavaType().isPrimitive()) {
            return ImmutableList.of(
                    new ParameterMetadata(STATE),
                    new ParameterMetadata(INPUT_CHANNEL, type));
        }
        else {
            return ImmutableList.of(
                    new ParameterMetadata(STATE),
                    new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                    new ParameterMetadata(BLOCK_INDEX));
        }
    }

    public static void input(MethodHandle methodHandle, NullableDoubleState state, double value)
    {
        compareAndUpdateState(methodHandle, state, value);
    }

    public static void input(MethodHandle methodHandle, NullableLongState state, long value)
    {
        compareAndUpdateState(methodHandle, state, value);
    }

    public static void input(MethodHandle methodHandle, NullableBooleanState state, boolean value)
    {
        compareAndUpdateState(methodHandle, state, value);
    }

    public static void minInput(Type type, BlockPositionState state, Block block, int position)
    {
        if (state.getBlock() == null || type.compareTo(block, position, state.getBlock(), state.getPosition()) < 0) {
            state.setBlock(block);
            state.setPosition(position);
        }
    }

    public static void maxInput(Type type, BlockPositionState state, Block block, int position)
    {
        if (state.getBlock() == null || type.compareTo(block, position, state.getBlock(), state.getPosition()) > 0) {
            state.setBlock(block);
            state.setPosition(position);
        }
    }

    public static void combine(MethodHandle methodHandle, NullableLongState state, NullableLongState otherState)
    {
        compareAndUpdateState(methodHandle, state, otherState.getLong());
    }

    public static void combine(MethodHandle methodHandle, NullableDoubleState state, NullableDoubleState otherState)
    {
        compareAndUpdateState(methodHandle, state, otherState.getDouble());
    }

    public static void combine(MethodHandle methodHandle, NullableBooleanState state, NullableBooleanState otherState)
    {
        compareAndUpdateState(methodHandle, state, otherState.getBoolean());
    }

    public static void minCombine(Type type, BlockPositionState state, BlockPositionState otherState)
    {
        if (state.getBlock() == null || type.compareTo(otherState.getBlock(), otherState.getPosition(), state.getBlock(), state.getPosition()) < 0) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
        }
    }

    public static void maxCombine(Type type, BlockPositionState state, BlockPositionState otherState)
    {
        if (state.getBlock() == null || type.compareTo(otherState.getBlock(), otherState.getPosition(), state.getBlock(), state.getPosition()) > 0) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
        }
    }

    private static void compareAndUpdateState(MethodHandle methodHandle, NullableLongState state, long value)
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
            throw internalError(t);
        }
    }

    private static void compareAndUpdateState(MethodHandle methodHandle, NullableDoubleState state, double value)
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
            throw internalError(t);
        }
    }

    private static void compareAndUpdateState(MethodHandle methodHandle, NullableBooleanState state, boolean value)
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
            throw internalError(t);
        }
    }
}
