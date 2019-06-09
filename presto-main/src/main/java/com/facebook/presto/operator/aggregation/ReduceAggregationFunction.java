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
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.state.NullableBooleanState;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public class ReduceAggregationFunction
        extends SqlAggregationFunction
{
    public static final ReduceAggregationFunction REDUCE_AGG = new ReduceAggregationFunction();
    private static final String NAME = "reduce_agg";

    private static final MethodHandle LONG_STATE_INPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "input", NullableLongState.class, Object.class, long.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle DOUBLE_STATE_INPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "input", NullableDoubleState.class, Object.class, double.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle BOOLEAN_STATE_INPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "input", NullableBooleanState.class, Object.class, boolean.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);

    private static final MethodHandle LONG_STATE_COMBINE_FUNCTION = methodHandle(ReduceAggregationFunction.class, "combine", NullableLongState.class, NullableLongState.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle DOUBLE_STATE_COMBINE_FUNCTION = methodHandle(ReduceAggregationFunction.class, "combine", NullableDoubleState.class, NullableDoubleState.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle BOOLEAN_STATE_COMBINE_FUNCTION = methodHandle(ReduceAggregationFunction.class, "combine", NullableBooleanState.class, NullableBooleanState.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);

    private static final MethodHandle LONG_STATE_OUTPUT_FUNCTION = methodHandle(NullableLongState.class, "write", Type.class, NullableLongState.class, BlockBuilder.class);
    private static final MethodHandle DOUBLE_STATE_OUTPUT_FUNCTION = methodHandle(NullableDoubleState.class, "write", Type.class, NullableDoubleState.class, BlockBuilder.class);
    private static final MethodHandle BOOLEAN_STATE_OUTPUT_FUNCTION = methodHandle(NullableBooleanState.class, "write", Type.class, NullableBooleanState.class, BlockBuilder.class);

    public ReduceAggregationFunction()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T"), typeVariable("S")),
                ImmutableList.of(),
                parseTypeSignature("S"),
                ImmutableList.of(
                        parseTypeSignature("T"),
                        parseTypeSignature("S"),
                        parseTypeSignature("function(S,T,S)"),
                        parseTypeSignature("function(S,S,S)")));
    }

    @Override
    public String getDescription()
    {
        return "Reduce input elements into a single value";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Type inputType = boundVariables.getTypeVariable("T");
        Type stateType = boundVariables.getTypeVariable("S");
        return generateAggregation(inputType, stateType);
    }

    private InternalAggregationFunction generateAggregation(Type inputType, Type stateType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ReduceAggregationFunction.class.getClassLoader());

        MethodHandle inputMethodHandle;
        MethodHandle combineMethodHandle;
        MethodHandle outputMethodHandle;
        AccumulatorStateDescriptor stateDescriptor;

        if (stateType.getJavaType() == long.class) {
            inputMethodHandle = LONG_STATE_INPUT_FUNCTION;
            combineMethodHandle = LONG_STATE_COMBINE_FUNCTION;
            outputMethodHandle = LONG_STATE_OUTPUT_FUNCTION.bindTo(stateType);
            stateDescriptor = new AccumulatorStateDescriptor(
                    NullableLongState.class,
                    StateCompiler.generateStateSerializer(NullableLongState.class, classLoader),
                    StateCompiler.generateStateFactory(NullableLongState.class, classLoader));
        }
        else if (stateType.getJavaType() == double.class) {
            inputMethodHandle = DOUBLE_STATE_INPUT_FUNCTION;
            combineMethodHandle = DOUBLE_STATE_COMBINE_FUNCTION;
            outputMethodHandle = DOUBLE_STATE_OUTPUT_FUNCTION.bindTo(stateType);
            stateDescriptor = new AccumulatorStateDescriptor(
                    NullableDoubleState.class,
                    StateCompiler.generateStateSerializer(NullableDoubleState.class, classLoader),
                    StateCompiler.generateStateFactory(NullableDoubleState.class, classLoader));
        }
        else if (stateType.getJavaType() == boolean.class) {
            inputMethodHandle = BOOLEAN_STATE_INPUT_FUNCTION;
            combineMethodHandle = BOOLEAN_STATE_COMBINE_FUNCTION;
            outputMethodHandle = BOOLEAN_STATE_OUTPUT_FUNCTION.bindTo(stateType);
            stateDescriptor = new AccumulatorStateDescriptor(
                    NullableBooleanState.class,
                    StateCompiler.generateStateSerializer(NullableBooleanState.class, classLoader),
                    StateCompiler.generateStateFactory(NullableBooleanState.class, classLoader));
        }
        else {
            // State with Slice or Block as native container type is intentionally not supported yet,
            // as it may result in excessive JVM memory usage of remembered set.
            // See JDK-8017163.
            throw new PrestoException(NOT_SUPPORTED, format("State type not supported for %s: %s", NAME, stateType.getDisplayName()));
        }

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getName(), inputType.getTypeSignature(), ImmutableList.of(inputType.getTypeSignature())),
                createInputParameterMetadata(inputType, stateType),
                inputMethodHandle.asType(
                        inputMethodHandle.type()
                                .changeParameterType(1, inputType.getJavaType())),
                combineMethodHandle,
                outputMethodHandle,
                ImmutableList.of(stateDescriptor),
                inputType,
                ImmutableList.of(BinaryFunctionInterface.class, BinaryFunctionInterface.class));

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(
                getSignature().getName(),
                ImmutableList.of(inputType),
                ImmutableList.of(stateType),
                stateType,
                true,
                false,
                factory,
                ImmutableList.of(BinaryFunctionInterface.class, BinaryFunctionInterface.class));
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type inputType, Type stateType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, inputType),
                new ParameterMetadata(INPUT_CHANNEL, stateType));
    }

    public static void input(NullableLongState state, Object value, long initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(initialStateValue);
        }
        state.setLong((long) inputFunction.apply(state.getLong(), value));
    }

    public static void input(NullableDoubleState state, Object value, double initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(initialStateValue);
        }
        state.setDouble((double) inputFunction.apply(state.getDouble(), value));
    }

    public static void input(NullableBooleanState state, Object value, boolean initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setBoolean(initialStateValue);
        }
        state.setBoolean((boolean) inputFunction.apply(state.getBoolean(), value));
    }

    public static void combine(NullableLongState state, NullableLongState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(otherState.getLong());
            return;
        }
        state.setLong((long) combineFunction.apply(state.getLong(), otherState.getLong()));
    }

    public static void combine(NullableDoubleState state, NullableDoubleState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(otherState.getDouble());
            return;
        }
        state.setDouble((double) combineFunction.apply(state.getDouble(), otherState.getDouble()));
    }

    public static void combine(NullableBooleanState state, NullableBooleanState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setBoolean(otherState.getBoolean());
            return;
        }
        state.setBoolean((boolean) combineFunction.apply(state.getBoolean(), otherState.getBoolean()));
    }
}
