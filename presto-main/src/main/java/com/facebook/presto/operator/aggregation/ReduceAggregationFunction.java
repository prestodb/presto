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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.state.ReduceAggregationState;
import com.facebook.presto.operator.aggregation.state.ReduceAggregationStateFactory;
import com.facebook.presto.operator.aggregation.state.ReduceAggregationStateSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public class ReduceAggregationFunction
        extends SqlAggregationFunction
{
    private static final String NAME = "reduce_agg";

    private static final MethodHandle INPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "input", Type.class, ReduceAggregationState.class, Object.class, Object.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ReduceAggregationFunction.class, "combine", ReduceAggregationState.class, ReduceAggregationState.class, BinaryFunctionInterface.class, BinaryFunctionInterface.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "write", Type.class, ReduceAggregationState.class, BlockBuilder.class);

    private final boolean supportsComplexTypes;

    public ReduceAggregationFunction(boolean supportsComplexTypes)
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

        this.supportsComplexTypes = supportsComplexTypes;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "Reduce input elements into a single value";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
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

        if (!supportsComplexTypes && !(stateType.getJavaType() == long.class || stateType.getJavaType() == double.class || stateType.getJavaType() == boolean.class)) {
            // For large heap, State with Slice or Block may result in excessive JVM memory usage of remembered set.
            // See JDK-8017163.
            throw new PrestoException(NOT_SUPPORTED, format("State type not enabled for %s: %s", NAME, stateType.getDisplayName()));
        }

        inputMethodHandle = INPUT_FUNCTION.bindTo(inputType);
        combineMethodHandle = COMBINE_FUNCTION;
        outputMethodHandle = OUTPUT_FUNCTION.bindTo(stateType);

        stateDescriptor = new AccumulatorStateDescriptor(
                ReduceAggregationState.class,
                new ReduceAggregationStateSerializer(stateType),
                new ReduceAggregationStateFactory());

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getNameSuffix(), inputType.getTypeSignature(), ImmutableList.of(inputType.getTypeSignature())),
                createInputParameterMetadata(inputType, stateType),
                inputMethodHandle.asType(
                        inputMethodHandle.type()
                                .changeParameterType(1, inputType.getJavaType())
                                .changeParameterType(2, stateType.getJavaType())),
                combineMethodHandle,
                outputMethodHandle,
                ImmutableList.of(stateDescriptor),
                stateType,
                ImmutableList.of(BinaryFunctionInterface.class, BinaryFunctionInterface.class));

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(
                getSignature().getNameSuffix(),
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

    public static void input(Type type, ReduceAggregationState state, Object value, Object initialStateValue, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.getValue() == null) {
            state.setValue(initialStateValue);
        }
        try {
            state.setValue(inputFunction.apply(state.getValue(), value));
        }
        catch (NullPointerException npe) {
            state.setValue(null);
        }
    }

    public static void combine(ReduceAggregationState state, ReduceAggregationState otherState, BinaryFunctionInterface inputFunction, BinaryFunctionInterface combineFunction)
    {
        if (state.getValue() == null) {
            state.setValue(otherState.getValue());
            return;
        }
        try {
            state.setValue(combineFunction.apply(state.getValue(), otherState.getValue()));
        }
        catch (NullPointerException npe) {
            state.setValue(null);
        }
    }

    public static void write(Type type, ReduceAggregationState state, BlockBuilder blockBuilder)
    {
        if (state.getValue() == null) {
            blockBuilder.appendNull();
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(blockBuilder, (long) state.getValue());
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(blockBuilder, (double) state.getValue());
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, (boolean) state.getValue());
        }
        else if (type.getJavaType() == Block.class) {
            type.writeObject(blockBuilder, state.getValue());
        }
        else {
            type.writeSlice(blockBuilder, (Slice) state.getValue());
        }
    }
}
