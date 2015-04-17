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

import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricAggregation;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.ArrayAggregationState;
import com.facebook.presto.operator.aggregation.state.ArrayAggregationStateFactory;
import com.facebook.presto.operator.aggregation.state.ArrayAggregationStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayAggregation
        extends ParametricAggregation
{
    public static final ArrayAggregation ARRAY_AGGREGATION = new ArrayAggregation();
    private static final String NAME = "array_agg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ArrayAggregation.class, "input", Type.class, ArrayAggregationState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ArrayAggregation.class, "combine", Type.class, ArrayAggregationState.class, ArrayAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ArrayAggregation.class, "output", ArrayAggregationState.class, BlockBuilder.class);
    private static final Signature SIGNATURE = new Signature(NAME, ImmutableList.of(typeParameter("T")), "array<T>", ImmutableList.of("T"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "return an array of values";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");
        Signature signature = new Signature(NAME,
                parameterizedTypeName("array", type.getTypeSignature()),
                type.getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(type);
        return new FunctionInfo(signature, getDescription(), aggregation);
    }

    private InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ArrayAggregation.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new ArrayAggregationStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new ArrayAggregationStateFactory();
        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION;
        Class<? extends AccumulatorState> stateInterface = ArrayAggregationState.class;

        List<Type> inputTypes = ImmutableList.of(type);
        Type outputType = new ArrayType(type);
        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type, inputTypes),
                inputParameterMetadata,
                inputFunction,
                null,
                null,
                combineFunction,
                outputFunction,
                stateInterface,
                stateSerializer,
                stateFactory,
                outputType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, ArrayAggregationState state, Block value, int position)
    {
        BlockBuilder blockBuilder = state.getBlockBuilder();
        if (blockBuilder == null) {
            blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus()); // not using type.createBlockBuilder because fixedWidth ones can't be serialized
            state.setBlockBuilder(blockBuilder);
        }
        long startSize = blockBuilder.getSizeInBytes();
        type.appendTo(value, position, blockBuilder);
        state.addMemoryUsage(blockBuilder.getSizeInBytes() - startSize);
    }

    public static void combine(Type type, ArrayAggregationState state, ArrayAggregationState otherState)
    {
        BlockBuilder stateBlockBuilder = state.getBlockBuilder();
        BlockBuilder otherStateBlockBuilder = otherState.getBlockBuilder();
        if (otherStateBlockBuilder == null) {
            return;
        }
        if (stateBlockBuilder == null) {
            state.setBlockBuilder(otherStateBlockBuilder);
            return;
        }
        int otherPositionCount = otherStateBlockBuilder.getPositionCount();
        long startSize = stateBlockBuilder.getSizeInBytes();
        for (int i = 0; i < otherPositionCount; i++) {
            type.appendTo(otherStateBlockBuilder, i, stateBlockBuilder);
        }
        state.addMemoryUsage(stateBlockBuilder.getSizeInBytes() - startSize);
    }

    public static void output(ArrayAggregationState state, BlockBuilder out)
    {
        BlockBuilder stateBlockBuilder = state.getBlockBuilder();
        if (stateBlockBuilder == null || stateBlockBuilder.getPositionCount() == 0) {
            out.appendNull();
        }
        else {
            VARBINARY.writeSlice(out, buildStructuralSlice(stateBlockBuilder));
        }
    }
}
