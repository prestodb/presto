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
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.state.ArrayAggregationState;
import com.facebook.presto.operator.aggregation.state.ArrayAggregationStateFactory;
import com.facebook.presto.operator.aggregation.state.ArrayAggregationStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayAggregationFunction
        extends SqlAggregationFunction
{
    private static final String NAME = "array_agg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ArrayAggregationFunction.class, "input", Type.class, ArrayAggregationState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ArrayAggregationFunction.class, "combine", Type.class, ArrayAggregationState.class, ArrayAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ArrayAggregationFunction.class, "output", Type.class, ArrayAggregationState.class, BlockBuilder.class);

    private final boolean legacyArrayAgg;

    public ArrayAggregationFunction(boolean legacyArrayAgg)
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature("T")));
        this.legacyArrayAgg = legacyArrayAgg;
    }

    @Override
    public String getDescription()
    {
        return "return an array of values";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type, legacyArrayAgg);
    }

    private static InternalAggregationFunction generateAggregation(Type type, boolean legacyArrayAgg)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ArrayAggregationFunction.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new ArrayAggregationStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new ArrayAggregationStateFactory();

        List<Type> inputTypes = ImmutableList.of(type);
        Type outputType = new ArrayType(type);
        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type, legacyArrayAgg);

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(outputType);
        Class<? extends AccumulatorState> stateInterface = ArrayAggregationState.class;

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                combineFunction,
                outputFunction,
                stateInterface,
                stateSerializer,
                stateFactory,
                outputType);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value, boolean legacyArrayAgg)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(legacyArrayAgg ? BLOCK_INPUT_CHANNEL : NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, ArrayAggregationState state, Block value, int position)
    {
        BlockBuilder blockBuilder = state.getBlockBuilder();
        if (blockBuilder == null) {
            blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 4);
            state.setBlockBuilder(blockBuilder);
        }
        long startSize = blockBuilder.getRetainedSizeInBytes();
        type.appendTo(value, position, blockBuilder);
        state.addMemoryUsage(blockBuilder.getRetainedSizeInBytes() - startSize);
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
        long startSize = stateBlockBuilder.getRetainedSizeInBytes();
        for (int i = 0; i < otherPositionCount; i++) {
            type.appendTo(otherStateBlockBuilder, i, stateBlockBuilder);
        }
        state.addMemoryUsage(stateBlockBuilder.getRetainedSizeInBytes() - startSize);
    }

    public static void output(Type outputType, ArrayAggregationState state, BlockBuilder out)
    {
        BlockBuilder stateBlockBuilder = state.getBlockBuilder();
        if (stateBlockBuilder == null || stateBlockBuilder.getPositionCount() == 0) {
            out.appendNull();
        }
        else {
            outputType.writeObject(out, stateBlockBuilder.build());
        }
    }
}
