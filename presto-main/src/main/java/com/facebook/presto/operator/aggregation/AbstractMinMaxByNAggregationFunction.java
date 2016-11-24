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
import com.facebook.presto.operator.aggregation.state.MinMaxByNState;
import com.facebook.presto.operator.aggregation.state.MinMaxByNStateFactory;
import com.facebook.presto.operator.aggregation.state.MinMaxByNStateSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public abstract class AbstractMinMaxByNAggregationFunction
        extends SqlAggregationFunction
{
    private static final MethodHandle INPUT_FUNCTION = methodHandle(AbstractMinMaxByNAggregationFunction.class, "input", BlockComparator.class, Type.class, Type.class, MinMaxByNState.class, Block.class, Block.class, int.class, long.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AbstractMinMaxByNAggregationFunction.class, "combine", MinMaxByNState.class, MinMaxByNState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AbstractMinMaxByNAggregationFunction.class, "output", ArrayType.class, MinMaxByNState.class, BlockBuilder.class);

    private final String name;
    private final Function<Type, BlockComparator> typeToComparator;

    protected AbstractMinMaxByNAggregationFunction(String name, Function<Type, BlockComparator> typeToComparator)
    {
        super(name,
                ImmutableList.of(typeVariable("V"), orderableTypeParameter("K")),
                ImmutableList.of(),
                parseTypeSignature("array(V)"),
                ImmutableList.of(parseTypeSignature("V"), parseTypeSignature("K"), parseTypeSignature(StandardTypes.BIGINT)));
        this.name = requireNonNull(name, "name is null");
        this.typeToComparator = requireNonNull(typeToComparator, "typeToComparator is null");
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        return generateAggregation(valueType, keyType);
    }

    public static void input(BlockComparator comparator, Type valueType, Type keyType, MinMaxByNState state, Block value, Block key, int blockIndex, long n)
    {
        TypedKeyValueHeap heap = state.getTypedKeyValueHeap();
        if (heap == null) {
            if (n <= 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "third argument of max_by/min_by must be a positive integer");
            }
            heap = new TypedKeyValueHeap(comparator, keyType, valueType, Ints.checkedCast(n));
            state.setTypedKeyValueHeap(heap);
        }

        long startSize = heap.getEstimatedSize();
        if (!key.isNull(blockIndex)) {
            heap.add(key, value, blockIndex);
        }
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void combine(MinMaxByNState state, MinMaxByNState otherState)
    {
        TypedKeyValueHeap otherHeap = otherState.getTypedKeyValueHeap();
        if (otherHeap == null) {
            return;
        }
        TypedKeyValueHeap heap = state.getTypedKeyValueHeap();
        if (heap == null) {
            state.setTypedKeyValueHeap(otherHeap);
            return;
        }
        long startSize = heap.getEstimatedSize();
        heap.addAll(otherHeap);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void output(ArrayType outputType, MinMaxByNState state, BlockBuilder out)
    {
        TypedKeyValueHeap heap = state.getTypedKeyValueHeap();
        if (heap == null || heap.isEmpty()) {
            out.appendNull();
            return;
        }

        Type elementType = outputType.getElementType();

        BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
        BlockBuilder reversedBlockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), heap.getCapacity());
        long startSize = heap.getEstimatedSize();
        heap.popAll(reversedBlockBuilder);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);

        for (int i = reversedBlockBuilder.getPositionCount() - 1; i >= 0; i--) {
            elementType.appendTo(reversedBlockBuilder, i, arrayBlockBuilder);
        }
        out.closeEntry();
    }

    protected InternalAggregationFunction generateAggregation(Type valueType, Type keyType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AbstractMinMaxNAggregationFunction.class.getClassLoader());

        BlockComparator comparator = typeToComparator.apply(keyType);
        List<Type> inputTypes = ImmutableList.of(valueType, keyType, BIGINT);
        MinMaxByNStateSerializer stateSerializer = new MinMaxByNStateSerializer(comparator, keyType, valueType);
        Type intermediateType = stateSerializer.getSerializedType();
        ArrayType outputType = new ArrayType(valueType);

        List<AggregationMetadata.ParameterMetadata> inputParameterMetadata =  ImmutableList.of(
                new AggregationMetadata.ParameterMetadata(STATE),
                new AggregationMetadata.ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, valueType),
                new AggregationMetadata.ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                new AggregationMetadata.ParameterMetadata(BLOCK_INDEX),
                new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, BIGINT));

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(name, valueType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                INPUT_FUNCTION.bindTo(comparator).bindTo(valueType).bindTo(keyType),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(outputType),
                MinMaxByNState.class,
                stateSerializer,
                new MinMaxByNStateFactory(),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(name, inputTypes, intermediateType, outputType, true, factory);
    }
}
