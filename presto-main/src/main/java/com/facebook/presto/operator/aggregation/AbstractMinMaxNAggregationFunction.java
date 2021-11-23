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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.state.MinMaxNState;
import com.facebook.presto.operator.aggregation.state.MinMaxNStateFactory;
import com.facebook.presto.operator.aggregation.state.MinMaxNStateSerializer;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.orderableTypeParameter;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public abstract class AbstractMinMaxNAggregationFunction
        extends SqlAggregationFunction
{
    private static final MethodHandle INPUT_FUNCTION = methodHandle(AbstractMinMaxNAggregationFunction.class, "input", BlockComparator.class, Type.class, MinMaxNState.class, Block.class, long.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AbstractMinMaxNAggregationFunction.class, "combine", MinMaxNState.class, MinMaxNState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AbstractMinMaxNAggregationFunction.class, "output", ArrayType.class, MinMaxNState.class, BlockBuilder.class);
    private static final long MAX_NUMBER_OF_VALUES = 10_000;

    private final Function<Type, BlockComparator> typeToComparator;

    protected AbstractMinMaxNAggregationFunction(String name, Function<Type, BlockComparator> typeToComparator)
    {
        super(name,
                ImmutableList.of(orderableTypeParameter("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature("E"), parseTypeSignature(StandardTypes.BIGINT)));
        requireNonNull(typeToComparator);
        this.typeToComparator = typeToComparator;
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = boundVariables.getPhysicalType("E");
        return generateAggregation(type);
    }

    protected InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AbstractMinMaxNAggregationFunction.class.getClassLoader());

        BlockComparator comparator = typeToComparator.apply(type);
        List<Type> inputTypes = ImmutableList.of(type, BIGINT);
        MinMaxNStateSerializer stateSerializer = new MinMaxNStateSerializer(comparator, type);
        Type intermediateType = stateSerializer.getSerializedType();
        ArrayType outputType = new ArrayType(type);

        List<ParameterMetadata> inputParameterMetadata = ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(INPUT_CHANNEL, BIGINT),
                new ParameterMetadata(BLOCK_INDEX));

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getNameSuffix(), type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                INPUT_FUNCTION.bindTo(comparator).bindTo(type),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(outputType),
                ImmutableList.of(new AccumulatorStateDescriptor(
                        MinMaxNState.class,
                        stateSerializer,
                        new MinMaxNStateFactory())),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getNameSuffix(), inputTypes, ImmutableList.of(intermediateType), outputType, true, false, factory);
    }

    public static void input(BlockComparator comparator, Type type, MinMaxNState state, Block block, long n, int blockIndex)
    {
        TypedHeap heap = state.getTypedHeap();
        if (heap == null) {
            if (n <= 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "second argument of max_n/min_n must be positive");
            }
            checkCondition(n <= MAX_NUMBER_OF_VALUES, INVALID_FUNCTION_ARGUMENT, "second argument of max_n/min_n must be less than or equal to %s; found %s", MAX_NUMBER_OF_VALUES, n);
            heap = new TypedHeap(comparator, type, toIntExact(n));
            state.setTypedHeap(heap);
        }
        long startSize = heap.getEstimatedSize();
        heap.add(block, blockIndex);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void combine(MinMaxNState state, MinMaxNState otherState)
    {
        TypedHeap otherHeap = otherState.getTypedHeap();
        if (otherHeap == null) {
            return;
        }
        TypedHeap heap = state.getTypedHeap();
        if (heap == null) {
            state.setTypedHeap(otherHeap);
            return;
        }
        long startSize = heap.getEstimatedSize();
        heap.addAll(otherHeap);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);
    }

    public static void output(ArrayType outputType, MinMaxNState state, BlockBuilder out)
    {
        TypedHeap heap = state.getTypedHeap();
        if (heap == null || heap.isEmpty()) {
            out.appendNull();
            return;
        }

        Type elementType = outputType.getElementType();

        BlockBuilder reversedBlockBuilder = elementType.createBlockBuilder(null, heap.getCapacity());
        long startSize = heap.getEstimatedSize();
        heap.popAll(reversedBlockBuilder);
        state.addMemoryUsage(heap.getEstimatedSize() - startSize);

        BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
        for (int i = reversedBlockBuilder.getPositionCount() - 1; i >= 0; i--) {
            elementType.appendTo(reversedBlockBuilder, i, arrayBlockBuilder);
        }
        out.closeEntry();
    }
}
