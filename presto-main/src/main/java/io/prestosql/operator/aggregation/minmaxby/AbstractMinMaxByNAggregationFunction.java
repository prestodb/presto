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
package io.prestosql.operator.aggregation.minmaxby;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AbstractMinMaxNAggregationFunction;
import io.prestosql.operator.aggregation.AccumulatorCompiler;
import io.prestosql.operator.aggregation.AggregationMetadata;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.BlockComparator;
import io.prestosql.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.aggregation.TypedKeyValueHeap;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.Signature.orderableTypeParameter;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public abstract class AbstractMinMaxByNAggregationFunction
        extends SqlAggregationFunction
{
    private static final MethodHandle INPUT_FUNCTION = methodHandle(AbstractMinMaxByNAggregationFunction.class, "input", BlockComparator.class, Type.class, Type.class, MinMaxByNState.class, Block.class, Block.class, int.class, long.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AbstractMinMaxByNAggregationFunction.class, "combine", MinMaxByNState.class, MinMaxByNState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AbstractMinMaxByNAggregationFunction.class, "output", ArrayType.class, MinMaxByNState.class, BlockBuilder.class);
    private static final long MAX_NUMBER_OF_VALUES = 10_000;

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
            checkCondition(n <= MAX_NUMBER_OF_VALUES, INVALID_FUNCTION_ARGUMENT, "third argument of max_by/min_by must be less than or equal to %s; found %s", MAX_NUMBER_OF_VALUES, n);
            heap = new TypedKeyValueHeap(comparator, keyType, valueType, toIntExact(n));
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
        BlockBuilder reversedBlockBuilder = elementType.createBlockBuilder(null, heap.getCapacity());
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

        List<AggregationMetadata.ParameterMetadata> inputParameterMetadata = ImmutableList.of(
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
                ImmutableList.of(new AccumulatorStateDescriptor(
                        MinMaxByNState.class,
                        stateSerializer,
                        new MinMaxByNStateFactory())),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(name, inputTypes, ImmutableList.of(intermediateType), outputType, true, false, factory);
    }
}
