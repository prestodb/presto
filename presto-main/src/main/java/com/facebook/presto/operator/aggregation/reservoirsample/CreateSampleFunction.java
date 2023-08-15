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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.max;

public class CreateSampleFunction
        extends SqlAggregationFunction
{
    public static final CreateSampleFunction RESERVOIR_SAMPLE = new CreateSampleFunction();
    private static final String NAME = "reservoir_sample";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(CreateSampleFunction.class, "input", Type.class, ReservoirSampleState.class, Block.class, int.class, long.class, Block.class, int.class, long.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(CreateSampleFunction.class, "combine", Type.class, ReservoirSampleState.class, ReservoirSampleState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(CreateSampleFunction.class, "output", Type.class, ReservoirSampleState.class, BlockBuilder.class);

    protected CreateSampleFunction()
    {
        super(NAME, ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("row(count bigint, sample array(T))"),
                // initial sample array (scalar value), initial seen count (scalar value), new set of samples to insert, reservoir size (scalar)
                ImmutableList.of(parseTypeSignature("array(T)"), BIGINT.getTypeSignature(), parseTypeSignature("T"), BIGINT.getTypeSignature()));
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(CreateSampleFunction.class.getClassLoader());
        AccumulatorStateSerializer<?> stateSerializer = new ReservoirSampleStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new ReservoirSampleStateFactory(type);
        List<Type> inputTypes = ImmutableList.of(new ArrayType(type), BIGINT, type, BIGINT);
        Type outputType = createOutputType(type);
        Type intermediateType = stateSerializer.getSerializedType();
        List<AggregationMetadata.ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(type);
        Class<? extends AccumulatorState> stateInterface = ReservoirSampleState.class;

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                combineFunction,
                outputFunction,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                outputType);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);

        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType), outputType,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static Type createOutputType(Type type)
    {
        RowType.Field count = new RowType.Field(Optional.of("count"), BIGINT);
        RowType.Field sample = new RowType.Field(Optional.of("sample"), new ArrayType(type));
        List<RowType.Field> fields = Arrays.asList(count, sample);
        return RowType.from(fields);
    }

    private static List<AggregationMetadata.ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(
                new AggregationMetadata.ParameterMetadata(STATE),
                new AggregationMetadata.ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type),
                new AggregationMetadata.ParameterMetadata(BLOCK_INDEX),
                new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, BIGINT),
                new AggregationMetadata.ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type),
                new AggregationMetadata.ParameterMetadata(BLOCK_INDEX),
                new AggregationMetadata.ParameterMetadata(INPUT_CHANNEL, BIGINT));
    }

    public static void input(Type type, ReservoirSampleState state, Block initialState, int pos, long seenCount, Block value, int position, long n)
    {
        state.add(value, position, n);
        state.initializeInitialSample(initialState, seenCount);
    }

    public static void combine(Type type, ReservoirSampleState state, ReservoirSampleState otherState)
    {
        state.mergeWith(otherState);
    }

    public static void output(Type elementType, ReservoirSampleState state, BlockBuilder out)
    {
        ReservoirSample reservoirSample = state.getSamples();
        Block initialSampleBlock = state.getInitialSample();
        long initialSeenCount = state.getInitialSeenCount();
        // merge the final state with the initial state given
        List<Block> samples = IntStream.range(0, initialSampleBlock.getPositionCount())
                .mapToObj(initialSampleBlock::getSingleValueBlock)
                .collect(Collectors.toList());
        if (initialSeenCount != -1 && initialSeenCount != initialSampleBlock.getPositionCount()) {
            checkArgument(reservoirSample.getMaxSampleSize() == initialSampleBlock.getPositionCount(), "initial sample size must be equal to the sample size");
        }
        ReservoirSample finalSample = new ReservoirSample(elementType, max(initialSeenCount, 0), reservoirSample.getMaxSampleSize(), new ArrayList<>(samples));
        finalSample.merge(reservoirSample);
        Type arrayType = new ArrayType(elementType);
        long count = finalSample.getSeenCount();
        BlockBuilder entryBuilder = out.beginBlockEntry();
        BIGINT.writeLong(entryBuilder, count);
        BlockBuilder sampleBlock = finalSample.getSampleBlockBuilder();
        arrayType.appendTo(sampleBlock.build(), 0, entryBuilder);
        out.closeEntry();
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type);
    }

    @Override
    public String getDescription()
    {
        return "return an array of values";
    }
}
