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
package io.prestosql.operator.aggregation.histogram;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AccumulatorCompiler;
import io.prestosql.operator.aggregation.AggregationMetadata;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class Histogram
        extends SqlAggregationFunction
{
    public static final String NAME = "histogram";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(Histogram.class, "output", Type.class, HistogramState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(Histogram.class, "input", Type.class, HistogramState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(Histogram.class, "combine", HistogramState.class, HistogramState.class);

    public static final int EXPECTED_SIZE_FOR_HASHING = 10;
    private final HistogramGroupImplementation groupMode;

    public Histogram(HistogramGroupImplementation groupMode)
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("K")),
                ImmutableList.of(),
                parseTypeSignature("map(K,bigint)"),
                ImmutableList.of(parseTypeSignature("K")));
        this.groupMode = groupMode;
    }

    @Override
    public String getDescription()
    {
        return "Count the number of times each value occurs";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type outputType = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(BIGINT.getTypeSignature())));
        return generateAggregation(NAME, keyType, outputType, groupMode);
    }

    private static InternalAggregationFunction generateAggregation(
            String functionName,
            Type keyType,
            Type outputType,
            HistogramGroupImplementation groupMode)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(Histogram.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(keyType);
        HistogramStateSerializer stateSerializer = new HistogramStateSerializer(keyType, outputType);
        Type intermediateType = stateSerializer.getSerializedType();
        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(keyType);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(outputType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(functionName, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(keyType),
                inputFunction,
                COMBINE_FUNCTION,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        HistogramState.class,
                        stateSerializer,
                        new HistogramStateFactory(keyType, EXPECTED_SIZE_FOR_HASHING, groupMode))),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(functionName, inputTypes, ImmutableList.of(intermediateType), outputType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type keyType)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, HistogramState state, Block key, int position)
    {
        TypedHistogram typedHistogram = state.get();
        long startSize = typedHistogram.getEstimatedSize();
        typedHistogram.add(position, key, 1L);
        state.addMemoryUsage(typedHistogram.getEstimatedSize() - startSize);
    }

    public static void combine(HistogramState state, HistogramState otherState)
    {
        // NOTE: state = current merged state; otherState = scratchState (new data to be added)
        // for grouped histograms and single histograms, we have a single histogram object. In neither case, can otherState.get() return null.
        // Semantically, a histogram object will be returned even if the group is empty.
        // In that case, the histogram object will represent an empty histogram until we call add() on
        // it.
        requireNonNull(otherState.get(), "scratch state should always be non-null");
        TypedHistogram typedHistogram = state.get();
        long startSize = typedHistogram.getEstimatedSize();
        typedHistogram.addAll(otherState.get());
        state.addMemoryUsage(typedHistogram.getEstimatedSize() - startSize);
    }

    public static void output(Type type, HistogramState state, BlockBuilder out)
    {
        TypedHistogram typedHistogram = state.get();
        typedHistogram.serialize(out);
    }
}
