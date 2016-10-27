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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.HistogramState;
import com.facebook.presto.operator.aggregation.state.HistogramStateFactory;
import com.facebook.presto.operator.aggregation.state.HistogramStateSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public class Histogram
        extends SqlAggregationFunction
{
    public static final Histogram HISTOGRAM = new Histogram();
    public static final String NAME = "histogram";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(Histogram.class, "output", Type.class, HistogramState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(Histogram.class, "input", Type.class, HistogramState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(Histogram.class, "combine", HistogramState.class, HistogramState.class);

    public static final int EXPECTED_SIZE_FOR_HASHING = 10;

    public Histogram()
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("K")),
                ImmutableList.of(),
                parseTypeSignature("map(K,bigint)"),
                ImmutableList.of(parseTypeSignature("K")));
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
        Type valueType = BigintType.BIGINT;
        return generateAggregation(keyType, valueType);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(Histogram.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(keyType);
        Type outputType = new MapType(keyType, valueType);
        HistogramStateSerializer stateSerializer = new HistogramStateSerializer(keyType);
        Type intermediateType = stateSerializer.getSerializedType();
        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(keyType);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(outputType);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(keyType),
                inputFunction,
                COMBINE_FUNCTION,
                outputFunction,
                HistogramState.class,
                stateSerializer,
                new HistogramStateFactory(),
                outputType);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, factory);
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
        if (typedHistogram == null) {
            typedHistogram = new TypedHistogram(type, EXPECTED_SIZE_FOR_HASHING);
            state.set(typedHistogram);
        }

        long startSize = typedHistogram.getEstimatedSize();
        try {
            typedHistogram.add(position, key, 1L);
        }
        catch (ExceededMemoryLimitException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
        }
        state.addMemoryUsage(typedHistogram.getEstimatedSize() - startSize);
    }

    public static void combine(HistogramState state, HistogramState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            TypedHistogram typedHistogram = state.get();
            long startSize = typedHistogram.getEstimatedSize();
            try {
                typedHistogram.addAll(otherState.get());
            }
            catch (ExceededMemoryLimitException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
            }
            state.addMemoryUsage(typedHistogram.getEstimatedSize() - startSize);
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    public static void output(Type type, HistogramState state, BlockBuilder out)
    {
        TypedHistogram typedHistogram = state.get();
        if (typedHistogram == null) {
            out.appendNull();
        }
        else {
            Block block = typedHistogram.serialize();
            type.writeObject(out, block);
        }
    }
}
