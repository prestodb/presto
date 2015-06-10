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
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricAggregation;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.state.KeyValuePairStateSerializer;
import com.facebook.presto.operator.aggregation.state.KeyValuePairsState;
import com.facebook.presto.operator.aggregation.state.KeyValuePairsStateFactory;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public class Histogram
        extends ParametricAggregation
{
    public static final Histogram HISTOGRAM = new Histogram();
    public static final String NAME = "histogram";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(Histogram.class, "output", KeyValuePairsState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(Histogram.class, "input", KeyValuePairsState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(Histogram.class, "combine", KeyValuePairsState.class, KeyValuePairsState.class);

    private static final Signature SIGNATURE = new Signature(NAME, ImmutableList.of(comparableTypeParameter("K")),
            "map<K,bigint>", ImmutableList.of("K"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "Count the number of times each value occurs";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = BigintType.BIGINT;
        Signature signature = new Signature(NAME, new MapType(keyType, valueType).getTypeSignature(), keyType.getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(keyType, valueType);
        return new FunctionInfo(signature, getDescription(), aggregation);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(Histogram.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(keyType);
        Type outputType = new MapType(keyType, valueType);
        KeyValuePairStateSerializer stateSerializer = new KeyValuePairStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType, inputTypes),
                createInputParameterMetadata(keyType),
                INPUT_FUNCTION,
                null,
                null,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                KeyValuePairsState.class,
                stateSerializer,
                new KeyValuePairsStateFactory(keyType, valueType),
                outputType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type keyType)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(KeyValuePairsState state, Block key, int position)
    {
        KeyValuePairs pairs = state.get();
        if (pairs == null) {
            pairs = new KeyValuePairsForHistogram(state.getKeyType(), state.getValueType());
            state.set(pairs);
        }

        long startSize = pairs.estimatedInMemorySize();
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 1);
        BIGINT.writeLong(builder, 1L);
        try {
            pairs.add(key, builder.build(), position, 0);
        }
        catch (ExceededMemoryLimitException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
        }
        state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
    }

    public static void combine(KeyValuePairsState state, KeyValuePairsState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            Block map = ((KeyValuePairsForHistogram)otherState.get()).getMap();
            KeyValuePairs pairs = state.get();
            long startSize = pairs.estimatedInMemorySize();
            for (int i = 0; i < map.getPositionCount(); i += 2) {
                try {
                    pairs.add(map, map, i, i + 1);
                }
                catch (ExceededMemoryLimitException e) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of histogram may not exceed %s", e.getMaxMemory()));
                }
            }
            state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    public static void output(KeyValuePairsState state, BlockBuilder out)
    {
        KeyValuePairs pairs = state.get();
        if (pairs == null) {
            out.appendNull();
        }
        else {
            Slice slice = pairs.serialize();
            out.writeBytes(slice, 0, slice.length());
            out.closeEntry();
        }
    }
}
