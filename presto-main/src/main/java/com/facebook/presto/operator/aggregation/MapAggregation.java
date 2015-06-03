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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public class MapAggregation
        extends ParametricAggregation
{
    public static final MapAggregation MAP_AGG = new MapAggregation();
    public static final String NAME = "map_agg";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MapAggregation.class, "output", KeyValuePairsState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MapAggregation.class, "input", KeyValuePairsState.class, Block.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MapAggregation.class, "combine", KeyValuePairsState.class, KeyValuePairsState.class);

    private static final Signature SIGNATURE = new Signature(NAME, ImmutableList.of(comparableTypeParameter("K"), typeParameter("V")),
                                                                   "map<K,V>", ImmutableList.of("K", "V"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "Aggregates all the rows (key/value pairs) into a single map";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");
        Signature signature = new Signature(NAME, new MapType(keyType, valueType).getTypeSignature(), keyType.getTypeSignature(), valueType.getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(keyType, valueType);
        return new FunctionInfo(signature, getDescription(), aggregation);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapAggregation.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(keyType, valueType);
        Type outputType = new MapType(keyType, valueType);
        KeyValuePairStateSerializer stateSerializer = new KeyValuePairStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType, inputTypes),
                createInputParameterMetadata(keyType, valueType),
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

    private static List<ParameterMetadata> createInputParameterMetadata(Type keyType, Type valueType)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                                new ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                                new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, valueType),
                                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(KeyValuePairsState state, Block key, Block value, int position)
    {
        KeyValuePairs pairs = state.get();
        if (pairs == null) {
            pairs = new KeyValuePairs(state.getKeyType(), state.getValueType());
            state.set(pairs);
        }

        long startSize = pairs.estimatedInMemorySize();
        try {
            pairs.add(key, value, position, position);
        }
        catch (ExceededMemoryLimitException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of map_agg may not exceed %s", e.getMaxMemory()));
        }
        state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
    }

    public static void combine(KeyValuePairsState state, KeyValuePairsState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            Block keys = otherState.get().getKeys();
            Block values = otherState.get().getValues();
            KeyValuePairs pairs = state.get();
            long startSize = pairs.estimatedInMemorySize();
            for (int i = 0; i < keys.getPositionCount(); i++) {
                try {
                    pairs.add(keys, values, i, i);
                }
                catch (ExceededMemoryLimitException e) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("The result of map_agg may not exceed %s", e.getMaxMemory()));
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
