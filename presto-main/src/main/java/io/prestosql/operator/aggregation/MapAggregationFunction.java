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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.state.KeyValuePairStateSerializer;
import io.prestosql.operator.aggregation.state.KeyValuePairsState;
import io.prestosql.operator.aggregation.state.KeyValuePairsStateFactory;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;

public class MapAggregationFunction
        extends SqlAggregationFunction
{
    public static final MapAggregationFunction MAP_AGG = new MapAggregationFunction();
    public static final String NAME = "map_agg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MapAggregationFunction.class, "input", Type.class, Type.class, KeyValuePairsState.class, Block.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MapAggregationFunction.class, "combine", KeyValuePairsState.class, KeyValuePairsState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MapAggregationFunction.class, "output", KeyValuePairsState.class, BlockBuilder.class);

    public MapAggregationFunction()
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V)"),
                ImmutableList.of(parseTypeSignature("K"), parseTypeSignature("V")));
    }

    @Override
    public String getDescription()
    {
        return "Aggregates all the rows (key/value pairs) into a single map";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        MapType outputType = (MapType) typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
        return generateAggregation(keyType, valueType, outputType);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType, MapType outputType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(keyType, valueType);
        KeyValuePairStateSerializer stateSerializer = new KeyValuePairStateSerializer(outputType);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(keyType, valueType),
                INPUT_FUNCTION.bindTo(keyType).bindTo(valueType),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        KeyValuePairsState.class,
                        stateSerializer,
                        new KeyValuePairsStateFactory(keyType, valueType))),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type keyType, Type valueType)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                new ParameterMetadata(BLOCK_INPUT_CHANNEL, keyType),
                new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, valueType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type keyType, Type valueType, KeyValuePairsState state, Block key, Block value, int position)
    {
        KeyValuePairs pairs = state.get();
        if (pairs == null) {
            pairs = new KeyValuePairs(keyType, valueType);
            state.set(pairs);
        }

        long startSize = pairs.estimatedInMemorySize();
        pairs.add(key, value, position, position);
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
                pairs.add(keys, values, i, i);
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
            pairs.serialize(out);
        }
    }
}
