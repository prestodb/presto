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
import com.facebook.presto.operator.aggregation.state.KeyValuePairStateSerializer;
import com.facebook.presto.operator.aggregation.state.KeyValuePairsState;
import com.facebook.presto.operator.aggregation.state.KeyValuePairsStateFactory;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MapUnionAggregation
        extends SqlAggregationFunction
{
    public static final MapUnionAggregation MAP_UNION = new MapUnionAggregation();
    public static final String NAME = "map_union";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MapUnionAggregation.class, "output", KeyValuePairsState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MapUnionAggregation.class, "input", Type.class, Type.class, KeyValuePairsState.class, Block.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MapUnionAggregation.class, "combine", KeyValuePairsState.class, KeyValuePairsState.class);

    public MapUnionAggregation()
    {
        super(NAME, ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")), ImmutableList.of(), parseTypeSignature("map<K,V>"), ImmutableList.of(parseTypeSignature("map<K,V>")));
    }

    @Override
    public String getDescription()
    {
        return "Aggregate all the maps into a single map";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        return generateAggregation(keyType, valueType);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapUnionAggregation.class.getClassLoader());
        Type outputType = new MapType(keyType, valueType);
        List<Type> inputTypes = ImmutableList.of(outputType);
        KeyValuePairStateSerializer stateSerializer = new KeyValuePairStateSerializer(keyType, valueType, false);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(outputType),
                INPUT_FUNCTION.bindTo(keyType).bindTo(valueType),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                KeyValuePairsState.class,
                stateSerializer,
                new KeyValuePairsStateFactory(keyType, valueType),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type inputType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, inputType));
    }

    public static void input(Type keyType, Type valueType, KeyValuePairsState state, Block value)
    {
        KeyValuePairs pairs = state.get();
        if (pairs == null) {
            pairs = new KeyValuePairs(keyType, valueType);
            state.set(pairs);
        }

        long startSize = pairs.estimatedInMemorySize();
        for (int i = 0; i < value.getPositionCount(); i += 2) {
            pairs.add(value, value, i, i + 1);
        }
        state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
    }

    public static void combine(KeyValuePairsState state, KeyValuePairsState otherState)
    {
        MapAggregationFunction.combine(state, otherState);
    }

    public static void output(KeyValuePairsState state, BlockBuilder out)
    {
        MapAggregationFunction.output(state, out);
    }
}
