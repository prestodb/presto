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
import com.facebook.presto.operator.aggregation.state.KeyValuePairsStateFactory.SingleState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

/**
 * Allows compatible {@link MapType map} columns to be merged using the "{@value #NAME}" aggregate function.
 * <p/>
 * <pre>
 * Example 1:
 * select map_union(foo), map_union(bar) from (
 *  select map(array['a', 'b'], array[1000, 2000]) as foo, cast(null as map<varchar, double>) as bar
 *  union
 *  select map(array['c', 'd'], array[3000, 4000]) as foo, cast(null as map<varchar, double>) as bar
 * );
 *
 * Example 2:
 * select map_union(foo), map_union(bar) from (
 *  select cast(null as map<varchar, bigint>) as foo,
 *         cast(null as map<bigint, double>) as bar
 *  union
 *  select map(array['a', 'b'], array[1000, 2000]) as foo,
 *         map(array[33], array[3300.33]) as bar
 *  union
 *  select map(array['c', 'd'], array[3000, 4000]) as foo,
 *         map(array[44, 55], array[4400.44, 5500.55]) as bar
 *  union
 *  select map(array['e', 'f', 'g', 'h', 'i'], array[50, 60, 70, 80, 90]) as foo,
 *         cast(null as map<bigint, double>) as bar
 *  union
 *  select map(array['j', 'k', 'a', 'b', 'n', 'o', 'p'], array[200, 300, -45, -55, 600, 700, 800]) as foo,
 *         cast(null as map<bigint, double>) as bar
 *  union
 *  select map(array['c', 'd'], array[90000, 120000]) as foo,
 *         map(array[44, 55], array[1100.0, 2200.0]) as bar
 * );
 *
 * Example 3:
 * with
 * part_a as (
 *  select map(array['a', 'b'], array[1000, 2000]) as foo, 'homer' as first_name, 'simpson' as last_name
 * ),
 * part_b as (
 *  select map(array['c', 'd'], array[3000, 4000]) as foo, 'marge' as first_name, 'simpson' as last_name
 * ),
 * part_c as (
 *  select map(array['a', 'd'], array[12, 3]) as foo, 'moe' as first_name, 'szyslak' as last_name
 * ),
 * part_d as (
 *  select cast(null as map<varchar, bigint>) as foo, 'bart' as first_name, 'simpson' as last_name
 * )
 * select last_name, map_union(foo) from (
 *  select * from part_a
 *  union
 *  select * from part_b
 *  union
 *  select * from part_c
 *  union
 *  select * from part_d
 * )
 * group by last_name;
 *
 * Example 4:
 * with
 * part_a as (
 *  select cast(null as map<varchar, bigint>) as foo, 'homer' as first_name, 'simpson' as last_name
 * ),
 * part_b as (
 *  select cast(null as map<varchar, bigint>) as foo, 'marge' as first_name, 'simpson' as last_name
 * ),
 * part_c as (
 *  select cast(null as map<varchar, bigint>) as foo, 'moe' as first_name, 'szyslak' as last_name
 * ),
 * part_d as (
 *  select cast(null as map<varchar, bigint>) as foo, 'bart' as first_name, 'simpson' as last_name
 * )
 * select last_name, map_union(foo) from (
 *  select * from part_a
 *  union
 *  select * from part_b
 *  union
 *  select * from part_c
 *  union
 *  select * from part_d
 * )
 * group by last_name;
 * </pre>
 */
public class MapUnionAggregation
        extends ParametricAggregation
{
    public static final MapUnionAggregation MAP_UNION = new MapUnionAggregation();
    public static final String NAME = "map_union";
    private static final MethodHandle OUTPUT_FUNCTION =
            methodHandle(MapUnionAggregation.class, "output", KeyValuePairsState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION =
            methodHandle(MapUnionAggregation.class, "input",
                    KeyValuePairStateSerializer.class, KeyValuePairsState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION =
            methodHandle(MapUnionAggregation.class, "combine", KeyValuePairsState.class, KeyValuePairsState.class);

    private static final Signature SIGNATURE =
            new Signature(NAME, ImmutableList.of(comparableTypeParameter("K"), typeParameter("V")),
                    "map<K,V>", ImmutableList.of("map<K,V>"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "Merges 2 maps";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager,
                                   FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");
        Signature signature = new Signature(NAME, new MapType(keyType, valueType).getTypeSignature(),
                new MapType(keyType, valueType).getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(keyType, valueType);
        return new FunctionInfo(signature, getDescription(), aggregation);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapUnionAggregation.class.getClassLoader());
        Type outputType = new MapType(keyType, valueType);
        List<Type> inputTypes = ImmutableList.of(outputType);
        KeyValuePairStateSerializer stateSerializer = new KeyValuePairStateSerializer(keyType, valueType, false);
        MethodHandle serializerBoundInputFn = INPUT_FUNCTION.bindTo(stateSerializer);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType, inputTypes),
                createInputParameterMetadata(outputType),
                serializerBoundInputFn,
                null,
                null,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                KeyValuePairsState.class,
                stateSerializer,
                new KeyValuePairsStateFactory(keyType, valueType),
                outputType,
                false);

        GenericAccumulatorFactoryBinder factory =
                new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type inputType)
    {
        return ImmutableList.of(new ParameterMetadata(STATE),
                new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, inputType),
                new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(KeyValuePairStateSerializer serializer,
                             KeyValuePairsState toState, Block value, int position)
    {
        Type keyType = toState.getKeyType();
        Type valueType = toState.getValueType();
        SingleState fromState = new SingleState(keyType, valueType);
        try {
            serializer.deserialize(value, position, fromState);
        }
        catch (ExceededMemoryLimitException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    format("The deserialization of %s may not exceed %s", NAME, e.getMaxMemory()));
        }
        if (fromState.get() != null) {
            combine(toState, fromState);
        }
    }

    public static void combine(KeyValuePairsState state, KeyValuePairsState otherState)
    {
        MapAggregation.combine(state, otherState);
    }

    public static void output(KeyValuePairsState state, BlockBuilder out)
    {
        MapAggregation.output(state, out);
    }
}
