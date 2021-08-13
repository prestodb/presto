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
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.state.MapUnionSumState;
import com.facebook.presto.operator.aggregation.state.MapUnionSumStateFactory;
import com.facebook.presto.operator.aggregation.state.MapUnionSumStateSerializer;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.function.Signature.nonDecimalNumericTypeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class MapUnionSumAggregation
        extends SqlAggregationFunction
{
    public static final String NAME = "map_union_sum";
    public static final MapUnionSumAggregation MAP_UNION_SUM = new MapUnionSumAggregation();

    private static final MethodHandle INPUT_FUNCTION = methodHandle(MapUnionSumAggregation.class, "input", Type.class, Type.class, MapUnionSumState.class, Block.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MapUnionSumAggregation.class, "combine", MapUnionSumState.class, MapUnionSumState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MapUnionSumAggregation.class, "output", MapUnionSumState.class, BlockBuilder.class);

    public MapUnionSumAggregation()
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("K"), nonDecimalNumericTypeParameter("V")),
                ImmutableList.of(),
                parseTypeSignature("map<K,V>"),
                ImmutableList.of(parseTypeSignature("map<K,V>")));
    }

    @Override
    public String getDescription()
    {
        return "Aggregate all the maps into a single map summing the values for matching keys";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        MapType outputType = (MapType) functionAndTypeManager.getParameterizedType(MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));

        return generateAggregation(keyType, valueType, outputType);
    }

    private static InternalAggregationFunction generateAggregation(Type keyType, Type valueType, MapType outputType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MapUnionSumAggregation.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(outputType);
        MapUnionSumStateSerializer stateSerializer = new MapUnionSumStateSerializer(outputType);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(outputType),
                INPUT_FUNCTION.bindTo(keyType).bindTo(valueType),
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        MapUnionSumState.class,
                        stateSerializer,
                        new MapUnionSumStateFactory(keyType, valueType))),
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), outputType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type inputType)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, inputType));
    }

    public static void input(Type keyType, Type valueType, MapUnionSumState state, Block mapBlock)
    {
        MapUnionSumResult mapUnionSumResult = state.get();
        long startSize;

        if (mapUnionSumResult == null) {
            startSize = 0;
            mapUnionSumResult = MapUnionSumResult.create(keyType, valueType, state.getAdder(), mapBlock);
            state.set(mapUnionSumResult);
        }
        else {
            startSize = mapUnionSumResult.getRetainedSizeInBytes();
            state.set(state.get().unionSum(mapBlock));
        }

        state.addMemoryUsage(mapUnionSumResult.getRetainedSizeInBytes() - startSize);
    }

    public static void combine(MapUnionSumState state, MapUnionSumState otherState)
    {
        if (state.get() == null) {
            state.set(otherState.get());
            return;
        }

        long startSize = state.get().getRetainedSizeInBytes();
        state.set(state.get().unionSum(otherState.get()));
        state.addMemoryUsage(state.get().getRetainedSizeInBytes() - startSize);
    }

    public static void output(MapUnionSumState state, BlockBuilder out)
    {
        MapUnionSumResult mapUnionSumResult = state.get();
        if (mapUnionSumResult == null) {
            out.appendNull();
        }
        else {
            mapUnionSumResult.serialize(out);
        }
    }
}
