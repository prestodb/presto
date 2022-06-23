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
package com.facebook.presto.operator.aggregation.multimapagg;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.AggregationMetadata;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.GenericAccumulatorFactoryBinder;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.type.TypeUtils.expectedValueSize;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class MultimapAggregationFunction
        extends SqlAggregationFunction
{
    public static final String NAME = "multimap_agg";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MultimapAggregationFunction.class, "output", Type.class, Type.class, MultimapAggregationState.class, BlockBuilder.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MultimapAggregationFunction.class, "combine", MultimapAggregationState.class, MultimapAggregationState.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MultimapAggregationFunction.class, "input", MultimapAggregationState.class, Block.class, Block.class, int.class);
    private static final int EXPECTED_ENTRY_SIZE = 100;
    private final MultimapAggGroupImplementation groupMode;

    public MultimapAggregationFunction(MultimapAggGroupImplementation groupMode)
    {
        super(NAME,
                ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K,array(V))"),
                ImmutableList.of(parseTypeSignature("K"), parseTypeSignature("V")));
        this.groupMode = groupMode;
    }

    @Override
    public String getDescription()
    {
        return "Aggregates all the rows (key/value pairs) into a single multimap";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        Type outputType = functionAndTypeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(new ArrayType(valueType).getTypeSignature())));
        return generateAggregation(keyType, valueType, outputType);
    }

    private InternalAggregationFunction generateAggregation(Type keyType, Type valueType, Type outputType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MultimapAggregationFunction.class.getClassLoader());
        List<Type> inputTypes = ImmutableList.of(keyType, valueType);
        MultimapAggregationStateSerializer stateSerializer = new MultimapAggregationStateSerializer(keyType, valueType);
        Type intermediateType = stateSerializer.getSerializedType();

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(keyType, valueType),
                INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION.bindTo(keyType).bindTo(valueType),
                ImmutableList.of(new AccumulatorStateDescriptor(
                        MultimapAggregationState.class,
                        stateSerializer,
                        new MultimapAggregationStateFactory(keyType, valueType, groupMode))),
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

    public static void input(MultimapAggregationState state, Block key, Block value, int position)
    {
        state.add(key, value, position);
    }

    public static void combine(MultimapAggregationState state, MultimapAggregationState otherState)
    {
        state.merge(otherState);
    }

    public static void output(Type keyType, Type valueType, MultimapAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            // TODO: Avoid copy value block associated with the same key by using strategy similar to multimap_from_entries
            ObjectBigArray<BlockBuilder> valueArrayBlockBuilders = new ObjectBigArray<>();
            valueArrayBlockBuilders.ensureCapacity(state.getEntryCount());
            BlockBuilder distinctKeyBlockBuilder = keyType.createBlockBuilder(null, state.getEntryCount(), expectedValueSize(keyType, 100));
            TypedSet keySet = new TypedSet(keyType, state.getEntryCount(), MultimapAggregationFunction.NAME);

            state.forEach((key, value, keyValueIndex) -> {
                // Merge values of the same key into an array
                if (!keySet.contains(key, keyValueIndex)) {
                    keySet.add(key, keyValueIndex);
                    keyType.appendTo(key, keyValueIndex, distinctKeyBlockBuilder);
                    BlockBuilder valueArrayBuilder = valueType.createBlockBuilder(null, 10, expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));
                    valueArrayBlockBuilders.set(keySet.positionOf(key, keyValueIndex), valueArrayBuilder);
                }
                valueType.appendTo(value, keyValueIndex, valueArrayBlockBuilders.get(keySet.positionOf(key, keyValueIndex)));
            });

            // Write keys and value arrays into one Block
            Type valueArrayType = new ArrayType(valueType);
            BlockBuilder multimapBlockBuilder = out.beginBlockEntry();
            for (int i = 0; i < distinctKeyBlockBuilder.getPositionCount(); i++) {
                keyType.appendTo(distinctKeyBlockBuilder, i, multimapBlockBuilder);
                valueArrayType.writeObject(multimapBlockBuilder, valueArrayBlockBuilders.get(i).build());
            }
            out.closeEntry();
        }
    }
}
