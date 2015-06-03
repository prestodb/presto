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

import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricAggregation;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.state.MaxOrMinByState;
import com.facebook.presto.operator.aggregation.state.MaxOrMinByStateFactory;
import com.facebook.presto.operator.aggregation.state.MaxOrMinByStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MaxBy
        extends ParametricAggregation
{
    public static final MaxBy MAX_BY = new MaxBy();
    private static final String NAME = "max_by";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MaxBy.class, "output", Type.class, MaxOrMinByState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MaxBy.class, "input", Type.class, MaxOrMinByState.class, Block.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MaxBy.class, "combine", Type.class, MaxOrMinByState.class, MaxOrMinByState.class);
    private static final Signature SIGNATURE = new Signature(NAME, ImmutableList.of(orderableTypeParameter("K"), typeParameter("V")), "V", ImmutableList.of("V", "K"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "Returns the value of the first argument, associated with the maximum value of the second argument";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");
        Signature signature = new Signature(NAME, valueType.getTypeSignature(), valueType.getTypeSignature(), keyType.getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(valueType, keyType);
        return new FunctionInfo(signature, getDescription(), aggregation);
    }

    private static InternalAggregationFunction generateAggregation(Type valueType, Type keyType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MaxBy.class.getClassLoader());

        MaxOrMinByStateSerializer stateSerializer = new MaxOrMinByStateSerializer(valueType, keyType);
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(valueType, keyType);

        MaxOrMinByStateFactory stateFactory = new MaxOrMinByStateFactory();
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, valueType, inputTypes),
                createInputParameterMetadata(valueType, keyType),
                INPUT_FUNCTION.bindTo(keyType),
                null,
                null,
                COMBINE_FUNCTION.bindTo(keyType),
                OUTPUT_FUNCTION.bindTo(valueType),
                MaxOrMinByState.class,
                stateSerializer,
                stateFactory,
                valueType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, valueType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value, Type key)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, key), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type keyType, MaxOrMinByState state, Block value, Block key, int position)
    {
        if (state.getKey() == null || state.getKey().isNull(0)) {
            state.setKey(key.getSingleValueBlock(position));
            state.setValue(value.getSingleValueBlock(position));
        }
        else if (keyType.compareTo(key, position, state.getKey(), 0) > 0) {
            state.setKey(key.getSingleValueBlock(position));
            state.setValue(value.getSingleValueBlock(position));
        }
    }

    public static void combine(Type keyType, MaxOrMinByState state, MaxOrMinByState otherState)
    {
        if (state.getKey() == null) {
            state.setKey(otherState.getKey());
            state.setValue(otherState.getValue());
        }
        else if (otherState.getKey() != null && keyType.compareTo(otherState.getKey(), 0, state.getKey(), 0) > 0) {
            state.setKey(otherState.getKey());
            state.setValue(otherState.getValue());
        }
    }

    public static void output(Type valueType, MaxOrMinByState state, BlockBuilder out)
    {
        if (state.getValue() == null) {
            out.appendNull();
        }
        else {
            valueType.appendTo(state.getValue(), 0, out);
        }
    }
}
