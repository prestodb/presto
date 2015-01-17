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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.util.Reflection.method;

public class MinBy
        extends ParametricAggregation
{
    public static final MinBy MIN_BY = new MinBy();
    private static final String NAME = "min_by";
    private static final Method OUTPUT_FUNCTION = method(MinBy.class, "output", MaxOrMinByState.class, BlockBuilder.class);
    private static final Method INPUT_FUNCTION = method(MinBy.class, "input", MaxOrMinByState.class, Block.class, Block.class, int.class);
    private static final Method COMBINE_FUNCTION = method(MinBy.class, "combine", MaxOrMinByState.class, MaxOrMinByState.class);
    private static final Signature SIGNATURE = new Signature(NAME, ImmutableList.of(orderableTypeParameter("K"), typeParameter("V")), "V", ImmutableList.of("V", "K"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "Returns the value of the first argument, associated with the minimum value of the second argument";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");
        Signature signature = new Signature(NAME, valueType.getTypeSignature(), valueType.getTypeSignature(), keyType.getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(valueType, keyType);
        return new FunctionInfo(signature, getDescription(), aggregation.getIntermediateType().getTypeSignature(), aggregation, false);
    }

    private static InternalAggregationFunction generateAggregation(Type valueType, Type keyType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MinBy.class.getClassLoader());

        MaxOrMinByStateSerializer stateSerializer = new MaxOrMinByStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(valueType, keyType);

        MaxOrMinByStateFactory stateFactory = new MaxOrMinByStateFactory(valueType, keyType);
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, valueType, inputTypes),
                createInputParameterMetadata(valueType, keyType),
                INPUT_FUNCTION,
                null,
                null,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
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
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_INPUT_CHANNEL, value), new ParameterMetadata(NULLABLE_INPUT_CHANNEL, key), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(MaxOrMinByState state, Block value, Block key, int position)
    {
        if (state.getKey() == null || state.getKey().isNull(0)) {
            state.setKey(key.getSingleValueBlock(position));
            state.setValue(value.getSingleValueBlock(position));
        }
        else if (state.getKeyType().compareTo(key, position, state.getKey(), 0) < 0) {
            state.setKey(key.getSingleValueBlock(position));
            state.setValue(value.getSingleValueBlock(position));
        }
    }

    public static void combine(MaxOrMinByState state, MaxOrMinByState otherState)
    {
        if (state.getKey() == null) {
            state.setKey(otherState.getKey());
            state.setValue(otherState.getValue());
        }
        else if (state.getKeyType().compareTo(otherState.getKey(), 0, state.getKey(), 0) < 0) {
            state.setKey(otherState.getKey());
            state.setValue(otherState.getValue());
        }
    }

    public static void output(MaxOrMinByState state, BlockBuilder out)
    {
        if (state.getValue() == null) {
            out.appendNull();
        }
        else {
            state.getValueType().appendTo(state.getValue(), 0, out);
        }
    }
}
