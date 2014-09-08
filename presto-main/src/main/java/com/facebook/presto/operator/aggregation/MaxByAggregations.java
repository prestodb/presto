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
import com.facebook.presto.operator.aggregation.state.MaxByState;
import com.facebook.presto.operator.aggregation.state.MaxByStateFactory;
import com.facebook.presto.operator.aggregation.state.MaxByStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;

public final class MaxByAggregations
{
    private static final String NAME = "max_by";
    private static final Method OUTPUT_FUNCTION;
    private static final Method INPUT_FUNCTION;
    private static final Method COMBINE_FUNCTION;

    static {
        try {
            OUTPUT_FUNCTION = MaxByAggregations.class.getMethod("output", MaxByState.class, BlockBuilder.class);
            INPUT_FUNCTION = MaxByAggregations.class.getMethod("input", MaxByState.class, Block.class, Block.class, int.class);
            COMBINE_FUNCTION = MaxByAggregations.class.getMethod("combine", MaxByState.class, MaxByState.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    private MaxByAggregations() {}

    public static List<InternalAggregationFunction> getAggregations(TypeManager typeManager)
    {
        ImmutableList.Builder<InternalAggregationFunction> builder = ImmutableList.builder();

        Set<Type> orderableTypes = FluentIterable.from(typeManager.getTypes()).filter(new Predicate<Type>() {
            @Override
            public boolean apply(Type input)
            {
                return input.isOrderable();
            }
        }).toSet();

        for (Type keyType : orderableTypes) {
            for (Type valueType : typeManager.getTypes()) {
                builder.add(generateAggregation(valueType, keyType));
            }
        }

        return builder.build();
    }

    private static InternalAggregationFunction generateAggregation(Type valueType, Type keyType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(MaxByAggregations.class.getClassLoader());

        MaxByStateSerializer stateSerializer = new MaxByStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(valueType, keyType);

        MaxByStateFactory stateFactory = new MaxByStateFactory(valueType, keyType);
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, valueType, inputTypes),
                createInputParameterMetadata(valueType, keyType),
                INPUT_FUNCTION,
                null,
                null,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                MaxByState.class,
                stateSerializer,
                stateFactory,
                valueType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new GenericAggregationFunction(NAME, inputTypes, intermediateType, valueType, false, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value, Type key)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_INPUT_CHANNEL, value), new ParameterMetadata(NULLABLE_INPUT_CHANNEL, key), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(MaxByState state, Block value, Block key, int position)
    {
        if (state.getKey() == null || state.getKey().isNull(0)) {
            state.setKey(key.getSingleValueBlock(position));
            state.setValue(value.getSingleValueBlock(position));
        }
        else if (state.getKeyType().compareTo(key, position, state.getKey(), 0) > 0) {
            state.setKey(key.getSingleValueBlock(position));
            state.setValue(value.getSingleValueBlock(position));
        }
    }

    public static void combine(MaxByState state, MaxByState otherState)
    {
        if (state.getKey() == null) {
            state.setKey(otherState.getKey());
            state.setValue(otherState.getValue());
        }
        else if (state.getKeyType().compareTo(otherState.getKey(), 0, state.getKey(), 0) > 0) {
            state.setKey(otherState.getKey());
            state.setValue(otherState.getValue());
        }
    }

    public static void output(MaxByState state, BlockBuilder out)
    {
        if (state.getValue() == null) {
            out.appendNull();
        }
        else {
            state.getValueType().appendTo(state.getValue(), 0, out);
        }
    }
}
