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
import com.facebook.presto.operator.aggregation.state.ArbitraryAggregationState;
import com.facebook.presto.operator.aggregation.state.ArbitraryAggregationStateFactory;
import com.facebook.presto.operator.aggregation.state.ArbitraryAggregationStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArbitraryAggregation
        extends ParametricAggregation
{
    public static final ArbitraryAggregation ARBITRARY_AGGREGATION = new ArbitraryAggregation();
    private static final String NAME = "arbitrary";
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ArbitraryAggregation.class, "output", ArbitraryAggregationState.class, BlockBuilder.class);
    private static final MethodHandle INPUT_FUNCTION = methodHandle(ArbitraryAggregation.class, "input", ArbitraryAggregationState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ArbitraryAggregation.class, "combine", ArbitraryAggregationState.class, ArbitraryAggregationState.class);
    private static final Signature SIGNATURE = new Signature(NAME, ImmutableList.of(typeParameter("T")), "T", ImmutableList.of("T"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "return an arbitrary non-null input value";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type valueType = types.get("T");
        Signature signature = new Signature(NAME, valueType.getTypeSignature(), valueType.getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(valueType);
        return new FunctionInfo(signature, getDescription(), aggregation);
    }

    private static InternalAggregationFunction generateAggregation(Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ArbitraryAggregation.class.getClassLoader());

        ArbitraryAggregationStateSerializer stateSerializer = new ArbitraryAggregationStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(valueType);

        ArbitraryAggregationStateFactory stateFactory = new ArbitraryAggregationStateFactory(valueType);
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, valueType, inputTypes),
                createInputParameterMetadata(valueType),
                INPUT_FUNCTION,
                null,
                null,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ArbitraryAggregationState.class,
                stateSerializer,
                stateFactory,
                valueType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, valueType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(ArbitraryAggregationState state, Block value, int position)
    {
        if (state.getValue() == null) {
            state.setValue(value.getSingleValueBlock(position));
        }
    }

    public static void combine(ArbitraryAggregationState state, ArbitraryAggregationState otherState)
    {
        if (state.getValue() == null && otherState.getValue() != null) {
            state.setValue(otherState.getValue());
        }
    }

    public static void output(ArbitraryAggregationState state, BlockBuilder out)
    {
        if (state.getValue() == null) {
            out.appendNull();
        }
        else {
            state.getType().appendTo(state.getValue(), 0, out);
        }
    }
}
