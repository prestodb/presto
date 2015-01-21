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

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.method;
import io.airlift.slice.Slice;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricAggregation;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.state.ArrayAggregationState;
import com.facebook.presto.operator.aggregation.state.ArrayAggregationStateFactory;
import com.facebook.presto.operator.aggregation.state.ArrayAggregationStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;

public class ArrayAggregation
        extends ParametricAggregation
{
    public static final ArrayAggregation ARRAY_AGGREGATION = new ArrayAggregation();
    private static final String NAME = "array_agg";
    private static final Method OUTPUT_FUNCTION = method(ArrayAggregation.class, "output", ArrayAggregationState.class, BlockBuilder.class);
    private static final Method INPUT_FUNCTION = method(ArrayAggregation.class, "input", ArrayAggregationState.class, Block.class, int.class);
    private static final Method COMBINE_FUNCTION = method(ArrayAggregation.class, "combine", ArrayAggregationState.class, ArrayAggregationState.class);
    private static final Signature SIGNATURE = new Signature(NAME, ImmutableList.of(typeParameter("T")), "array<T>", ImmutableList.of("T"), false, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "return an array of values";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type valueType = types.get("T");
        Signature signature = new Signature(NAME,
                parameterizedTypeName("array", valueType.getTypeSignature()),
                valueType.getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(valueType);
        return new FunctionInfo(signature, getDescription(), aggregation.getIntermediateType().getTypeSignature(), aggregation, false);
    }

    private static InternalAggregationFunction generateAggregation(Type valueType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ArrayAggregation.class.getClassLoader());

        ArrayAggregationStateSerializer stateSerializer = new ArrayAggregationStateSerializer();
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(valueType);
        Type outputType = new ArrayType(valueType);
        ArrayAggregationStateFactory stateFactory = new ArrayAggregationStateFactory(new ArrayType(valueType));
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, valueType, inputTypes),
                createInputParameterMetadata(valueType),
                INPUT_FUNCTION,
                null,
                null,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ArrayAggregationState.class,
                stateSerializer,
                stateFactory,
                outputType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, valueType, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(ArrayAggregationState state, Block value, int position)
    {
        Type t = state.getArrayType().getElementType();
        Object v = t.getObjectValue(null /* session */, value, position);
        if (state.getArray() == null) {
            ArrayList<Object> values = new ArrayList<Object>();
            values.add(v);
            state.setArray(values);
        }
        else {
            state.getArray().add(v);
        }
    }

    public static void combine(ArrayAggregationState state, ArrayAggregationState otherState)
    {
        ArrayList<Object> s1 = state.getArray();
        ArrayList<Object> s2 = otherState.getArray();
        if (s1 == null && s2 != null) {
            state.setArray(s2);
        }
        else if (s1 != null) {
            s1.addAll(s2);
        }
    }

    public static void output(ArrayAggregationState state, BlockBuilder out)
    {
        if (state.getArray() == null) {
            out.appendNull();
        }
        else {
            Slice s = ArrayType.toStackRepresentation(state.getArray());
            out.writeBytes(s, 0, s.length());
            out.closeEntry();
        }
    }
}
