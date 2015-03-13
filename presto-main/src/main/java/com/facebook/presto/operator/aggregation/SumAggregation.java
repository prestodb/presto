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
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableDoubleStateSerializer;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.NullableLongStateSerializer;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.util.Reflection.methodHandle;

public class SumAggregation
        extends ParametricAggregation
{
    private static final MethodHandle LONG_INPUT_FUNCTION = methodHandle(SumAggregation.class, "input", NullableLongState.class, long.class);
    private static final MethodHandle DOUBLE_INPUT_FUNCTION = methodHandle(SumAggregation.class, "input", NullableDoubleState.class, double.class);

    public static final SumAggregation SUM_AGGREGATION = new SumAggregation();
    private static final String NAME = "sum";
    private static final Signature SIGNATURE = new Signature(NAME, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("E"), false, false);
    private static final StateCompiler COMPILER = new StateCompiler();

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public String getDescription()
    {
        return "Returns the sum of input arguments";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        Signature signature = new Signature(NAME, type.getTypeSignature(), type.getTypeSignature());
        InternalAggregationFunction aggregation = generateAggregation(type);
        return new FunctionInfo(signature, getDescription(), aggregation.getIntermediateType().getTypeSignature(), aggregation, false);
    }

    public static InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(SumAggregation.class.getClassLoader());

        List<Type> inputTypes = ImmutableList.of(type);

        AccumulatorStateSerializer stateSerializer;
        AccumulatorStateFactory stateFactory;
        MethodHandle inputFunction;
        Class<? extends AccumulatorState> stateInterface;

        if (type.getJavaType() == long.class) {
            stateFactory = COMPILER.generateStateFactory(NullableLongState.class, classLoader);
            stateSerializer = new NullableLongStateSerializer(type);
            stateInterface = NullableLongState.class;
            inputFunction = LONG_INPUT_FUNCTION;
        }
        else if (type.getJavaType() == double.class) {
            stateFactory = COMPILER.generateStateFactory(NullableDoubleState.class, classLoader);
            stateSerializer = new NullableDoubleStateSerializer(type);
            stateInterface = NullableDoubleState.class;
            inputFunction = DOUBLE_INPUT_FUNCTION;
        }
        else {
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "Argument type to sum unsupported");
        }

        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type, inputTypes),
                inputParameterMetadata,
                inputFunction,
                inputParameterMetadata,
                inputFunction,
                null,
                null,
                stateInterface,
                stateSerializer,
                stateFactory,
                type,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, type, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(INPUT_CHANNEL, type));
    }

    public static void input(NullableDoubleState state, double value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(value);
            return;
        }
        state.setNull(false);
        state.setDouble(state.getDouble() + value);
    }

    public static void input(NullableLongState state, long value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(value);
            return;
        }
        state.setNull(false);
        state.setLong(state.getLong() + value);
    }
}
