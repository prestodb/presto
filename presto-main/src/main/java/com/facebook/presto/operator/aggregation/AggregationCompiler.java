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

import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AggregationCompiler
{
    public AggregationFunction generateAggregationFunction(Class<?> clazz)
    {
        AggregationFunctionMetadata metadata = clazz.getAnnotation(AggregationFunctionMetadata.class);
        checkNotNull(metadata, "AggregationFunctionMetadata annotate missing");

        Class<?> stateClass = getStateClass(clazz);
        AccumulatorStateSerializer<?> stateSerializer = new StateCompiler().generateStateSerializer(stateClass);
        Type intermediateType = stateSerializer.getSerializedType();
        Type outputType = getOutputType(clazz, stateSerializer);
        AccumulatorStateFactory<?> stateFactory = new StateCompiler().generateStateFactory(stateClass);
        // TODO: support approximate aggregations
        AccumulatorFactory factory = new AccumulatorCompiler().generateAccumulatorFactory(clazz, stateClass, intermediateType, outputType, stateSerializer, stateFactory, false);
        // TODO: support un-decomposable aggregations
        return new GenericAggregationFunction(getParameterTypes(clazz), intermediateType, outputType, false, factory);
    }

    private static Type getOutputType(Class<?> clazz, AccumulatorStateSerializer<?> serializer)
    {
        Method outputFunction = CompilerUtils.findPublicStaticMethodWithAnnotation(clazz, OutputFunction.class);
        if (outputFunction == null) {
            return serializer.getSerializedType();
        }
        else {
            return getTypeInstance(outputFunction.getAnnotation(OutputFunction.class).value());
        }
    }

    private static List<Type> getParameterTypes(Class<?> clazz)
    {
        Method inputFunction = CompilerUtils.findPublicStaticMethodWithAnnotation(clazz, InputFunction.class);

        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        Annotation[][] annotations = inputFunction.getParameterAnnotations();
        Class<?>[] parameters = inputFunction.getParameterTypes();
        for (int i = 0; i < parameters.length; i++) {
            Class<?> parameter = parameters[i];
            for (Annotation annotation : annotations[i]) {
                if (annotation instanceof SqlType) {
                    checkArgument(AccumulatorCompiler.SUPPORTED_PARAMETER_TYPES.contains(parameter), "Unsupported type %s", parameter.getSimpleName());
                    builder.add(getTypeInstance(((SqlType) annotation).value()));
                }
            }
        }

        ImmutableList<Type> types = builder.build();
        checkArgument(!types.isEmpty(), "Aggregation has no input parameters");
        return types;
    }

    private static Class<?> getStateClass(Class<?> clazz)
    {
        Method inputFunction = CompilerUtils.findPublicStaticMethodWithAnnotation(clazz, InputFunction.class);
        checkNotNull(inputFunction, "Input function is null");
        checkArgument(inputFunction.getParameterTypes().length > 0, "Input function has no parameters");
        Class<?> stateClass = inputFunction.getParameterTypes()[0];
        checkArgument(AccumulatorState.class.isAssignableFrom(stateClass), "stateClass is not a subclass of AccumulatorState");

        return stateClass;
    }

    private static Type getTypeInstance(Class<?> clazz)
    {
        try {
            return (Type) clazz.getMethod("getInstance").invoke(null);
        }
        catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }
}
