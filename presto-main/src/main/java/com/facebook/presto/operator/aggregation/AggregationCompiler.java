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
import com.google.common.base.CaseFormat;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AggregationCompiler
{
    private static List<Method> findPublicStaticMethodsWithAnnotation(Class<?> clazz, Class<?> annotationClass)
    {
        ImmutableList.Builder<Method> methods = ImmutableList.builder();
        for (Method method : clazz.getMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                if (annotationClass.isInstance(annotation)) {
                    checkArgument(Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers()), "%s annotated with %s must be static and public", method.getName(), annotationClass.getSimpleName());
                    methods.add(method);
                }
            }
        }
        return methods.build();
    }

    public InternalAggregationFunction generateAggregationFunction(Class<?> clazz)
    {
        List<InternalAggregationFunction> aggregations = generateAggregationFunctions(clazz);
        checkArgument(aggregations.size() == 1, "More than one aggregation function found");
        return aggregations.get(0);
    }

    public InternalAggregationFunction generateAggregationFunction(Class<?> clazz, Type returnType, List<Type> argumentTypes)
    {
        checkNotNull(returnType, "returnType is null");
        checkNotNull(argumentTypes, "argumentTypes is null");
        for (InternalAggregationFunction aggregation : generateAggregationFunctions(clazz)) {
            if (aggregation.getFinalType() == returnType && aggregation.getParameterTypes().equals(argumentTypes)) {
                return aggregation;
            }
        }
        throw new IllegalArgumentException(String.format("No method with return type %s and arguments %s", returnType, argumentTypes));
    }

    public List<InternalAggregationFunction> generateAggregationFunctions(Class<?> clazz)
    {
        AggregationFunction aggregationAnnotation = clazz.getAnnotation(AggregationFunction.class);
        ApproximateAggregationFunction approximateAnnotation = clazz.getAnnotation(ApproximateAggregationFunction.class);
        checkArgument(aggregationAnnotation != null || approximateAnnotation != null, "Aggregation function annotation is missing");
        checkArgument(aggregationAnnotation == null || approximateAnnotation == null, "Aggregation function cannot be exact and approximate");

        ImmutableList.Builder<InternalAggregationFunction> builder = ImmutableList.builder();
        for (Class<?> stateClass : getStateClasses(clazz)) {
            AccumulatorStateSerializer<?> stateSerializer = new StateCompiler().generateStateSerializer(stateClass);
            Type intermediateType = stateSerializer.getSerializedType();
            Method intermediateInputFunction = getIntermediateInputFunction(clazz, stateClass);
            Method combineFunction = getCombineFunction(clazz, stateClass);
            AccumulatorStateFactory<?> stateFactory = new StateCompiler().generateStateFactory(stateClass);

            for (Method outputFunction : getOutputFunctions(clazz, stateClass)) {
                for (Method inputFunction : getInputFunctions(clazz, stateClass)) {
                    List<Type> inputTypes = getInputTypes(inputFunction);
                    Type outputType = getOutputType(outputFunction, stateSerializer);
                    String name = getName(outputFunction, aggregationAnnotation, approximateAnnotation);

                    StringBuilder sb = new StringBuilder();
                    sb.append(outputType.getName());
                    for (Type inputType : inputTypes) {
                        sb.append(inputType.getName());
                    }
                    sb.append(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name.toLowerCase()));

                    AccumulatorFactory factory = new AccumulatorCompiler().generateAccumulatorFactory(
                            sb.toString(),
                            inputFunction,
                            intermediateInputFunction,
                            combineFunction,
                            outputFunction,
                            stateClass,
                            intermediateType,
                            outputType,
                            stateSerializer,
                            stateFactory,
                            approximateAnnotation != null);
                    // TODO: support un-decomposable aggregations
                    builder.add(new GenericAggregationFunction(name, inputTypes, intermediateType, outputType, false, approximateAnnotation != null, factory));
                }
            }
        }

        return builder.build();
    }

    private static String getName(@Nullable Method outputFunction, AggregationFunction aggregationAnnotation, ApproximateAggregationFunction approximateAnnotation)
    {
        String defaultName;
        if (aggregationAnnotation != null) {
            defaultName = aggregationAnnotation.value();
        }
        else {
            defaultName = approximateAnnotation.value();
        }

        if (outputFunction == null) {
            return defaultName;
        }

        AggregationFunction annotation = outputFunction.getAnnotation(AggregationFunction.class);
        if (annotation == null) {
            return defaultName;
        }
        else {
            return annotation.value();
        }
    }

    private static Method getIntermediateInputFunction(Class<?> clazz, Class<?> stateClass)
    {
        for (Method method : findPublicStaticMethodsWithAnnotation(clazz, IntermediateInputFunction.class)) {
            if (method.getParameterTypes()[0] == stateClass) {
                return method;
            }
        }
        return null;
    }

    private static Method getCombineFunction(Class<?> clazz, Class<?> stateClass)
    {
        for (Method method : findPublicStaticMethodsWithAnnotation(clazz, CombineFunction.class)) {
            if (method.getParameterTypes()[0] == stateClass) {
                return method;
            }
        }
        return null;
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, final Class<?> stateClass)
    {
        ImmutableList<Method> methods = FluentIterable.from(findPublicStaticMethodsWithAnnotation(clazz, OutputFunction.class))
                .filter(new Predicate<Method>()
                {
                    @Override
                    public boolean apply(Method method)
                    {
                        // Filter out methods that don't match this state class
                        return method.getParameterTypes()[0] == stateClass;
                    }
                }).toList();
        if (methods.isEmpty()) {
            List<Method> noOutputFunction = new ArrayList<>();
            noOutputFunction.add(null);
            return noOutputFunction;
        }
        return methods;
    }

    private static Type getOutputType(Method outputFunction, AccumulatorStateSerializer<?> serializer)
    {
        if (outputFunction == null) {
            return serializer.getSerializedType();
        }
        else {
            return getTypeInstance(outputFunction.getAnnotation(OutputFunction.class).value());
        }
    }

    private static List<Method> getInputFunctions(Class<?> clazz, final Class<?> stateClass)
    {
        List<Method> inputFunctions = FluentIterable.from(findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class))
                .filter(new Predicate<Method>()
                {
                    @Override
                    public boolean apply(Method method)
                    {
                        // Filter out methods that don't match this state class
                        return method.getParameterTypes()[0] == stateClass;
                    }
                }).toList();
        checkArgument(!inputFunctions.isEmpty(), "Aggregation has no input functions");
        return inputFunctions;
    }

    private static List<Type> getInputTypes(Method inputFunction)
    {
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        Annotation[][] parameterAnnotations = inputFunction.getParameterAnnotations();
        for (Annotation[] annotations : parameterAnnotations) {
            for (Annotation annotation : annotations) {
                if (annotation instanceof SqlType) {
                    builder.add(getTypeInstance(((SqlType) annotation).value()));
                }
            }
        }

        ImmutableList<Type> types = builder.build();
        checkArgument(!types.isEmpty(), "Input function has no parameters");
        return types;
    }

    private static Set<Class<?>> getStateClasses(Class<?> clazz)
    {
        ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();
        for (Method inputFunction : findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class)) {
            checkArgument(inputFunction.getParameterTypes().length > 0, "Input function has no parameters");
            Class<?> stateClass = inputFunction.getParameterTypes()[0];
            checkArgument(AccumulatorState.class.isAssignableFrom(stateClass), "stateClass is not a subclass of AccumulatorState");
            builder.add(stateClass);
        }
        ImmutableSet<Class<?>> stateClasses = builder.build();
        checkArgument(!stateClasses.isEmpty(), "No input functions found");

        return stateClasses;
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
