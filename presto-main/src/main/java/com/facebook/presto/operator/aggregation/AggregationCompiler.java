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
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AggregationCompiler
{
    private AggregationCompiler()
    {
    }

    // This function should only be used for function matching for testing purposes.
    // General purpose function matching is done through FunctionRegistry.
    @VisibleForTesting
    public static BindableAggregationFunction generateAggregationBindableFunction(Class<?> clazz)
    {
        List<BindableAggregationFunction> aggregations = generateBindableAggregationFunctions(clazz);
        checkArgument(aggregations.size() == 1, "More than one aggregation function found");
        return aggregations.get(0);
    }

    // This function should only be used for function matching for testing purposes.
    // General purpose function matching is done through FunctionRegistry.
    public static BindableAggregationFunction generateAggregationBindableFunction(Class<?> clazz, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        for (BindableAggregationFunction aggregation : generateBindableAggregationFunctions(clazz)) {
            if (aggregation.getSignature().getReturnType().equals(returnType) &&
                    aggregation.getSignature().getArgumentTypes().equals(argumentTypes)) {
                return aggregation;
            }
        }
        throw new IllegalArgumentException(String.format("No method with return type %s and arguments %s", returnType, argumentTypes));
    }

    public static List<BindableAggregationFunction> generateBindableAggregationFunctions(Class<?> aggregationDefinition)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");

        DynamicClassLoader classLoader = new DynamicClassLoader(aggregationDefinition.getClassLoader());

        ImmutableList.Builder<BindableAggregationFunction> builder = ImmutableList.builder();

        for (Class<?> stateClass : getStateClasses(aggregationDefinition)) {
            AccumulatorStateSerializer<?> stateSerializer = new StateCompiler().generateStateSerializer(stateClass, classLoader);

            for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateClass)) {
                for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                    for (String name : getNames(outputFunction, aggregationAnnotation)) {
                        List<TypeSignature> inputTypes = getInputTypesSignatures(inputFunction);
                        TypeSignature outputType = TypeSignature.parseTypeSignature(outputFunction.getAnnotation(OutputFunction.class).value());

                        builder.add(
                                new BindableAggregationFunction(
                                        new Signature(
                                                name,
                                                FunctionKind.AGGREGATE,
                                                ImmutableList.of(), // TODO parse constrains from annotations
                                                ImmutableList.of(), // TODO parse constrains from annotations
                                                outputType,
                                                inputTypes,
                                                false),
                                        getDescription(aggregationDefinition, outputFunction),
                                        aggregationAnnotation.decomposable(),
                                        aggregationDefinition,
                                        stateClass,
                                        inputFunction,
                                        outputFunction));
                    }
                }
            }
        }

        return builder.build();
    }

    public static boolean isParameterNullable(Annotation[] annotations)
    {
        return Arrays.asList(annotations).stream().anyMatch(annotation -> annotation instanceof NullablePosition);
    }

    public static boolean isParameterBlock(Annotation[] annotations)
    {
        return Arrays.asList(annotations).stream().anyMatch(annotation -> annotation instanceof BlockPosition);
    }

    private static List<String> getNames(@Nullable Method outputFunction, AggregationFunction aggregationAnnotation)
    {
        List<String> defaultNames = ImmutableList.<String>builder().add(aggregationAnnotation.value()).addAll(Arrays.asList(aggregationAnnotation.alias())).build();

        if (outputFunction == null) {
            return defaultNames;
        }

        AggregationFunction annotation = outputFunction.getAnnotation(AggregationFunction.class);
        if (annotation == null) {
            return defaultNames;
        }
        else {
            return ImmutableList.<String>builder().add(annotation.value()).addAll(Arrays.asList(annotation.alias())).build();
        }
    }

    public static Method getCombineFunction(Class<?> clazz, Class<?> stateClass)
    {
        for (Method method : findPublicStaticMethodsWithAnnotation(clazz, CombineFunction.class)) {
            if (method.getParameterTypes()[0] == stateClass) {
                return method;
            }
        }
        throw new IllegalArgumentException(String.format("No method with @CombineFunction annotation found in class %s for %s", clazz.toGenericString(), stateClass.toGenericString()));
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> methods = findPublicStaticMethodsWithAnnotation(clazz, OutputFunction.class).stream()
                .filter(method -> method.getParameterTypes()[0] == stateClass)
                .collect(toImmutableList());

        checkArgument(!methods.isEmpty(), "Aggregation has no output functions");
        return methods;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> inputFunctions = findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class).stream()
                .filter(method -> method.getParameterTypes()[0] == stateClass)
                .collect(toImmutableList());

        checkArgument(!inputFunctions.isEmpty(), "Aggregation has no input functions");
        return inputFunctions;
    }

    private static List<TypeSignature> getInputTypesSignatures(Method inputFunction)
    {
        // FIXME Literal parameters should be part of class annotations.
        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        Set<String> literalParameters = getLiteralParameter(inputFunction);

        Annotation[][] parameterAnnotations = inputFunction.getParameterAnnotations();
        for (Annotation[] annotations : parameterAnnotations) {
            for (Annotation annotation : annotations) {
                if (annotation instanceof SqlType) {
                    String typeName = ((SqlType) annotation).value();
                    builder.add(parseTypeSignature(typeName, literalParameters));
                }
            }
        }

        return builder.build();
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

    private static String getDescription(AnnotatedElement base, AnnotatedElement override)
    {
        Description description = override.getAnnotation(Description.class);
        if (description != null) {
            return description.value();
        }
        description = base.getAnnotation(Description.class);
        return (description == null) ? null : description.value();
    }

    private static Set<String> getLiteralParameter(Method inputFunction)
    {
        ImmutableSet.Builder<String> literalParametersBuilder = ImmutableSet.builder();

        Annotation[] literalParameters = inputFunction.getAnnotations();
        for (Annotation annotation : literalParameters) {
            if (annotation instanceof LiteralParameters) {
                for (String literal : ((LiteralParameters) annotation).value()) {
                   literalParametersBuilder.add(literal);
                }
            }
        }

        return literalParametersBuilder.build();
    }

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
}
