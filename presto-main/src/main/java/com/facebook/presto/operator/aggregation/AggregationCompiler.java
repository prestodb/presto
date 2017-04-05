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
import com.facebook.presto.metadata.LongVariableConstraint;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.Constraint;
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
import java.util.stream.Stream;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
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
            AccumulatorStateSerializer<?> stateSerializer = StateCompiler.generateStateSerializer(stateClass, classLoader);

            for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateClass)) {
                for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                    List<LongVariableConstraint> longVariableConstraints = parseLongVariableConstraints(inputFunction);

                    for (String name : getNames(outputFunction, aggregationAnnotation)) {
                        List<TypeSignature> inputTypes = getInputTypesSignatures(inputFunction);
                        TypeSignature outputType = TypeSignature.parseTypeSignature(outputFunction.getAnnotation(OutputFunction.class).value());

                        builder.add(
                                new BindableAggregationFunction(
                                        new Signature(
                                                name,
                                                FunctionKind.AGGREGATE,
                                                ImmutableList.of(), // TODO parse constrains from annotations
                                                longVariableConstraints,
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

    private static List<LongVariableConstraint> parseLongVariableConstraints(Method inputFunction)
    {
        return Stream.of(inputFunction.getAnnotationsByType(Constraint.class))
                .map(annotation -> new LongVariableConstraint(annotation.variable(), annotation.expression()))
                .collect(toImmutableList());
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
        // Only include methods that match this state class
        List<Method> combineFunctions = findPublicStaticMethodsWithAnnotation(clazz, CombineFunction.class).stream()
                .filter(method -> method.getParameterTypes()[findAggregationStateParamId(method, 0)] == stateClass)
                .filter(method -> method.getParameterTypes()[findAggregationStateParamId(method, 1)] == stateClass)
                .collect(toImmutableList());

        checkArgument(combineFunctions.size() == 1, String.format("There must be exactly one @CombineFunction in class %s for the @AggregationState %s ", clazz.toGenericString(), stateClass.toGenericString()));
        return getOnlyElement(combineFunctions);
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> outputFunctions = findPublicStaticMethodsWithAnnotation(clazz, OutputFunction.class).stream()
                .filter(method -> method.getParameterTypes()[findAggregationStateParamId(method)] == stateClass)
                .collect(toImmutableList());

        checkArgument(!outputFunctions.isEmpty(), "Aggregation has no output functions");
        return outputFunctions;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> inputFunctions = findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class).stream()
                .filter(method -> (method.getParameterTypes()[findAggregationStateParamId(method)] == stateClass))
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
            Class<?> stateClass = findAggregationStateParamType(inputFunction);

            checkArgument(AccumulatorState.class.isAssignableFrom(stateClass), "stateClass is not a subclass of AccumulatorState");
            builder.add(stateClass);
        }
        ImmutableSet<Class<?>> stateClasses = builder.build();
        checkArgument(!stateClasses.isEmpty(), "No input functions found");

        return stateClasses;
    }

    private static Class<?> findAggregationStateParamType(Method inputFunction)
    {
        return inputFunction.getParameterTypes()[findAggregationStateParamId(inputFunction)];
    }

    public static int findAggregationStateParamId(Method method)
    {
        return findAggregationStateParamId(method, 0);
    }

    public static int findAggregationStateParamId(Method method, int id)
    {
        int currentParamId = 0;
        int found = 0;
        for (Annotation[] annotations : method.getParameterAnnotations()) {
            for (Annotation annotation : annotations) {
                if (annotation instanceof AggregationState) {
                    if (found++ == id) {
                        return currentParamId;
                    }
                }
            }
            currentParamId++;
        }

        // backward compatibility @AggregationState annotation didn't exists before
        // some third party aggregates may assume that State will be id-th parameter
        return id;
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
