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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.operator.ParametricImplementationsGroup;
import com.facebook.presto.operator.annotations.FunctionsParserHelper;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationStateSerializerFactory;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nullable;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.operator.aggregation.AggregationImplementation.Parser.parseImplementation;
import static com.facebook.presto.operator.annotations.FunctionsParserHelper.parseDescription;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class AggregationFromAnnotationsParser
{
    private AggregationFromAnnotationsParser()
    {
    }

    // This function should only be used for function matching for testing purposes.
    // General purpose function matching is done through FunctionRegistry.
    @VisibleForTesting
    public static ParametricAggregation parseFunctionDefinitionWithTypesConstraint(Class<?> clazz, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        for (ParametricAggregation aggregation : parseFunctionDefinitions(clazz, JAVA_BUILTIN_NAMESPACE)) {
            if (aggregation.getSignature().getReturnType().equals(returnType) &&
                    aggregation.getSignature().getArgumentTypes().equals(argumentTypes)) {
                return aggregation;
            }
        }
        throw new IllegalArgumentException(String.format("No method with return type %s and arguments %s", returnType, argumentTypes));
    }

    public static List<ParametricAggregation> parseFunctionDefinitions(Class<?> aggregationDefinition)
    {
        return parseFunctionDefinitions(aggregationDefinition, JAVA_BUILTIN_NAMESPACE);
    }

    public static List<ParametricAggregation> parseFunctionDefinitions(Class<?> aggregationDefinition, CatalogSchemaName functionNamespace)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");

        ImmutableList.Builder<ParametricAggregation> builder = ImmutableList.builder();

        for (Class<?> stateClass : getStateClasses(aggregationDefinition)) {
            Method combineFunction = getCombineFunction(aggregationDefinition, stateClass);
            Optional<Method> aggregationStateSerializerFactory = getAggregationStateSerializerFactory(aggregationDefinition, stateClass);
            for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateClass)) {
                for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                    for (AggregationHeader header : parseHeaders(aggregationDefinition, outputFunction)) {
                        AggregationImplementation onlyImplementation = parseImplementation(aggregationDefinition, header, stateClass, inputFunction, outputFunction, combineFunction, aggregationStateSerializerFactory, functionNamespace);
                        ParametricImplementationsGroup<AggregationImplementation> implementations = ParametricImplementationsGroup.of(onlyImplementation);
                        builder.add(new ParametricAggregation(implementations.getSignature(), header, implementations));
                    }
                }
            }
        }

        return builder.build();
    }

    public static ParametricAggregation parseFunctionDefinition(Class<?> aggregationDefinition)
    {
        ParametricImplementationsGroup.Builder<AggregationImplementation> implementationsBuilder = ParametricImplementationsGroup.builder();
        AggregationHeader header = parseHeader(aggregationDefinition);

        for (Class<?> stateClass : getStateClasses(aggregationDefinition)) {
            Method combineFunction = getCombineFunction(aggregationDefinition, stateClass);
            Optional<Method> aggregationStateSerializerFactory = getAggregationStateSerializerFactory(aggregationDefinition, stateClass);
            Method outputFunction = getOnlyElement(getOutputFunctions(aggregationDefinition, stateClass));
            for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                AggregationImplementation implementation = parseImplementation(aggregationDefinition, header, stateClass, inputFunction, outputFunction, combineFunction, aggregationStateSerializerFactory, JAVA_BUILTIN_NAMESPACE);
                implementationsBuilder.addImplementation(implementation);
            }
        }

        ParametricImplementationsGroup<AggregationImplementation> implementations = implementationsBuilder.build();
        return new ParametricAggregation(implementations.getSignature(), header, implementations);
    }

    private static Optional<Method> getAggregationStateSerializerFactory(Class<?> aggregationDefinition, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> stateSerializerFactories = FunctionsParserHelper.findPublicStaticMethods(aggregationDefinition, AggregationStateSerializerFactory.class).stream()
                .filter(method -> ((AggregationStateSerializerFactory) method.getAnnotation(AggregationStateSerializerFactory.class)).value().equals(stateClass))
                .collect(toImmutableList());

        if (stateSerializerFactories.isEmpty()) {
            return Optional.empty();
        }

        checkArgument(stateSerializerFactories.size() == 1,
                String.format(
                        "Expect at most 1 @AggregationStateSerializerFactory(%s.class) annotation, found %s in %s",
                        stateClass.toGenericString(),
                        stateSerializerFactories.size(),
                        aggregationDefinition.toGenericString()));
        return Optional.of(getOnlyElement(stateSerializerFactories));
    }

    private static AggregationHeader parseHeader(AnnotatedElement aggregationDefinition)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");
        return new AggregationHeader(
                aggregationAnnotation.value(),
                parseDescription(aggregationDefinition),
                aggregationAnnotation.decomposable(),
                aggregationAnnotation.isOrderSensitive(),
                aggregationAnnotation.visibility(),
                aggregationAnnotation.isCalledOnNullInput());
    }

    private static List<AggregationHeader> parseHeaders(AnnotatedElement aggregationDefinition, AnnotatedElement toParse)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);

        return getNames(toParse, aggregationAnnotation).stream()
                .map(name ->
                        new AggregationHeader(
                                name,
                                parseDescription(aggregationDefinition, toParse),
                                aggregationAnnotation.decomposable(),
                                aggregationAnnotation.isOrderSensitive(),
                                aggregationAnnotation.visibility(),
                                aggregationAnnotation.isCalledOnNullInput()))
                .collect(toImmutableList());
    }

    private static List<String> getNames(@Nullable AnnotatedElement outputFunction, AggregationFunction aggregationAnnotation)
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
        List<Method> combineFunctions = FunctionsParserHelper.findPublicStaticMethods(clazz, CombineFunction.class).stream()
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method, 0)] == stateClass)
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method, 1)] == stateClass)
                .collect(toImmutableList());

        checkArgument(combineFunctions.size() == 1, String.format("There must be exactly one @CombineFunction in class %s for the @AggregationState %s ", clazz.toGenericString(), stateClass.toGenericString()));
        return getOnlyElement(combineFunctions);
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> outputFunctions = FunctionsParserHelper.findPublicStaticMethods(clazz, OutputFunction.class).stream()
                .filter(method -> method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method)] == stateClass)
                .collect(toImmutableList());

        checkArgument(!outputFunctions.isEmpty(), "Aggregation has no output functions");
        return outputFunctions;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> inputFunctions = FunctionsParserHelper.findPublicStaticMethods(clazz, InputFunction.class).stream()
                .filter(method -> (method.getParameterTypes()[AggregationImplementation.Parser.findAggregationStateParamId(method)] == stateClass))
                .collect(toImmutableList());

        checkArgument(!inputFunctions.isEmpty(), "Aggregation has no input functions");
        return inputFunctions;
    }

    private static Set<Class<?>> getStateClasses(Class<?> clazz)
    {
        ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();
        for (Method inputFunction : FunctionsParserHelper.findPublicStaticMethods(clazz, InputFunction.class)) {
            checkArgument(inputFunction.getParameterTypes().length > 0, "Input function has no parameters");
            Class<?> stateClass = AggregationImplementation.Parser.findAggregationStateParamType(inputFunction);

            checkArgument(AccumulatorState.class.isAssignableFrom(stateClass), "stateClass is not a subclass of AccumulatorState");
            builder.add(stateClass);
        }
        ImmutableSet<Class<?>> stateClasses = builder.build();
        checkArgument(!stateClasses.isEmpty(), "No input functions found");

        return stateClasses;
    }
}
