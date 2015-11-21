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
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.fromAnnotations;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class AggregationCompiler
{
    private final TypeManager typeManager;

    public AggregationCompiler()
    {
        this(new TypeRegistry());
    }

    public AggregationCompiler(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
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

    public InternalAggregationFunction generateAggregationFunction(Class<?> clazz)
    {
        List<InternalAggregationFunction> aggregations = generateAggregationFunctions(clazz);
        checkArgument(aggregations.size() == 1, "More than one aggregation function found");
        return aggregations.get(0);
    }

    public InternalAggregationFunction generateAggregationFunction(Class<?> clazz, Type returnType, List<Type> argumentTypes)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        for (InternalAggregationFunction aggregation : generateAggregationFunctions(clazz)) {
            if (aggregation.getFinalType().equals(returnType) && aggregation.getParameterTypes().equals(argumentTypes)) {
                return aggregation;
            }
        }
        throw new IllegalArgumentException(String.format("No method with return type %s and arguments %s", returnType, argumentTypes));
    }

    public List<InternalAggregationFunction> generateAggregationFunctions(Class<?> clazz)
    {
        AggregationFunction aggregationAnnotation = clazz.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");

        DynamicClassLoader classLoader = new DynamicClassLoader(clazz.getClassLoader());

        ImmutableList.Builder<InternalAggregationFunction> builder = ImmutableList.builder();
        for (Class<?> stateClass : getStateClasses(clazz)) {
            AccumulatorStateSerializer<?> stateSerializer = new StateCompiler().generateStateSerializer(stateClass, classLoader);
            Type intermediateType = stateSerializer.getSerializedType();
            Method intermediateInputFunction = getIntermediateInputFunction(clazz, stateClass);
            Method combineFunction = getCombineFunction(clazz, stateClass);
            AccumulatorStateFactory<?> stateFactory = new StateCompiler().generateStateFactory(stateClass, classLoader);

            for (Method outputFunction : getOutputFunctions(clazz, stateClass)) {
                for (Method inputFunction : getInputFunctions(clazz, stateClass)) {
                    for (String name : getNames(outputFunction, aggregationAnnotation)) {
                        List<Type> inputTypes = getInputTypes(inputFunction);
                        Type outputType = AggregationUtils.getOutputType(outputFunction, stateSerializer, typeManager);

                        AggregationMetadata metadata;
                        try {
                            MethodHandle inputHandle = lookup().unreflect(inputFunction);
                            MethodHandle intermediateInputHandle = intermediateInputFunction == null ? null : lookup().unreflect(intermediateInputFunction);
                            MethodHandle combineHandle = combineFunction == null ? null : lookup().unreflect(combineFunction);
                            MethodHandle outputHandle = outputFunction == null ? null : lookup().unreflect(outputFunction);
                            metadata = new AggregationMetadata(
                                    generateAggregationName(name, outputType, inputTypes),
                                    getParameterMetadata(inputFunction, aggregationAnnotation.approximate()),
                                    inputHandle,
                                    getParameterMetadata(intermediateInputFunction, false),
                                    intermediateInputHandle,
                                    combineHandle,
                                    outputHandle,
                                    stateClass,
                                    stateSerializer,
                                    stateFactory,
                                    outputType,
                                    aggregationAnnotation.approximate());
                        }
                        catch (IllegalAccessException e) {
                            throw Throwables.propagate(e);
                        }

                        AccumulatorFactoryBinder factory = new LazyAccumulatorFactoryBinder(metadata, classLoader);
                        builder.add(new InternalAggregationFunction(name, inputTypes, intermediateType, outputType, aggregationAnnotation.decomposable(), aggregationAnnotation.approximate(), factory));
                    }
                }
            }
        }

        return builder.build();
    }

    private List<ParameterMetadata> getParameterMetadata(@Nullable Method method, boolean sampleWeightAllowed)
    {
        if (method == null) {
            return null;
        }

        ImmutableList.Builder<ParameterMetadata> builder = ImmutableList.builder();
        builder.add(new ParameterMetadata(STATE));

        Annotation[][] annotations = method.getParameterAnnotations();
        // Start at 1 because 0 is the STATE
        for (int i = 1; i < annotations.length; i++) {
            builder.add(fromAnnotations(annotations[i], method.getDeclaringClass() + "." + method.getName(), typeManager, sampleWeightAllowed));
        }
        return builder.build();
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

    private List<Type> getInputTypes(Method inputFunction)
    {
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        Annotation[][] parameterAnnotations = inputFunction.getParameterAnnotations();
        for (Annotation[] annotations : parameterAnnotations) {
            for (Annotation annotation : annotations) {
                if (annotation instanceof SqlType) {
                    String typeName = ((SqlType) annotation).value();
                    builder.add(typeManager.getType(parseTypeSignature(typeName)));
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
}
