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
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.ParametricImplementations;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.operator.annotations.ImplementationDependency;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.aggregation.state.StateCompiler.generateStateSerializer;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class BindableAggregationFunction
    extends SqlAggregationFunction
{
    AggregationHeader details;
    ParametricImplementations<AggregationImplementation> implementations;

    public BindableAggregationFunction(Signature signature,
            AggregationHeader details,
            ParametricImplementations implementations)
    {
        super(signature);
        this.details = details;
        this.implementations = implementations;
    }

    @Override
    public String getDescription()
    {
        return details.getDescription().orElse("");
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables variables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Optional<AggregationImplementation> implementation = Optional.empty();

        Signature boundSignature = applyBoundVariables(getSignature(), variables, arity);
        if (implementations.getExactImplementations().containsKey(boundSignature)) {
            implementation = Optional.of(implementations.getExactImplementations().get(boundSignature));
        }
        else {
            for (AggregationImplementation genericImpl : implementations.getGenericImplementations()) {
                if (genericImpl.areTypesAssignable(boundSignature, variables, typeManager, functionRegistry)) {
                    if (implementation.isPresent()) {
                        throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, format("Ambiguous function call (%s) for %s", variables, getSignature()));
                    }
                    implementation = Optional.of(genericImpl);
                }
            }
        }

        if (!implementation.isPresent()) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported type parameters (%s) for %s", variables, getSignature()));
        }

        AggregationImplementation concreteImplementation = implementation.get();
        Class<?> definitionClass = concreteImplementation.getDefinitionClass();
        Class<?> stateClass = concreteImplementation.getStateClass();
        Method inputFunction = concreteImplementation.getInputFunction();
        Method outputFunction = concreteImplementation.getOutputFunction();
        Method combineFunction = concreteImplementation.getCombineFunction();

        checkCondition(
                definitionClass.getAnnotation(AggregationFunction.class) != null,
                FUNCTION_IMPLEMENTATION_ERROR,
                format("Class that defines aggregation function must be annotated with @AggregationFunction"));

        List<Type> inputTypes = boundSignature.getArgumentTypes().stream().map(x -> typeManager.getType(x)).collect(toImmutableList());
        Type outputType = typeManager.getType(boundSignature.getReturnType());

        DynamicClassLoader classLoader = new DynamicClassLoader(definitionClass.getClassLoader(), getClass().getClassLoader());

        AggregationMetadata metadata;
        AccumulatorStateSerializer<?> stateSerializer = getAccumulatorStateSerializer(concreteImplementation, variables, typeManager, functionRegistry, stateClass, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();
        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateClass, classLoader);

        MethodHandle inputHandle = bindDependencies(inputFunction, concreteImplementation.getInputDependencies(), variables, typeManager, functionRegistry);
        MethodHandle combineHandle = bindDependencies(combineFunction, concreteImplementation.getCombineDependencies(), variables, typeManager, functionRegistry);
        MethodHandle outputHandle = bindDependencies(outputFunction, concreteImplementation.getOutputDependencies(), variables, typeManager, functionRegistry);

        metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getName(), outputType.getTypeSignature(), signaturesFromTypes(inputTypes)),
                buildParameterMetadata(concreteImplementation.getInputParameterMetadataTypes(), inputTypes),
                inputHandle,
                combineHandle,
                outputHandle,
                stateClass,
                stateSerializer,
                stateFactory,
                outputType);

        AccumulatorFactoryBinder factory = new LazyAccumulatorFactoryBinder(metadata, classLoader);

        return new InternalAggregationFunction(getSignature().getName(),
                inputTypes,
                intermediateType,
                outputType,
                details.isDecomposable(),
                factory);
    }

    // TODO Extract for scalar use case
    private static MethodHandle bindDependencies(Method function, List<ImplementationDependency> dependencies, BoundVariables variables, TypeManager typeManager, FunctionRegistry functionRegistry)
            throws IllegalAccessException
    {
        MethodHandle handle = methodHandle(function);
        for (ImplementationDependency dependency : dependencies) {
            handle = handle.bindTo(dependency.resolve(variables, typeManager, functionRegistry));
        }
        return handle;
    }

    private AccumulatorStateSerializer<?> getAccumulatorStateSerializer(AggregationImplementation implementation, BoundVariables variables, TypeManager typeManager, FunctionRegistry functionRegistry, Class<?> stateClass, DynamicClassLoader classLoader)
    {
        AccumulatorStateSerializer<?> stateSerializer;
        Optional<Method> stateSerializerFactory = implementation.getStateSerializerFactory();
        if (stateSerializerFactory.isPresent()) {
            try {
                MethodHandle factoryHandle = bindDependencies(stateSerializerFactory.get(), implementation.getStateSerializerFactoryDependencies(), variables, typeManager, functionRegistry);
                stateSerializer = (AccumulatorStateSerializer<?>) factoryHandle.invoke();
            }
            catch (Throwable e) {
                throw Throwables.propagate(e);
            }
        }
        else {
            stateSerializer = generateStateSerializer(stateClass, classLoader);
        }
        return stateSerializer;
    }

    public InternalAggregationFunction specialize(BoundVariables variables, int arity, TypeManager typeManager)
    {
        return specialize(variables, arity, typeManager, null);
    }

    private static List<TypeSignature> signaturesFromTypes(List<Type> types)
    {
        return types
                .stream()
                .map(x -> x.getTypeSignature())
                .collect(toImmutableList());
    }

    private static List<ParameterMetadata> buildParameterMetadata(List<ParameterType> parameterMetadataTypes, List<Type> inputTypes)
    {
        ImmutableList.Builder<ParameterMetadata> builder = ImmutableList.builder();
        int inputId = 0;

        for (ParameterType parameterMetadataType : parameterMetadataTypes) {
            switch (parameterMetadataType) {
                case STATE:
                case BLOCK_INDEX:
                    builder.add(new ParameterMetadata(parameterMetadataType));
                    break;
                case INPUT_CHANNEL:
                case BLOCK_INPUT_CHANNEL:
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    builder.add(new ParameterMetadata(parameterMetadataType, inputTypes.get(inputId++)));
                    break;
            }
        }

        return builder.build();
    }
}
