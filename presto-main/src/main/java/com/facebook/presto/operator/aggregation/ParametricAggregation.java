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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.ParametricImplementationsGroup;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.Signature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.operator.ParametricFunctionHelpers.bindDependencies;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.aggregation.state.StateCompiler.generateStateSerializer;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParametricAggregation
        extends SqlAggregationFunction
{
    final AggregationHeader details;
    final ParametricImplementationsGroup<AggregationImplementation> implementations;

    public ParametricAggregation(
            Signature signature,
            AggregationHeader details,
            ParametricImplementationsGroup<AggregationImplementation> implementations)
    {
        super(signature, details.getVisibility());
        this.details = requireNonNull(details, "details is null");
        this.implementations = requireNonNull(implementations, "implementations is null");
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables variables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        // Bind variables
        Signature boundSignature = applyBoundVariables(getSignature(), variables, arity);

        // Find implementation matching arguments
        AggregationImplementation concreteImplementation = findMatchingImplementation(boundSignature, variables, functionAndTypeManager);

        // Build argument and return Types from signatures
        List<Type> inputTypes = boundSignature.getArgumentTypes().stream().map(functionAndTypeManager::getType).collect(toImmutableList());
        Type outputType = functionAndTypeManager.getType(boundSignature.getReturnType());

        // Create classloader for additional aggregation dependencies
        Class<?> definitionClass = concreteImplementation.getDefinitionClass();
        DynamicClassLoader classLoader = new DynamicClassLoader(definitionClass.getClassLoader(), getClass().getClassLoader());

        // Build state factory and serializer
        Class<?> stateClass = concreteImplementation.getStateClass();
        AccumulatorStateSerializer<?> stateSerializer = getAccumulatorStateSerializer(concreteImplementation, variables, functionAndTypeManager, stateClass, classLoader);
        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateClass, classLoader);

        // Bind provided dependencies to aggregation method handlers
        MethodHandle inputHandle = bindDependencies(concreteImplementation.getInputFunction(), concreteImplementation.getInputDependencies(), variables, functionAndTypeManager);
        MethodHandle combineHandle = bindDependencies(concreteImplementation.getCombineFunction(), concreteImplementation.getCombineDependencies(), variables, functionAndTypeManager);
        MethodHandle outputHandle = bindDependencies(concreteImplementation.getOutputFunction(), concreteImplementation.getOutputDependencies(), variables, functionAndTypeManager);

        // Build metadata of input parameters
        List<ParameterMetadata> parametersMetadata = buildParameterMetadata(concreteImplementation.getInputParameterMetadataTypes(), inputTypes);

        // Generate Aggregation name
        String aggregationName = generateAggregationName(getSignature().getNameSuffix(), outputType.getTypeSignature(), signaturesFromTypes(inputTypes));

        // Collect all collected data in Metadata
        AggregationMetadata metadata = new AggregationMetadata(
                aggregationName,
                parametersMetadata,
                inputHandle,
                combineHandle,
                outputHandle,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateClass,
                        stateSerializer,
                        stateFactory)),
                outputType);

        // Create specialized InternalAggregregationFunction for Presto
        return new InternalAggregationFunction(getSignature().getNameSuffix(),
                inputTypes,
                ImmutableList.of(stateSerializer.getSerializedType()),
                outputType,
                details.isDecomposable(),
                details.isOrderSensitive(),
                new LazyAccumulatorFactoryBinder(metadata, classLoader));
    }

    @VisibleForTesting
    public ParametricImplementationsGroup<AggregationImplementation> getImplementations()
    {
        return implementations;
    }

    @Override
    public String getDescription()
    {
        return details.getDescription().orElse("");
    }

    private AggregationImplementation findMatchingImplementation(Signature boundSignature, BoundVariables variables, FunctionAndTypeManager functionAndTypeManager)
    {
        Optional<AggregationImplementation> foundImplementation = Optional.empty();
        if (implementations.getExactImplementations().containsKey(boundSignature)) {
            foundImplementation = Optional.of(implementations.getExactImplementations().get(boundSignature));
        }
        else {
            for (AggregationImplementation candidate : implementations.getGenericImplementations()) {
                if (candidate.areTypesAssignable(boundSignature, variables, functionAndTypeManager)) {
                    if (foundImplementation.isPresent()) {
                        throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, format("Ambiguous function call (%s) for %s", variables, getSignature()));
                    }
                    foundImplementation = Optional.of(candidate);
                }
            }
        }

        if (!foundImplementation.isPresent()) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported type parameters (%s) for %s", variables, getSignature()));
        }
        return foundImplementation.get();
    }

    private static AccumulatorStateSerializer<?> getAccumulatorStateSerializer(AggregationImplementation implementation, BoundVariables variables, FunctionAndTypeManager functionAndTypeManager, Class<?> stateClass, DynamicClassLoader classLoader)
    {
        AccumulatorStateSerializer<?> stateSerializer;
        Optional<MethodHandle> stateSerializerFactory = implementation.getStateSerializerFactory();
        if (stateSerializerFactory.isPresent()) {
            try {
                MethodHandle factoryHandle = bindDependencies(stateSerializerFactory.get(), implementation.getStateSerializerFactoryDependencies(), variables, functionAndTypeManager);
                stateSerializer = (AccumulatorStateSerializer<?>) factoryHandle.invoke();
            }
            catch (Throwable t) {
                throwIfUnchecked(t);
                throw new RuntimeException(t);
            }
        }
        else {
            stateSerializer = generateStateSerializer(stateClass, classLoader);
        }
        return stateSerializer;
    }

    private static List<TypeSignature> signaturesFromTypes(List<Type> types)
    {
        return types
                .stream()
                .map(Type::getTypeSignature)
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
