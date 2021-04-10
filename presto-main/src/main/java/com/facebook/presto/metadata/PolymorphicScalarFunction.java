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
package com.facebook.presto.metadata;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.PolymorphicScalarFunctionBuilder.MethodAndNativeContainerTypes;
import com.facebook.presto.metadata.PolymorphicScalarFunctionBuilder.MethodsGroup;
import com.facebook.presto.metadata.PolymorphicScalarFunctionBuilder.SpecializeContext;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ReturnPlaceConvention;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ScalarImplementationChoice;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

class PolymorphicScalarFunction
        extends SqlScalarFunction
{
    private final String description;
    private final SqlFunctionVisibility visibility;
    private final boolean deterministic;
    private final boolean calledOnNullInput;
    private final List<PolymorphicScalarFunctionChoice> choices;

    PolymorphicScalarFunction(
            Signature signature,
            String description,
            SqlFunctionVisibility visibility,
            boolean deterministic,
            boolean calledOnNullInput,
            List<PolymorphicScalarFunctionChoice> choices)
    {
        super(signature);

        this.description = description;
        this.visibility = visibility;
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
        this.choices = requireNonNull(choices, "choices is null");
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return visibility;
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        ImmutableList.Builder<ScalarImplementationChoice> implementationChoices = ImmutableList.builder();

        for (PolymorphicScalarFunctionChoice choice : choices) {
            implementationChoices.add(getScalarFunctionImplementationChoice(boundVariables, functionAndTypeManager, choice));
        }

        return new BuiltInScalarFunctionImplementation(implementationChoices.build());
    }

    private ScalarImplementationChoice getScalarFunctionImplementationChoice(
            BoundVariables boundVariables,
            FunctionAndTypeManager functionAndTypeManager,
            PolymorphicScalarFunctionChoice choice)
    {
        List<Type> resolvedParameterTypes = applyBoundVariables(functionAndTypeManager, getSignature().getArgumentTypes(), boundVariables);
        Type resolvedReturnType = applyBoundVariables(functionAndTypeManager, getSignature().getReturnType(), boundVariables);
        SpecializeContext context = new SpecializeContext(boundVariables, resolvedParameterTypes, resolvedReturnType, functionAndTypeManager);
        Optional<MethodAndNativeContainerTypes> matchingMethod = Optional.empty();

        Optional<MethodsGroup> matchingMethodsGroup = Optional.empty();
        for (MethodsGroup candidateMethodsGroup : choice.getMethodsGroups()) {
            for (MethodAndNativeContainerTypes candidateMethod : candidateMethodsGroup.getMethods()) {
                if (matchesParameterAndReturnTypes(candidateMethod, resolvedParameterTypes, resolvedReturnType, choice.getArgumentProperties(), choice.isNullableResult())) {
                    if (matchingMethod.isPresent()) {
                        throw new IllegalStateException("two matching methods (" + matchingMethod.get().getMethod().getName() + " and " + candidateMethod.getMethod().getName() + ") for parameter types " + resolvedParameterTypes);
                    }

                    matchingMethod = Optional.of(candidateMethod);
                    matchingMethodsGroup = Optional.of(candidateMethodsGroup);
                }
            }
        }
        checkState(matchingMethod.isPresent(), "no matching method for parameter types %s", resolvedParameterTypes);

        List<Object> extraParameters = computeExtraParameters(matchingMethodsGroup.get(), context);
        MethodHandle methodHandle = applyExtraParameters(matchingMethod.get().getMethod(), extraParameters, choice.getArgumentProperties());
        return new ScalarImplementationChoice(choice.isNullableResult(), choice.getArgumentProperties(), choice.getReturnPlaceConvention(), methodHandle, Optional.empty());
    }

    private static boolean matchesParameterAndReturnTypes(
            MethodAndNativeContainerTypes methodAndNativeContainerTypes,
            List<Type> resolvedTypes,
            Type returnType,
            List<ArgumentProperty> argumentProperties,
            boolean nullableResult)
    {
        Method method = methodAndNativeContainerTypes.getMethod();
        checkState(method.getParameterCount() >= resolvedTypes.size(),
                "method %s has not enough arguments: %s (should have at least %s)", method.getName(), method.getParameterCount(), resolvedTypes.size());

        Class<?>[] methodParameterJavaTypes = method.getParameterTypes();
        for (int i = 0, methodParameterIndex = 0; i < resolvedTypes.size(); i++) {
            NullConvention nullConvention = argumentProperties.get(i).getNullConvention();
            Class<?> expectedType = null;
            Class<?> actualType;
            switch (nullConvention) {
                case RETURN_NULL_ON_NULL:
                case USE_NULL_FLAG:
                    expectedType = methodParameterJavaTypes[methodParameterIndex];
                    actualType = getNullAwareContainerType(resolvedTypes.get(i).getJavaType(), false);
                    break;
                case USE_BOXED_TYPE:
                    expectedType = methodParameterJavaTypes[methodParameterIndex];
                    actualType = getNullAwareContainerType(resolvedTypes.get(i).getJavaType(), true);
                    break;
                case BLOCK_AND_POSITION:
                    Optional<Class<?>> explicitNativeContainerTypes = methodAndNativeContainerTypes.getExplicitNativeContainerTypes().get(i);
                    if (explicitNativeContainerTypes.isPresent()) {
                        expectedType = explicitNativeContainerTypes.get();
                    }
                    actualType = getNullAwareContainerType(resolvedTypes.get(i).getJavaType(), false);
                    break;
                default:
                    throw new UnsupportedOperationException("unknown NullConvention");
            }
            if (!actualType.equals(expectedType)) {
                return false;
            }
            methodParameterIndex += nullConvention.getParameterCount();
        }
        return method.getReturnType().equals(getNullAwareContainerType(returnType.getJavaType(), nullableResult));
    }

    private static List<Object> computeExtraParameters(MethodsGroup methodsGroup, SpecializeContext context)
    {
        return methodsGroup.getExtraParametersFunction().map(function -> function.apply(context)).orElse(emptyList());
    }

    private static int getNullFlagsCount(List<ArgumentProperty> argumentProperties)
    {
        return (int) argumentProperties.stream()
                .filter(argumentProperty -> argumentProperty.getNullConvention() == USE_NULL_FLAG)
                .count();
    }

    private static int getBlockPositionCount(List<ArgumentProperty> argumentProperties)
    {
        return (int) argumentProperties.stream()
                .filter(argumentProperty -> argumentProperty.getNullConvention() == BLOCK_AND_POSITION)
                .count();
    }

    private MethodHandle applyExtraParameters(Method matchingMethod, List<Object> extraParameters, List<ArgumentProperty> argumentProperties)
    {
        Signature signature = getSignature();
        int expectedArgumentsCount = signature.getArgumentTypes().size() + getNullFlagsCount(argumentProperties) + getBlockPositionCount(argumentProperties) + extraParameters.size();
        int matchingMethodArgumentCount = matchingMethod.getParameterCount();
        checkState(matchingMethodArgumentCount == expectedArgumentsCount,
                "method %s has invalid number of arguments: %s (should have %s)", matchingMethod.getName(), matchingMethodArgumentCount, expectedArgumentsCount);

        MethodHandle matchingMethodHandle = Reflection.methodHandle(matchingMethod);
        matchingMethodHandle = MethodHandles.insertArguments(
                matchingMethodHandle,
                matchingMethodArgumentCount - extraParameters.size(),
                extraParameters.toArray());
        return matchingMethodHandle;
    }

    private static Class<?> getNullAwareContainerType(Class<?> clazz, boolean nullable)
    {
        if (nullable) {
            return Primitives.wrap(clazz);
        }
        return clazz;
    }

    static final class PolymorphicScalarFunctionChoice
    {
        private final boolean nullableResult;
        private final List<ArgumentProperty> argumentProperties;
        private final ReturnPlaceConvention returnPlaceConvention;
        private final List<MethodsGroup> methodsGroups;

        PolymorphicScalarFunctionChoice(
                boolean nullableResult,
                List<ArgumentProperty> argumentProperties,
                ReturnPlaceConvention returnPlaceConvention,
                List<MethodsGroup> methodsGroups)
        {
            this.nullableResult = nullableResult;
            this.argumentProperties = ImmutableList.copyOf(requireNonNull(argumentProperties, "argumentProperties is null"));
            this.returnPlaceConvention = requireNonNull(returnPlaceConvention, "returnPlaceConvention is null");
            this.methodsGroups = ImmutableList.copyOf(requireNonNull(methodsGroups, "methodsWithExtraParametersFunctions is null"));
        }

        boolean isNullableResult()
        {
            return nullableResult;
        }

        List<MethodsGroup> getMethodsGroups()
        {
            return methodsGroups;
        }

        List<ArgumentProperty> getArgumentProperties()
        {
            return argumentProperties;
        }

        ReturnPlaceConvention getReturnPlaceConvention()
        {
            return returnPlaceConvention;
        }
    }
}
