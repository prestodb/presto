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

import com.facebook.presto.metadata.SqlScalarFunctionBuilder.MethodsGroup;
import com.facebook.presto.metadata.SqlScalarFunctionBuilder.SpecializeContext;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.util.Reflection;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.SignatureBinder.bindVariables;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

class PolymorphicScalarFunction
        extends SqlScalarFunction
{
    private final String description;
    private final boolean hidden;
    private final boolean deterministic;
    private final boolean nullableResult;
    private final List<Boolean> nullableArguments;
    private final List<MethodsGroup> methodsGroups;

    PolymorphicScalarFunction(
            Signature signature,
            String description,
            boolean hidden,
            boolean deterministic,
            boolean nullableResult,
            List<Boolean> nullableArguments,
            List<MethodsGroup> methodsGroups)
    {
        super(signature);

        this.description = description;
        this.hidden = hidden;
        this.deterministic = deterministic;
        this.nullableResult = nullableResult;
        this.nullableArguments = requireNonNull(nullableArguments, "nullableArguments is null");
        this.methodsGroups = requireNonNull(methodsGroups, "methodsWithExtraParametersFunctions is null");
    }

    @Override
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        List<TypeSignature> resolvedParameterTypeSignatures = bindVariables(getSignature().getArgumentTypes(), boundVariables);
        List<Type> resolvedParameterTypes = resolveTypes(resolvedParameterTypeSignatures, typeManager);
        TypeSignature resolvedReturnTypeSignature = bindVariables(getSignature().getReturnType(), boundVariables);
        Type resolvedReturnType = typeManager.getType(resolvedReturnTypeSignature);

        SpecializeContext context = new SpecializeContext(boundVariables, resolvedParameterTypes, resolvedReturnType, typeManager, functionRegistry);
        Optional<Method> matchingMethod = Optional.empty();

        Optional<MethodsGroup> matchingMethodsGroup = Optional.empty();
        for (MethodsGroup candidateMethodsGroup : methodsGroups) {
            for (Method candidateMethod : candidateMethodsGroup.getMethods()) {
                if (matchesParameterAndReturnTypes(candidateMethod, resolvedParameterTypes, resolvedReturnType) &&
                        predicateIsTrue(candidateMethodsGroup, context)) {
                    if (matchingMethod.isPresent()) {
                        if (onlyFirstMatchedMethodHasPredicate(matchingMethodsGroup.get(), candidateMethodsGroup)) {
                            continue;
                        }
                        throw new IllegalStateException("two matching methods (" + matchingMethod.get().getName() + " and " + candidateMethod.getName() + ") for parameter types " + resolvedParameterTypeSignatures);
                    }

                    matchingMethod = Optional.of(candidateMethod);
                    matchingMethodsGroup = Optional.of(candidateMethodsGroup);
                }
            }
        }
        checkState(matchingMethod.isPresent(), "no matching method for parameter types %s", resolvedParameterTypes);

        List<Object> extraParameters = computeExtraParameters(matchingMethodsGroup.get(), context);
        MethodHandle matchingMethodHandle = applyExtraParameters(matchingMethod.get(), extraParameters);

        return new ScalarFunctionImplementation(nullableResult, nullableArguments, matchingMethodHandle, deterministic);
    }

    private boolean matchesParameterAndReturnTypes(Method method, List<Type> resolvedTypes, Type returnType)
    {
        checkState(method.getParameterCount() >= resolvedTypes.size(),
                "method %s has not enough arguments: %s (should have at least %s)", method.getName(), method.getParameterCount(), resolvedTypes.size());

        Class<?>[] methodParameterJavaTypes = method.getParameterTypes();
        for (int i = 0; i < resolvedTypes.size(); ++i) {
            if (!methodParameterJavaTypes[i].equals(getNullAwareContainerType(resolvedTypes.get(i).getJavaType(), nullableArguments.get(i)))) {
                return false;
            }
        }

        return method.getReturnType().equals(getNullAwareContainerType(returnType.getJavaType(), nullableResult));
    }

    private boolean onlyFirstMatchedMethodHasPredicate(MethodsGroup matchingMethodsGroup, MethodsGroup methodsGroup)
    {
        return matchingMethodsGroup.getPredicate().isPresent() && !methodsGroup.getPredicate().isPresent();
    }

    private boolean predicateIsTrue(MethodsGroup methodsGroup, SpecializeContext context)
    {
        return methodsGroup.getPredicate().map(predicate -> predicate.test(context)).orElse(true);
    }

    private List<Object> computeExtraParameters(MethodsGroup methodsGroup, SpecializeContext context)
    {
        return methodsGroup.getExtraParametersFunction().map(function -> function.apply(context)).orElse(emptyList());
    }

    private MethodHandle applyExtraParameters(Method matchingMethod, List<Object> extraParameters)
    {
        Signature signature = getSignature();
        int expectedNumberOfArguments = signature.getArgumentTypes().size() + extraParameters.size();
        int matchingMethodParameterCount = matchingMethod.getParameterCount();
        checkState(matchingMethodParameterCount == expectedNumberOfArguments,
                "method %s has invalid number of arguments: %s (should have %s)", matchingMethod.getName(), matchingMethodParameterCount, expectedNumberOfArguments);

        MethodHandle matchingMethodHandle = Reflection.methodHandle(matchingMethod);
        matchingMethodHandle = MethodHandles.insertArguments(matchingMethodHandle, signature.getArgumentTypes().size(), extraParameters.toArray());
        return matchingMethodHandle;
    }

    private static Class<?> getNullAwareContainerType(Class<?> clazz, boolean nullable)
    {
        if (nullable) {
            return Primitives.wrap(clazz);
        }
        checkArgument(clazz != void.class);
        return clazz;
    }
}
