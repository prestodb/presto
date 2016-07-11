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

import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public final class SqlScalarFunctionBuilder
{
    private final Class<?> clazz;
    private Signature signature;
    private String description;
    private Optional<Boolean> hidden = Optional.empty();
    private boolean deterministic;
    private boolean nullableResult;
    private List<Boolean> nullableArguments = emptyList();
    private List<MethodsGroup> methodsGroups = new ArrayList<>();

    public SqlScalarFunctionBuilder(Class<?> clazz)
    {
        this.clazz = clazz;
    }

    public SqlScalarFunctionBuilder signature(Signature signature)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.hidden = Optional.of(hidden.orElse(isOperator(signature)));
        return this;
    }

    public SqlScalarFunctionBuilder description(String description)
    {
        this.description = description;
        return this;
    }

    public SqlScalarFunctionBuilder hidden(boolean hidden)
    {
        this.hidden = Optional.of(hidden);
        return this;
    }

    public SqlScalarFunctionBuilder deterministic(boolean deterministic)
    {
        this.deterministic = deterministic;
        return this;
    }

    public SqlScalarFunctionBuilder nullableResult(boolean nullableResult)
    {
        this.nullableResult = nullableResult;
        return this;
    }

    public SqlScalarFunctionBuilder nullableArguments(boolean... nullableArguments)
    {
        requireNonNull(nullableArguments, "nullableArguments is null");

        ImmutableList.Builder<Boolean> nullableArgumentsBuilder = ImmutableList.builder();
        for (boolean nullableArgument : nullableArguments) {
            nullableArgumentsBuilder.add(nullableArgument);
        }
        this.nullableArguments = nullableArgumentsBuilder.build();
        return this;
    }

    public SqlScalarFunctionBuilder nullableArguments(List<Boolean> nullableArguments)
    {
        this.nullableArguments = copyOf(requireNonNull(nullableArguments, "nullableArguments is null"));
        return this;
    }

    public SqlScalarFunctionBuilder implementation(Function<MethodsGroupBuilder, MethodsGroupBuilder> methodGroupSpecification)
    {
        MethodsGroupBuilder methodsGroupBuilder = new MethodsGroupBuilder(clazz);
        methodGroupSpecification.apply(methodsGroupBuilder);
        MethodsGroup methodsGroup = methodsGroupBuilder.build();
        methodsGroups.add(methodsGroup);
        return this;
    }

    public SqlScalarFunction build()
    {
        checkState(signature != null, "signature is null");

        if (nullableArguments.isEmpty()) {
            nullableArguments = Collections.nCopies(signature.getArgumentTypes().size(), false);
        }

        return new PolymorphicScalarFunction(signature, description, hidden.orElse(false), deterministic, nullableResult, nullableArguments, methodsGroups);
    }

    @SafeVarargs
    public static Function<SpecializeContext, List<Object>> concat(Function<SpecializeContext, List<Object>>... extraParametersFunctions)
    {
        return context -> {
            ImmutableList.Builder<Object> extraParametersBuilder = ImmutableList.builder();
            for (Function<SpecializeContext, List<Object>> extraParametersFunction : extraParametersFunctions) {
                extraParametersBuilder.addAll(extraParametersFunction.apply(context));
            }
            return extraParametersBuilder.build();
        };
    }

    public static <T> Function<SpecializeContext, List<Object>> constant(T value)
    {
        return context -> ImmutableList.of(value);
    }

    private static boolean isOperator(Signature signature)
    {
        for (OperatorType operator : OperatorType.values()) {
            if (signature.getName().equals(FunctionRegistry.mangleOperatorName(operator))) {
                return true;
            }
        }

        return false;
    }

    public static class SpecializeContext
    {
        private final BoundVariables boundVariables;
        private final List<Type> parameterTypes;
        private final Type returnType;
        private final TypeManager typeManager;
        private final FunctionRegistry functionRegistry;

        SpecializeContext(BoundVariables boundVariables, List<Type> parameterTypes, Type returnType, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            this.boundVariables = requireNonNull(boundVariables, "boundVariables is null");
            this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes is null");
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.returnType = requireNonNull(returnType, "returnType is null");
            this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
        }

        public Type getType(String name)
        {
            return boundVariables.getTypeVariable(name);
        }

        public Long getLiteral(String name)
        {
            return boundVariables.getLongVariable(name);
        }

        public List<Type> getParameterTypes()
        {
            return parameterTypes;
        }

        public Type getReturnType()
        {
            return returnType;
        }

        public TypeManager getTypeManager()
        {
            return typeManager;
        }

        public FunctionRegistry getFunctionRegistry()
        {
            return functionRegistry;
        }
    }

    public static class MethodsGroupBuilder
    {
        private final Class<?> clazz;
        private List<Method> methods = null;
        private Optional<Predicate<SpecializeContext>> predicate = Optional.empty();
        private Optional<Function<SpecializeContext, List<Object>>> extraParametersFunction = Optional.empty();

        private MethodsGroupBuilder(Class<?> clazz)
        {
            this.clazz = clazz;
        }

        public MethodsGroupBuilder methods(String... methodNames)
        {
            return methods(asList(requireNonNull(methodNames, "methodNames is null")));
        }

        public MethodsGroupBuilder methods(List<String> methodNames)
        {
            requireNonNull(methodNames, "methodNames is null");
            checkArgument(!methodNames.isEmpty(), "methods list is empty");

            List<Method> matchingMethods = asList(clazz.getMethods()).stream()
                    .filter(method -> methodNames.contains(method.getName()))
                    .collect(toImmutableList());
            List<String> matchingMethodNames = matchingMethods.stream()
                    .map(Method::getName)
                    .collect(toImmutableList());

            for (String methodName : methodNames) {
                checkState(matchingMethodNames.contains(methodName), "method %s was not found in %s", methodName, clazz);
            }

            this.methods = matchingMethods;
            return this;
        }

        public MethodsGroupBuilder withPredicate(Predicate<SpecializeContext> predicate)
        {
            checkState(methods != null, "methods must be selected first");
            requireNonNull(predicate, "predicate is null");
            this.predicate = Optional.of(predicate);
            return this;
        }

        public MethodsGroupBuilder withExtraParameters(Function<SpecializeContext, List<Object>> extraParametersFunction)
        {
            checkState(methods != null, "methods must be selected first");
            requireNonNull(extraParametersFunction, "extraParametersFunction is null");
            this.extraParametersFunction = Optional.of(extraParametersFunction);
            return this;
        }

        public MethodsGroup build()
        {
            return new MethodsGroup(methods, predicate, extraParametersFunction);
        }
    }

    static class MethodsGroup
    {
        private final List<Method> methods;
        private final Optional<Predicate<SpecializeContext>> predicate;
        private final Optional<Function<SpecializeContext, List<Object>>> extraParametersFunction;

        private MethodsGroup(
                List<Method> methods,
                Optional<Predicate<SpecializeContext>> predicate,
                Optional<Function<SpecializeContext, List<Object>>> extraParametersFunction)
        {
            this.methods = requireNonNull(methods, "methods is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
            this.extraParametersFunction = requireNonNull(extraParametersFunction, "extraParametersFunction is null");
        }

        List<Method> getMethods()
        {
            return methods;
        }

        Optional<Predicate<SpecializeContext>> getPredicate()
        {
            return predicate;
        }

        Optional<Function<SpecializeContext, List<Object>>> getExtraParametersFunction()
        {
            return extraParametersFunction;
        }
    }
}
