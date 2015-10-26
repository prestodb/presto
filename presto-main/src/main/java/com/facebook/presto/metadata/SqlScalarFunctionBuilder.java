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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class SqlScalarFunctionBuilder
{
    private final Optional<Class<?>> clazz;
    private Signature signature;
    private String description;
    private Optional<Boolean> hidden = Optional.empty();
    private boolean deterministic;
    private boolean nullableResult;
    private List<Boolean> nullableArguments = emptyList();
    private List<MethodsGroup> methodsGroups = newArrayList();
    private Optional<MethodsGroup> currentMethodGroup = Optional.empty();
    private boolean methodsFlushedForCurrentGroup;

    public SqlScalarFunctionBuilder(Class<?> clazz)
    {
        this.clazz = Optional.of(clazz);
    }

    public SqlScalarFunctionBuilder()
    {
        this.clazz = Optional.empty();
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

    public SqlScalarFunctionBuilder methods(String... methodNames)
    {
        return methods(asList(requireNonNull(methodNames, "methodNames is null")));
    }

    public SqlScalarFunctionBuilder methods(List<String> methodNames)
    {
        requireNonNull(methodNames, "methodNames is null");
        methodNames.forEach(this::method);
        return this;
    }

    public SqlScalarFunctionBuilder method(String methodName)
    {
        initMethodGroupIfNeeded();
        checkState(clazz.isPresent(), "class instance not set");
        List<TargetMethodDelegate> matchingMethods = asList(clazz.get().getMethods()).stream()
                .filter(method -> methodName.equals(method.getName()))
                .map(Reflection::methodHandle)
                .map(methodHandle -> new TargetMethodDelegate(methodName, methodHandle))
                .collect(toList());
        checkState(!matchingMethods.isEmpty(), "method %s was not found in %s", methodName, clazz.get());
        matchingMethods.forEach(currentMethodGroup.get()::addMethod);
        return this;
    }

    private void initMethodGroupIfNeeded()
    {
        if (!currentMethodGroup.isPresent() || methodsFlushedForCurrentGroup) {
            MethodsGroup newMethodGroup = new MethodsGroup();
            currentMethodGroup = Optional.of(newMethodGroup);
            methodsGroups.add(newMethodGroup);
            methodsFlushedForCurrentGroup = false;
        }
    }

    public SqlScalarFunctionBuilder method(SqlScalarFunction scalarFunctionDelegate, MethodHandle methodHandle)
    {
        initMethodGroupIfNeeded();
        currentMethodGroup.get().addMethod(new TargetMethodDelegate(scalarFunctionDelegate, methodHandle));
        return this;
    }

    public SqlScalarFunctionBuilder predicate(Predicate<SpecializeContext> predicate)
    {
        requireNonNull(predicate, "predicate is null");
        checkState(currentMethodGroup.isPresent(), "no methods are selected (call methods() first)");
        checkState(!currentMethodGroup.get().getPredicate().isPresent(), "predicate already defined for selected methods");
        currentMethodGroup.get().setPredicate(predicate);
        methodsFlushedForCurrentGroup = true;
        return this;
    }

    public SqlScalarFunctionBuilder extraParameters(Function<SpecializeContext, List<Object>> extraParametersFunction)
    {
        requireNonNull(extraParametersFunction, "extraParametersFunction is null");
        checkState(currentMethodGroup.isPresent(), "no methods are selected (call methods() first)");
        checkState(!currentMethodGroup.get().getExtraParametersFunction().isPresent(), "extraParameters already defined for selected methods");
        checkState(!currentMethodGroup.get().hasScalarMethodDelegates(), "scalar method delegates cannot be used with extraParameters");

        currentMethodGroup.get().setExtraParametersFunction(extraParametersFunction);
        methodsFlushedForCurrentGroup = true;
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
        private final Map<String, Type> types;
        private final Map<String, Long> literals;
        private final List<Type> parameterTypes;
        private final Type returnType;
        private final TypeManager typeManager;
        private final FunctionRegistry functionRegistry;

        SpecializeContext(Map<String, Type> types, Map<String, Long> literals, List<Type> parameterTypes, Type returnType, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            this.types = requireNonNull(types, "types is null");
            this.literals = requireNonNull(literals, "literals is null");
            this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes is null");
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.returnType = requireNonNull(returnType, "returnType is null");
            this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
        }

        public Map<String, Type> getTypes()
        {
            return types;
        }

        public Type getType(String name)
        {
            return types.get(name);
        }

        public Map<String, Long> getLiterals()
        {
            return literals;
        }

        public Long getLiteral(String name)
        {
            return literals.get(name);
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

    static class TargetMethodDelegate
    {
        private final String name;
        private final MethodHandle methodHandle;
        private final Optional<SqlScalarFunction> scalarFunctionDelegate;

        public TargetMethodDelegate(SqlScalarFunction scalarFunctionDelegate, MethodHandle methodHandle)
        {
            this.name = scalarFunctionDelegate.getSignature().getName();
            this.methodHandle = methodHandle;
            this.scalarFunctionDelegate = Optional.of(scalarFunctionDelegate);
        }

        public TargetMethodDelegate(String name, MethodHandle methodHandle)
        {
            this.name = name;
            this.methodHandle = methodHandle;
            this.scalarFunctionDelegate = Optional.empty();
        }

        public String getName()
        {
            return name;
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public Optional<SqlScalarFunction> getScalarFunctionDelegate()
        {
            return scalarFunctionDelegate;
        }
    }

    static class MethodsGroup
    {
        private final List<TargetMethodDelegate> methods = new ArrayList<>();
        private Optional<Predicate<SpecializeContext>> predicate = Optional.empty();
        private Optional<Function<SpecializeContext, List<Object>>> extraParametersFunction = Optional.empty();

        private MethodsGroup()
        {
        }

        List<TargetMethodDelegate> getMethods()
        {
            return copyOf(methods);
        }

        private void addMethod(TargetMethodDelegate method)
        {
            methods.add(method);
        }

        Optional<Predicate<SpecializeContext>> getPredicate()
        {
            return predicate;
        }

        private void setPredicate(Predicate<SpecializeContext> predicate)
        {
            this.predicate = Optional.of(predicate);
        }

        Optional<Function<SpecializeContext, List<Object>>> getExtraParametersFunction()
        {
            return extraParametersFunction;
        }

        private void setExtraParametersFunction(Function<SpecializeContext, List<Object>> extraParametersFunction)
        {
            this.extraParametersFunction = Optional.of(extraParametersFunction);
        }

        public boolean hasScalarMethodDelegates()
        {
            return methods.stream().anyMatch(method -> method.scalarFunctionDelegate.isPresent());
        }
    }
}
