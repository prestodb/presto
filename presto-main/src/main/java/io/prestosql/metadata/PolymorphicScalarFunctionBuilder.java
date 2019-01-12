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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.PolymorphicScalarFunction.PolymorphicScalarFunctionChoice;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class PolymorphicScalarFunctionBuilder
{
    private final Class<?> clazz;
    private Signature signature;
    private String description;
    private Optional<Boolean> hidden = Optional.empty();
    private Boolean deterministic;
    private final List<PolymorphicScalarFunctionChoice> choices = new ArrayList<>();

    public PolymorphicScalarFunctionBuilder(Class<?> clazz)
    {
        this.clazz = clazz;
    }

    public PolymorphicScalarFunctionBuilder signature(Signature signature)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.hidden = Optional.of(hidden.orElse(isOperator(signature)));
        return this;
    }

    public PolymorphicScalarFunctionBuilder description(String description)
    {
        this.description = description;
        return this;
    }

    public PolymorphicScalarFunctionBuilder hidden(boolean hidden)
    {
        this.hidden = Optional.of(hidden);
        return this;
    }

    public PolymorphicScalarFunctionBuilder deterministic(boolean deterministic)
    {
        this.deterministic = deterministic;
        return this;
    }

    public PolymorphicScalarFunctionBuilder choice(Function<ChoiceBuilder, ChoiceBuilder> choiceSpecification)
    {
        ChoiceBuilder choiceBuilder = new ChoiceBuilder(clazz, signature);
        choiceSpecification.apply(choiceBuilder);
        choices.add(choiceBuilder.build());
        return this;
    }

    public SqlScalarFunction build()
    {
        checkState(signature != null, "signature is null");
        checkState(deterministic != null, "deterministic is null");

        return new PolymorphicScalarFunction(
                signature,
                description,
                hidden.orElse(false),
                deterministic,
                choices);
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

    public static final class SpecializeContext
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
        private final Signature signature;
        private List<ArgumentProperty> argumentProperties;
        private final ImmutableList.Builder<MethodAndNativeContainerTypes> methodAndNativeContainerTypesList = ImmutableList.builder();

        private Optional<Function<SpecializeContext, List<Object>>> extraParametersFunction = Optional.empty();

        private MethodsGroupBuilder(Class<?> clazz, Signature signature, List<ArgumentProperty> argumentProperties)
        {
            this.clazz = clazz;
            this.signature = signature;
            this.argumentProperties = argumentProperties;
        }

        public MethodsGroupBuilder methods(String... methodNames)
        {
            return methods(asList(requireNonNull(methodNames, "methodNames is null")));
        }

        public MethodsGroupBuilder methods(List<String> methodNames)
        {
            requireNonNull(methodNames, "methodNames is null");
            checkArgument(!methodNames.isEmpty(), "methods list is empty");
            methodNames.forEach(methodName -> this.methodWithExplicitJavaTypes(methodName, nCopies(signature.getArgumentTypes().size(), Optional.empty())));
            return this;
        }

        public MethodsGroupBuilder withExtraParameters(Function<SpecializeContext, List<Object>> extraParametersFunction)
        {
            checkState(!methodAndNativeContainerTypesList.build().isEmpty(), "methods must be selected first");
            requireNonNull(extraParametersFunction, "extraParametersFunction is null");
            this.extraParametersFunction = Optional.of(extraParametersFunction);
            return this;
        }

        public MethodsGroupBuilder methodWithExplicitJavaTypes(String methodName, List<Optional<Class<?>>> types)
        {
            requireNonNull(methodName, "methodName is null");
            List<MethodAndNativeContainerTypes> matchingMethod = asList(clazz.getMethods()).stream()
                    .filter(method -> methodName.equals(method.getName()))
                    .map(method -> new MethodAndNativeContainerTypes(method, types))
                    .collect(toImmutableList());

            checkState(!matchingMethod.isEmpty(), "method %s was not found in %s", methodName, clazz);
            checkState(matchingMethod.size() == 1, "multiple methods %s was not found in %s", methodName, clazz);
            MethodAndNativeContainerTypes methodAndNativeContainerTypes = matchingMethod.get(0);
            int argumentSize = signature.getArgumentTypes().size();
            checkState(types.size() == argumentSize, "not matching number of arguments from signature: %s (should have %s)",
                    types.size(), argumentSize);
            checkState(types.size() == argumentProperties.size(), "not matching number of arguments from argument properties: %s (should have %s)",
                    types.size(), argumentProperties.size());
            Iterator<ArgumentProperty> argumentPropertyIterator = argumentProperties.iterator();
            Iterator<Optional<Class<?>>> typesIterator = types.iterator();
            while (argumentPropertyIterator.hasNext() && typesIterator.hasNext()) {
                Optional<Class<?>> classOptional = typesIterator.next();
                ArgumentProperty argumentProperty = argumentPropertyIterator.next();
                checkState((argumentProperty.getNullConvention() == BLOCK_AND_POSITION) == classOptional.isPresent(),
                        "Explicit type is not set when null convention is BLOCK_AND_POSITION");
            }
            methodAndNativeContainerTypesList.add(methodAndNativeContainerTypes);
            return this;
        }

        public MethodsGroup build()
        {
            return new MethodsGroup(methodAndNativeContainerTypesList.build(), extraParametersFunction);
        }
    }

    public static class ChoiceBuilder
    {
        private final Class<?> clazz;
        private final Signature signature;
        private boolean nullableResult;
        private List<ArgumentProperty> argumentProperties;
        private final ImmutableList.Builder<MethodsGroup> methodsGroups = ImmutableList.builder();

        private ChoiceBuilder(Class<?> clazz, Signature signature)
        {
            this.clazz = requireNonNull(clazz, "clazz is null");
            this.signature = requireNonNull(signature, "signature is null");
        }

        public ChoiceBuilder implementation(Function<MethodsGroupBuilder, MethodsGroupBuilder> methodsGroupSpecification)
        {
            // if the argumentProperties is not set yet. We assume it is set to the default value.
            if (argumentProperties == null) {
                argumentProperties = nCopies(signature.getArgumentTypes().size(), valueTypeArgumentProperty(RETURN_NULL_ON_NULL));
            }
            MethodsGroupBuilder methodsGroupBuilder = new MethodsGroupBuilder(clazz, signature, argumentProperties);
            methodsGroupSpecification.apply(methodsGroupBuilder);
            methodsGroups.add(methodsGroupBuilder.build());
            return this;
        }

        public ChoiceBuilder nullableResult(boolean nullableResult)
        {
            this.nullableResult = nullableResult;
            return this;
        }

        public ChoiceBuilder argumentProperties(ArgumentProperty... argumentProperties)
        {
            requireNonNull(argumentProperties, "argumentProperties is null");
            checkState(this.argumentProperties == null,
                    "The `argumentProperties` method must be invoked only once, and must be invoked before the `implementation` method");
            this.argumentProperties = ImmutableList.copyOf(argumentProperties);
            return this;
        }

        public PolymorphicScalarFunctionChoice build()
        {
            return new PolymorphicScalarFunctionChoice(nullableResult, argumentProperties, methodsGroups.build());
        }
    }

    static final class MethodsGroup
    {
        private final Optional<Function<SpecializeContext, List<Object>>> extraParametersFunction;
        private final List<MethodAndNativeContainerTypes> methodAndNativeContainerTypes;

        MethodsGroup(
                List<MethodAndNativeContainerTypes> methodAndNativeContainerTypes,
                Optional<Function<SpecializeContext, List<Object>>> extraParametersFunction)
        {
            this.methodAndNativeContainerTypes = requireNonNull(methodAndNativeContainerTypes, "methodAndNativeContainerTypes is null");
            this.extraParametersFunction = requireNonNull(extraParametersFunction, "extraParametersFunction is null");
        }

        List<MethodAndNativeContainerTypes> getMethods()
        {
            return methodAndNativeContainerTypes;
        }

        Optional<Function<SpecializeContext, List<Object>>> getExtraParametersFunction()
        {
            return extraParametersFunction;
        }
    }

    static class MethodAndNativeContainerTypes
    {
        private final Method method;
        private List<Optional<Class<?>>> explicitNativeContainerTypes;

        MethodAndNativeContainerTypes(Method method, List<Optional<Class<?>>> explicitNativeContainerTypes)
        {
            this.method = method;
            this.explicitNativeContainerTypes = explicitNativeContainerTypes;
        }

        public Method getMethod()
        {
            return method;
        }

        List<Optional<Class<?>>> getExplicitNativeContainerTypes()
        {
            return explicitNativeContainerTypes;
        }

        void setExplicitNativeContainerTypes(List<Optional<Class<?>>> explicitNativeContainerTypes)
        {
            this.explicitNativeContainerTypes = explicitNativeContainerTypes;
        }
    }
}
