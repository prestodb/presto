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
package com.facebook.presto.type;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.CompilerUtils;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.DIVIDE;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.MODULUS;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorType.NEGATION;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.metadata.SignatureBinder.bindVariables;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.util.Arrays.fill;

public final class UnknownOperators
{
    public static final SqlScalarFunction[] UNKNOWN_OPERATORS = defineUnknownTypeOperators();

    private UnknownOperators()
    {
    }

    private static SqlScalarFunction[] defineUnknownTypeOperators()
    {
        ImmutableList.Builder<SqlScalarFunction> operators = ImmutableList.builder();

        // arithmetic binary
        operators.addAll(createArithmeticOperators(ADD));
        operators.addAll(createArithmeticOperators(SUBTRACT));
        operators.addAll(createArithmeticOperators(MULTIPLY));
        operators.addAll(createArithmeticOperators(DIVIDE));
        operators.addAll(createArithmeticOperators(MODULUS));

        // arithmetic unary
        operators.add(createUnknownOperator(NEGATION, UnknownType.NAME, UnknownType.NAME));
        operators.add(createUnknownOperator(HASH_CODE, BIGINT, UnknownType.NAME));

        // inequality binary operators
        operators.addAll(createEqualityOperators(EQUAL));
        operators.addAll(createEqualityOperators(NOT_EQUAL));
        operators.addAll(createEqualityOperators(LESS_THAN));
        operators.addAll(createEqualityOperators(LESS_THAN_OR_EQUAL));
        operators.addAll(createEqualityOperators(GREATER_THAN));
        operators.addAll(createEqualityOperators(GREATER_THAN_OR_EQUAL));

        // inequality ternary operators
        operators.addAll(createBetweenOperators());

        // concat with unknown operator
        operators.addAll(createConcatWithUnknownOperators());

        // length of unknown operator
        operators.add(createUnknownFunction("length", BIGINT, UnknownType.NAME));

        // unknown operators for json types
        operators.addAll(createJsonUnknownOperators());

        ImmutableList<SqlScalarFunction> list = operators.build();
        return list.toArray(new SqlScalarFunction[list.size()]);
    }

    private static List<SqlScalarFunction> createArithmeticOperators(OperatorType operatorType)
    {
        return ImmutableList.of(
                createUnknownOperator(operatorType, UnknownType.NAME, UnknownType.NAME, UnknownType.NAME),
                createUnknownOperator(operatorType, "T", UnknownType.NAME, "T"),
                createUnknownOperator(operatorType, "T", "T", UnknownType.NAME)
        );
    }

    private static List<SqlScalarFunction> createEqualityOperators(OperatorType operatorType)
    {
        return ImmutableList.of(
                createUnknownOperator(operatorType, BOOLEAN, UnknownType.NAME, UnknownType.NAME),
                createUnknownOperator(operatorType, BOOLEAN, UnknownType.NAME, "T"),
                createUnknownOperator(operatorType, BOOLEAN, "T", UnknownType.NAME)
        );
    }

    private static List<SqlScalarFunction> createBetweenOperators()
    {
        return ImmutableList.of(
                createUnknownOperator(BETWEEN, BOOLEAN, UnknownType.NAME, UnknownType.NAME, UnknownType.NAME),
                createUnknownOperator(BETWEEN, BOOLEAN, UnknownType.NAME, "T", "T"),
                createUnknownOperator(BETWEEN, BOOLEAN, "T", UnknownType.NAME, "T"),
                createUnknownOperator(BETWEEN, BOOLEAN, "T", "T", UnknownType.NAME)
        );
    }

    private static List<SqlScalarFunction> createConcatWithUnknownOperators()
    {
        return ImmutableList.of(
                createUnknownFunction("json_array_length", BIGINT, UnknownType.NAME),
                createUnknownFunction("json_array_contains", BOOLEAN, UnknownType.NAME, UnknownType.NAME),
                createUnknownFunction("json_array_contains", BOOLEAN, UnknownType.NAME, "T"),
                createUnknownFunction("json_array_contains", BOOLEAN, "T", UnknownType.NAME),
                createUnknownFunction("json_size", BIGINT, UnknownType.NAME, UnknownType.NAME),
                createUnknownFunction("json_size", BIGINT, UnknownType.NAME, "T"),
                createUnknownFunction("json_size", BIGINT, "T", UnknownType.NAME)
        );
    }

    private static List<SqlScalarFunction> createJsonUnknownOperators()
    {
        return ImmutableList.of(
                createUnknownFunction("concat", UnknownType.NAME, UnknownType.NAME, UnknownType.NAME),
                createUnknownFunction("concat", VARCHAR, UnknownType.NAME, VARCHAR),
                createUnknownFunction("concat", VARCHAR, VARCHAR, UnknownType.NAME)
        );
    }

    private static SqlScalarFunction createUnknownOperator(
            OperatorType operatorType,
            String returnType,
            String... argumentTypes)
    {
        return createUnknownFunction(mangleOperatorName(operatorType), returnType, argumentTypes);
    }

    private static SqlScalarFunction createUnknownFunction(
            String functionName,
            String returnType,
            String... argumentTypes)
    {
        ImmutableList<String> argumentTypesList = ImmutableList.copyOf(argumentTypes);
        List<TypeVariableConstraint> typeVariables = ImmutableList.of();
        if (argumentTypesList.contains("T")) {
            typeVariables = ImmutableList.of(typeVariable("T"));
        }
        return new UnknownFunction(functionName, typeVariables, returnType, argumentTypesList);
    }

    private static final class UnknownFunction
            extends SqlScalarFunction
    {
        public UnknownFunction(
                String functionName,
                List<TypeVariableConstraint> typeVariables,
                String returnType,
                List<String> argumentTypes)
        {
            super(functionName, typeVariables, ImmutableList.of(), returnType, argumentTypes, false);
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            List<TypeSignature> resolvedParameterTypeSignatures = bindVariables(getSignature().getArgumentTypes(), boundVariables);
            List<Type> resolvedParameterTypes = resolveTypes(resolvedParameterTypeSignatures, typeManager);
            TypeSignature resolvedReturnTypeSignature = bindVariables(getSignature().getReturnType(), boundVariables);
            Type resolvedReturnType = typeManager.getType(resolvedReturnTypeSignature);

            List<Class<?>> parametersJavaTypes = resolvedParameterTypes.stream()
                    .map((prestoType) -> Primitives.wrap(prestoType.getJavaType()))
                    .collect(Collectors.toList());
            Class<?> returnJavaType = Primitives.wrap(resolvedReturnType.getJavaType());

            ImplementationSignature implementationSignature = new ImplementationSignature(returnJavaType, parametersJavaTypes);
            MethodHandle implementation = getFunctionImplementation(implementationSignature);

            Boolean[] nullableArguments = new Boolean[resolvedParameterTypes.size()];
            fill(nullableArguments, Boolean.TRUE);

            return new ScalarFunctionImplementation(true, ImmutableList.copyOf(nullableArguments), implementation, true);
        }

        @Override
        public boolean isHidden()
        {
            return true;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public String getDescription()
        {
            return "Unknown operator";
        }
    }

    private static Cache<ImplementationSignature, MethodHandle> implementations = CacheBuilder.newBuilder().build();

    private static MethodHandle getFunctionImplementation(ImplementationSignature implementationSignature)
    {
        try {
            return implementations.get(implementationSignature, () -> generateFunctionImplementation(implementationSignature));
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private static MethodHandle generateFunctionImplementation(ImplementationSignature implementationSignature)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName("Unknown operator " + implementationSignature + " function"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate operator()
        AtomicInteger index = new AtomicInteger();
        List<Class<?>> argumentTypes = implementationSignature.getArgumentTypes();
        List<Parameter> parameters = argumentTypes.stream()
                .map(clazz -> arg("arg" + index.incrementAndGet(), clazz))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "operator",
                type(implementationSignature.getReturnType()), parameters);
        BytecodeBlock body = method.getBody();
        body.pushNull();
        body.retObject();

        Class<?> generatedClazz = defineClass(
                definition,
                Object.class,
                ImmutableMap.of(),
                new DynamicClassLoader(UnknownOperators.class.getClassLoader()));

        return methodHandle(generatedClazz, "operator", argumentTypes.toArray(new Class[argumentTypes.size()]));
    }

    private static class ImplementationSignature
    {
        private final Class<?> returnType;
        private final List<Class<?>> argumentTypes;

        private ImplementationSignature(Class<?> returnType, List<Class<?>> argumentTypes)
        {
            this.returnType = returnType;
            this.argumentTypes = ImmutableList.copyOf(argumentTypes);
        }

        public Class<?> getReturnType()
        {
            return returnType;
        }

        public List<Class<?>> getArgumentTypes()
        {
            return argumentTypes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ImplementationSignature that = (ImplementationSignature) o;
            return Objects.equals(returnType, that.returnType) &&
                    Objects.equals(argumentTypes, that.argumentTypes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(returnType, argumentTypes);
        }

        @Override
        public String toString()
        {
            return "ImplementationSignature{" +
                    "returnType=" + returnType +
                    ", argumentTypes=" + argumentTypes +
                    '}';
        }
    }
}
