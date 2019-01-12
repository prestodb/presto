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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.prestosql.metadata.Signature.internalOperator;
import static io.prestosql.metadata.Signature.orderableTypeParameter;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.gen.BytecodeUtils.invoke;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public abstract class AbstractGreatestLeast
        extends SqlScalarFunction
{
    private static final MethodHandle CHECK_NOT_NAN = methodHandle(AbstractGreatestLeast.class, "checkNotNaN", String.class, double.class);

    private final OperatorType operatorType;

    protected AbstractGreatestLeast(String name, OperatorType operatorType)
    {
        super(new Signature(
                name,
                FunctionKind.SCALAR,
                ImmutableList.of(orderableTypeParameter("E")),
                ImmutableList.of(),
                parseTypeSignature("E"),
                ImmutableList.of(parseTypeSignature("E")),
                true));
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("E");
        checkArgument(type.isOrderable(), "Type must be orderable");

        MethodHandle compareMethod = functionRegistry.getScalarFunctionImplementation(internalOperator(operatorType, BOOLEAN, ImmutableList.of(type, type))).getMethodHandle();

        List<Class<?>> javaTypes = IntStream.range(0, arity)
                .mapToObj(i -> type.getJavaType())
                .collect(toImmutableList());

        Class<?> clazz = generate(javaTypes, type, compareMethod);
        MethodHandle methodHandle = methodHandle(clazz, getSignature().getName(), javaTypes.toArray(new Class<?>[javaTypes.size()]));

        return new ScalarFunctionImplementation(
                false,
                nCopies(javaTypes.size(), valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static void checkNotNaN(String name, double value)
    {
        if (Double.isNaN(value)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid argument to %s(): NaN", name));
        }
    }

    private Class<?> generate(List<Class<?>> javaTypes, Type type, MethodHandle compareMethod)
    {
        checkCondition(javaTypes.size() <= 127, NOT_SUPPORTED, "Too many arguments for function call %s()", getSignature().getName());
        String javaTypeName = javaTypes.stream()
                .map(Class::getSimpleName)
                .collect(joining());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(javaTypeName + "$" + getSignature().getName()),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        List<Parameter> parameters = IntStream.range(0, javaTypes.size())
                .mapToObj(i -> arg("arg" + i, javaTypes.get(i)))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                getSignature().getName(),
                type(javaTypes.get(0)),
                parameters);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        CallSiteBinder binder = new CallSiteBinder();

        if (type.getTypeSignature().getBase().equals(StandardTypes.DOUBLE)) {
            for (Parameter parameter : parameters) {
                body.append(parameter);
                body.append(invoke(binder.bind(CHECK_NOT_NAN.bindTo(getSignature().getName())), "checkNotNaN"));
            }
        }

        Variable value = scope.declareVariable(javaTypes.get(0), "value");

        body.append(value.set(parameters.get(0)));

        for (int i = 1; i < javaTypes.size(); i++) {
            body.append(new IfStatement()
                    .condition(new BytecodeBlock()
                            .append(parameters.get(i))
                            .append(value)
                            .append(invoke(binder.bind(compareMethod), "compare")))
                    .ifTrue(value.set(parameters.get(i))));
        }

        body.append(value.ret());

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(getClass().getClassLoader()));
    }
}
