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
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Math.addExact;
import static java.util.Collections.nCopies;

public final class ConcatFunction
        extends SqlScalarFunction
{
    // TODO design new variadic functions binding mechanism that will allow to produce VARCHAR(x) where x < MAX_LENGTH.
    public static final ConcatFunction VARCHAR_CONCAT = new ConcatFunction(createUnboundedVarcharType().getTypeSignature(), "concatenates given strings");

    public static final ConcatFunction VARBINARY_CONCAT = new ConcatFunction(VARBINARY.getTypeSignature(), "concatenates given varbinary values");

    private final String description;

    private ConcatFunction(TypeSignature type, String description)
    {
        super(new Signature(
                "concat",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                type,
                ImmutableList.of(type),
                true));
        this.description = description;
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
    public String getDescription()
    {
        return description;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments");
        }

        Class<?> clazz = generateConcat(getSignature().getReturnType(), arity);
        MethodHandle methodHandle = methodHandle(clazz, "concat", nCopies(arity, Slice.class).toArray(new Class<?>[arity]));

        return new ScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    private static Class<?> generateConcat(TypeSignature type, int arity)
    {
        checkCondition(arity <= 254, NOT_SUPPORTED, "Too many arguments for string concatenation");
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(type.getBase() + "_concat" + arity + "ScalarFunction"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate concat()
        List<Parameter> parameters = IntStream.range(0, arity)
                .mapToObj(i -> arg("arg" + i, Slice.class))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "concat", type(Slice.class), parameters);
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable length = scope.declareVariable(int.class, "length");
        body.append(length.set(constantInt(0)));

        for (int i = 0; i < arity; ++i) {
            body.append(length.set(generateCheckedAdd(length, parameters.get(i).invoke("length", int.class))));
        }

        Variable result = scope.declareVariable(Slice.class, "result");
        body.append(result.set(invokeStatic(Slices.class, "allocate", Slice.class, length)));

        Variable position = scope.declareVariable(int.class, "position");
        body.append(position.set(constantInt(0)));

        for (int i = 0; i < arity; ++i) {
            body.append(result.invoke("setBytes", void.class, position, parameters.get(i)));
            body.append(position.set(add(position, parameters.get(i).invoke("length", int.class))));
        }

        body.getVariable(result)
                .retObject();

        return defineClass(definition, Object.class, ImmutableMap.of(), new DynamicClassLoader(ConcatFunction.class.getClassLoader()));
    }

    private static BytecodeExpression generateCheckedAdd(BytecodeExpression x, BytecodeExpression y)
    {
        return invokeStatic(ConcatFunction.class, "checkedAdd", int.class, x, y);
    }

    @UsedByGeneratedCode
    public static int checkedAdd(int x, int y)
    {
        try {
            return addExact(x, y);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
        }
    }
}
