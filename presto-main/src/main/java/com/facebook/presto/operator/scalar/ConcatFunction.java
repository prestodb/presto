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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.CompilerUtils;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.Math.addExact;

public final class ConcatFunction
        extends SqlScalarFunction
{
    public static final ConcatFunction CONCAT = new ConcatFunction();

    public ConcatFunction()
    {
        // TODO design new variadic functions binding mechanism that will allow to produce VARCHAR(x) where x < MAX_LENGTH.
        super(new Signature(
                "concat",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                createUnboundedVarcharType().getTypeSignature(),
                ImmutableList.of(createUnboundedVarcharType().getTypeSignature()),
                true));
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
        return "concatenates given strings";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments");
        }

        Class<?> clazz = generateConcat(arity);
        MethodHandle methodHandle = methodHandle(clazz, "concat", Collections.nCopies(arity, Slice.class).toArray(new Class<?>[arity]));
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(arity, false));

        return new ScalarFunctionImplementation(false, nullableParameters, methodHandle, isDeterministic());
    }

    private static Class<?> generateConcat(int arity)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName("Concat" + arity + "ScalarFunction"),
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
