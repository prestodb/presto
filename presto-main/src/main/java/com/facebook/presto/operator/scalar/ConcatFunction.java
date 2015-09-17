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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.add;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.invokeStatic;
import static com.facebook.presto.metadata.FunctionType.SCALAR;
import static com.facebook.presto.metadata.Signature.internalScalarFunction;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.Math.addExact;

public final class ConcatFunction
        implements ParametricFunction
{
    public static final ConcatFunction CONCAT = new ConcatFunction();

    private static final Signature SIGNATURE = new Signature("concat", SCALAR, ImmutableList.of(), StandardTypes.VARCHAR, ImmutableList.of(StandardTypes.VARCHAR), true);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
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
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments");
        }

        Class<?> clazz = generateConcat(arity);
        MethodHandle methodHandle = methodHandle(clazz, "concat", Collections.nCopies(arity, Slice.class).toArray(new Class<?>[arity]));
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(arity, false));

        Signature specializedSignature = internalScalarFunction(SIGNATURE.getName(), VARCHAR.getTypeSignature(), Collections.nCopies(arity, VARCHAR.getTypeSignature()));
        return new FunctionInfo(specializedSignature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, nullableParameters);
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
        ByteCodeBlock body = method.getBody();

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

    private static ByteCodeExpression generateCheckedAdd(ByteCodeExpression x, ByteCodeExpression y)
    {
        return invokeStatic(ConcatFunction.class, "checkedAdd", int.class, x, y);
    }

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
