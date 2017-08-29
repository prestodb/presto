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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.lambda.LambdaFunctionInterface;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.primitives.Primitives.wrap;

public final class TryFunction
        extends SqlScalarFunction
{
    public static final TryFunction TRY_FUNCTION = new TryFunction();
    public static final String TRY_FUNCTION_NAME = "$internal$try_function";
    private static final MethodHandle METHOD_HANDLE = methodHandle(TryFunction.class, "apply", TryFunction.TryLambda.class);

    public TryFunction()
    {
        super(new Signature(
                TRY_FUNCTION_NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("function(T)")),
                false));
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
        return "try function";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type returnType = boundVariables.getTypeVariable("T");
        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(false),
                ImmutableList.of(false),
                ImmutableList.of(Optional.of(TryFunction.TryLambda.class)),
                METHOD_HANDLE.asType(
                        METHOD_HANDLE.type()
                                .changeReturnType(wrap(returnType.getJavaType()))),
                isDeterministic());
    }

    public static Object apply(TryFunction.TryLambda function)
    {
        try {
            return function.apply();
        }
        catch (Throwable throwable) {
            return null;
        }
    }

    @FunctionalInterface
    public interface TryLambda
            extends LambdaFunctionInterface
    {
        Object apply();
    }
}
