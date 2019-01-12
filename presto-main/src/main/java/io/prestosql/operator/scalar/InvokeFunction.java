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
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.lambda.LambdaFunctionInterface;

import java.lang.invoke.MethodHandle;

import static com.google.common.primitives.Primitives.wrap;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;

/**
 * This scalar function exists primarily to test lambda expression support.
 */
public final class InvokeFunction
        extends SqlScalarFunction
{
    public static final InvokeFunction INVOKE_FUNCTION = new InvokeFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(InvokeFunction.class, "invoke", InvokeLambda.class);

    private InvokeFunction()
    {
        super(new Signature(
                "invoke",
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
        return "lambda invoke function";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type returnType = boundVariables.getTypeVariable("T");
        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(functionTypeArgumentProperty(InvokeLambda.class)),
                METHOD_HANDLE.asType(
                        METHOD_HANDLE.type()
                                .changeReturnType(wrap(returnType.getJavaType()))),
                isDeterministic());
    }

    public static Object invoke(InvokeLambda function)
    {
        return function.apply();
    }

    @FunctionalInterface
    public interface InvokeLambda
            extends LambdaFunctionInterface
    {
        Object apply();
    }
}
