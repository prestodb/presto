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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.lambda.UnaryFunctionInterface;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.primitives.Primitives.wrap;

/**
 * This scalar function exists primarily to test lambda expression support.
 */
public final class ApplyFunction
        extends SqlScalarFunction
{
    public static final ApplyFunction APPLY_FUNCTION = new ApplyFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ApplyFunction.class, "apply", Object.class, UnaryFunctionInterface.class);

    private ApplyFunction()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "apply"),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("U")),
                ImmutableList.of(),
                parseTypeSignature("U"),
                ImmutableList.of(parseTypeSignature("T"), parseTypeSignature("function(T,U)")),
                false));
    }

    @Override
    public final SqlFunctionVisibility getVisibility()
    {
        return HIDDEN;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "lambda apply function";
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type argumentType = boundVariables.getTypeVariable("T");
        Type returnType = boundVariables.getTypeVariable("U");
        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        functionTypeArgumentProperty(UnaryFunctionInterface.class)),
                METHOD_HANDLE.asType(
                        METHOD_HANDLE.type()
                                .changeReturnType(wrap(returnType.getJavaType()))
                                .changeParameterType(0, wrap(argumentType.getJavaType()))));
    }

    public static Object apply(Object input, UnaryFunctionInterface function)
    {
        return function.apply(input);
    }
}
