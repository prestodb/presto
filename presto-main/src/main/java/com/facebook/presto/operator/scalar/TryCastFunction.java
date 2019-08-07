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
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty;
import com.facebook.presto.spi.function.FunctionFeature;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.relation.FullyQualifiedName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.metadata.BuiltInFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.spi.function.FunctionFeature.CAN_RETURN_NULL_FOR_NON_NULL_INPUT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.Sets.immutableEnumSet;
import static java.lang.invoke.MethodHandles.catchException;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodType.methodType;

public class TryCastFunction
        extends SqlScalarFunction
{
    public static final TryCastFunction TRY_CAST = new TryCastFunction();
    public static final String TRY_CAST_NAME = "TRY_CAST";

    public TryCastFunction()
    {
        super(new Signature(
                FullyQualifiedName.of(DEFAULT_NAMESPACE, TRY_CAST_NAME),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("F"), typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("F")),
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
    public Set<FunctionFeature> getFunctionFeatures()
    {
        return immutableEnumSet(CAN_RETURN_NULL_FOR_NON_NULL_INPUT);
    }

    @Override
    public String getDescription()
    {
        return "";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Type fromType = boundVariables.getTypeVariable("F");
        Type toType = boundVariables.getTypeVariable("T");

        Class<?> returnType = Primitives.wrap(toType.getJavaType());
        List<ArgumentProperty> argumentProperties;
        MethodHandle tryCastHandle;

        // the resulting method needs to return a boxed type
        FunctionHandle functionHandle = functionManager.lookupCast(CAST, fromType.getTypeSignature(), toType.getTypeSignature());
        ScalarFunctionImplementation implementation = functionManager.getScalarFunctionImplementation(functionHandle);
        argumentProperties = ImmutableList.of(implementation.getArgumentProperty(0));
        MethodHandle coercion = implementation.getMethodHandle();
        coercion = coercion.asType(methodType(returnType, coercion.type()));

        MethodHandle exceptionHandler = dropArguments(constant(returnType, null), 0, RuntimeException.class);
        tryCastHandle = catchException(coercion, RuntimeException.class, exceptionHandler);

        return new ScalarFunctionImplementation(true, argumentProperties, tryCastHandle);
    }
}
