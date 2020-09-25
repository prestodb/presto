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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class CastFromUnknownOperator
        extends SqlOperator
{
    public static final CastFromUnknownOperator CAST_FROM_UNKNOWN = new CastFromUnknownOperator();
    private static final MethodHandle METHOD_HANDLE_NON_NULL = methodHandle(CastFromUnknownOperator.class, "handleNonNull", boolean.class);

    public CastFromUnknownOperator()
    {
        super(CAST,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("E"),
                ImmutableList.of(parseTypeSignature("unknown")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(
            BoundVariables boundVariables, int arity,
            FunctionAndTypeManager functionAndTypeManager)
    {
        Type toType = boundVariables.getTypeVariable("E");
        MethodHandle methodHandle = METHOD_HANDLE_NON_NULL.asType(METHOD_HANDLE_NON_NULL.type().changeReturnType(toType.getJavaType()));
        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Object handleNonNull(boolean arg)
    {
        throw new IllegalArgumentException("value of unknown type should always be null");
    }
}
