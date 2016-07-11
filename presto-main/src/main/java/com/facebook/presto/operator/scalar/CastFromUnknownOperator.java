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
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class CastFromUnknownOperator
        extends SqlOperator
{
    public static final CastFromUnknownOperator CAST_FROM_UNKNOWN = new CastFromUnknownOperator();
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(CastFromUnknownOperator.class, "toLong", Void.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(CastFromUnknownOperator.class, "toDouble", Void.class);
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(CastFromUnknownOperator.class, "toBoolean", Void.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(CastFromUnknownOperator.class, "toSlice", Void.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(CastFromUnknownOperator.class, "toObject", Void.class);

    public CastFromUnknownOperator()
    {
        super(CAST,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("E"),
                ImmutableList.of(parseTypeSignature("unknown")));
    }

    @Override
    public ScalarFunctionImplementation specialize(
            BoundVariables boundVariables, int arity, TypeManager typeManager,
            FunctionRegistry functionRegistry)
    {
        Type toType = boundVariables.getTypeVariable("E");
        MethodHandle methodHandle;
        if (toType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (toType.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (toType.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (toType.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT.asType(METHOD_HANDLE_OBJECT.type().changeReturnType(toType.getJavaType()));
        }
        return new ScalarFunctionImplementation(true, ImmutableList.of(true), methodHandle, isDeterministic());
    }

    @UsedByGeneratedCode
    public static Long toLong(Void arg)
    {
        return null;
    }

    @UsedByGeneratedCode
    public static Double toDouble(Void arg)
    {
        return null;
    }

    @UsedByGeneratedCode
    public static Boolean toBoolean(Void arg)
    {
        return null;
    }

    @UsedByGeneratedCode
    public static Slice toSlice(Void arg)
    {
        return null;
    }

    @UsedByGeneratedCode
    public static Object toObject(Void arg)
    {
        return null;
    }
}
