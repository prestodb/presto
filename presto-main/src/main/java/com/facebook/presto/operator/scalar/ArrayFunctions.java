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

//import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;
import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;

//import javax.annotation.Nullable;

public final class ArrayFunctions
        extends ParametricScalar
{
    private ArrayFunctions()
    {
    }

    @ScalarFunction(hidden = true)
    @SqlType("array<unknown>")
    public static Slice arrayConstructor()
    {
        return Slices.utf8Slice("[]");
    }

    private static final String FUNCTION_NAME = "contains";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(typeParameter("T")), "array<T>", ImmutableList.of("array<T>", "T"), false, false);

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
        return "Determines whether given value exists in given array; returns boolean";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");
        TypeSignature valueType = type.getTypeSignature();
        TypeSignature arrayType = parameterizedTypeName("array", valueType);
        TypeSignature returnType = parseTypeSignature(StandardTypes.BOOLEAN);
        MethodHandle methodHandle = methodHandle(JsonFunctions.class, "jsonArrayContains", Slice.class, type.getJavaType());
        Signature signature = new Signature(FUNCTION_NAME, returnType, arrayType, valueType);

        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false, false));
    }

    @Nullable
    public static Boolean equals(Type type, Slice slice, Slice value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        blockBuilder.writeBytes(slice, 0, slice.length());
        return JsonFunctions.jsonArrayContains(blockBuilder.closeEntry().build(), 0, value);
    }
/*
    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<bigint>") Slice slice, @SqlType(StandardTypes.BIGINT) long value)
    {
        return JsonFunctions.jsonArrayContains(slice, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<boolean>") Slice slice, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return JsonFunctions.jsonArrayContains(slice, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<double>") Slice slice, @SqlType(StandardTypes.DOUBLE) double value)
    {
        return JsonFunctions.jsonArrayContains(slice, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<varchar>") Slice slice, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return JsonFunctions.jsonArrayContains(slice, value);
    }*/
}
