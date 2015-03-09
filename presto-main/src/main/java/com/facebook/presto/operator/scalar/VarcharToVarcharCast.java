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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;

public class VarcharToVarcharCast
        extends SqlOperator
{
    private static final MethodHandle TRUNCATE_METHOD_HANDLE = methodHandle(VarcharToVarcharCast.class, "truncate", Slice.class, int.class);

    public static final VarcharToVarcharCast VARCHAR_TO_VARCHAR_CAST = new VarcharToVarcharCast();

    private VarcharToVarcharCast()
    {
        super(CAST, ImmutableList.of(comparableWithVariadicBound("F", VARCHAR), comparableWithVariadicBound("T", VARCHAR)), "T", ImmutableList.of("F"));
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        VarcharType fromType = (VarcharType) types.get("F");
        VarcharType toType = (VarcharType) types.get("T");

        MethodHandle methodHandle = getMethodHandle(fromType, toType);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, true);
    }

    private static MethodHandle getMethodHandle(VarcharType fromType, VarcharType toType)
    {
        if (toType.getLength().isPresent()) {
            int toLength = toType.getLength().getAsInt();
            if (toLength < fromType.getLength().orElse(Integer.MAX_VALUE)) {
                return MethodHandles.insertArguments(TRUNCATE_METHOD_HANDLE, 1, toLength);
            }
        }
        return MethodHandles.identity(Slice.class);
    }

    private static MethodHandle methodHandle(Class<?> clazz, String name, Class<?>... parameterTypes)
    {
        try {
            return MethodHandles.lookup().unreflect(clazz.getMethod(name, parameterTypes));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new PrestoException(INTERNAL_ERROR, e);
        }
    }

    public static Slice truncate(Slice slice, int length)
    {
        if (length <= 0) {
            return Slices.EMPTY_SLICE;
        }
        int indexEnd = offsetOfCodePoint(slice, length);
        if (indexEnd < 0) {
            return slice;
        }
        return slice.slice(0, indexEnd);
    }
}
