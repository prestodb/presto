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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class VarcharToVarcharCast
        extends SqlOperator
{
    private static final MethodHandle TRUNCATE_METHOD_HANDLE = methodHandle(VarcharToVarcharCast.class, "truncate", Slice.class, int.class);

    public static final VarcharToVarcharCast VARCHAR_TO_VARCHAR_CAST = new VarcharToVarcharCast();

    private VarcharToVarcharCast()
    {
        super(CAST,
                ImmutableList.of(comparableWithVariadicBound("F", VARCHAR), comparableWithVariadicBound("T", VARCHAR)),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("F")));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        VarcharType fromType = (VarcharType) boundVariables.getTypeVariable("F");
        VarcharType toType = (VarcharType) boundVariables.getTypeVariable("T");

        MethodHandle methodHandle = getMethodHandle(fromType, toType);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, true);
    }

    private static MethodHandle getMethodHandle(VarcharType fromType, VarcharType toType)
    {
        if (toType.getLength() < fromType.getLength()) {
            return MethodHandles.insertArguments(TRUNCATE_METHOD_HANDLE, 1, toType.getLength());
        }
        return MethodHandles.identity(Slice.class);
    }

    public static Slice truncate(Slice slice, int length)
    {
        if (length < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Length smaller then zero");
        }
        return truncateToLength(slice, length);
    }
}
