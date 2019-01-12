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

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.util.Failures.internalError;

@Description("Determines whether given value exists in the array")
@ScalarFunction("contains")
public final class ArrayContains
{
    private ArrayContains() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") Block value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact((Block) elementType.getObject(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") Slice value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(elementType.getSlice(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") long value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(elementType.getLong(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") boolean value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(elementType.getBoolean(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(
            @TypeParameter("T") Type elementType,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("T") double value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                Boolean result = (Boolean) equals.invokeExact(elementType.getDouble(arrayBlock, i), value);
                checkNotIndeterminate(result);
                if (result) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    private static void checkNotIndeterminate(Boolean equalsResult)
    {
        if (equalsResult == null) {
            throw new PrestoException(NOT_SUPPORTED, "contains does not support arrays with elements that are null or contain null");
        }
    }
}
