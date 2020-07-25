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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.Failures.internalError;

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
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
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
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
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
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
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
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
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
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equals,
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
