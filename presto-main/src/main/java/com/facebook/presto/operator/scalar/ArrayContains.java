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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;

@Description("Determines whether given value exists in the array")
@ScalarFunction("contains")
public final class ArrayContains
{
    private ArrayContains() {}

    @SqlType(StandardTypes.BOOLEAN)
    @Nullable
    public static Boolean arrayWithUnknownType(@SqlType("array(unknown)") Block arrayBlock, @Nullable @SqlType("unknown") Void value)
    {
        return null;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @Nullable
    public static Boolean contains(@TypeParameter("T") Type elementType,
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
                if ((boolean) equals.invokeExact((Block) elementType.getObject(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @Nullable
    public static Boolean contains(@TypeParameter("T") Type elementType,
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
                if ((boolean) equals.invokeExact(elementType.getSlice(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @Nullable
    public static Boolean contains(@TypeParameter("T") Type elementType,
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
                if ((boolean) equals.invokeExact(elementType.getLong(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @Nullable
    public static Boolean contains(@TypeParameter("T") Type elementType,
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
                if ((boolean) equals.invokeExact(elementType.getBoolean(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @Nullable
    public static Boolean contains(@TypeParameter("T") Type elementType,
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
                if ((boolean) equals.invokeExact(elementType.getDouble(arrayBlock, i), value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }
}
