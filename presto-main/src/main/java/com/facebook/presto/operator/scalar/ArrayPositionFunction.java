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
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;

@Description("Returns the position of the first occurrence of the given value in array (or 0 if not found)")
@ScalarFunction("array_position")
public final class ArrayPositionFunction
{
    private ArrayPositionFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(@TypeParameter("T") Type type,
                                     @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
                                     @SqlType("array(T)") Block array,
                                     @SqlType("T") boolean element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                boolean arrayValue = type.getBoolean(array, i);
                try {
                    if ((boolean) equalMethodHandle.invokeExact(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(@TypeParameter("T") Type type,
                                     @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
                                     @SqlType("array(T)") Block array,
                                     @SqlType("T") long element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                long arrayValue = type.getLong(array, i);
                try {
                    if ((boolean) equalMethodHandle.invokeExact(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(@TypeParameter("T") Type type,
                                     @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
                                     @SqlType("array(T)") Block array,
                                     @SqlType("T") double element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                double arrayValue = type.getDouble(array, i);
                try {
                    if ((boolean) equalMethodHandle.invokeExact(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(@TypeParameter("T") Type type,
                                     @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
                                     @SqlType("array(T)") Block array,
                                     @SqlType("T") Slice element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                Slice arrayValue = type.getSlice(array, i);
                try {
                    if ((boolean) equalMethodHandle.invokeExact(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(@TypeParameter("T") Type type,
                                     @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
                                     @SqlType("array(T)") Block array,
                                     @SqlType("T") Block element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                Object arrayValue = type.getObject(array, i);
                try {
                    if ((boolean) equalMethodHandle.invoke(arrayValue, element)) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
                }
            }
        }
        return 0;
    }

    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long arrayPosition(@SqlType("array(unknown)") Block array, @SqlNullable @SqlType("unknown") Void element)
    {
        return null;
    }
}
