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
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.util.Failures.internalError;

@Description("Returns the position of the first occurrence of the given value in array (or 0 if not found)")
@ScalarFunction("array_position")
public final class ArrayPositionFunction
{
    private ArrayPositionFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") boolean element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                boolean arrayValue = type.getBoolean(array, i);
                try {
                    Boolean result = (Boolean) equalMethodHandle.invokeExact(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
            }
        }
        return 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") long element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                long arrayValue = type.getLong(array, i);
                try {
                    Boolean result = (Boolean) equalMethodHandle.invokeExact(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
            }
        }
        return 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") double element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                double arrayValue = type.getDouble(array, i);
                try {
                    Boolean result = (Boolean) equalMethodHandle.invokeExact(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
            }
        }
        return 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") Slice element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                Slice arrayValue = type.getSlice(array, i);
                try {
                    Boolean result = (Boolean) equalMethodHandle.invokeExact(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
            }
        }
        return 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPosition(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") Block element)
    {
        int size = array.getPositionCount();
        for (int i = 0; i < size; i++) {
            if (!array.isNull(i)) {
                Object arrayValue = type.getObject(array, i);
                try {
                    Boolean result = (Boolean) equalMethodHandle.invoke(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        return i + 1; // result is 1-based (instead of 0)
                    }
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
            }
        }
        return 0;
    }

    private static void checkNotIndeterminate(Boolean equalsResult)
    {
        if (equalsResult == null) {
            throw new PrestoException(NOT_SUPPORTED, "array_position does not support arrays with elements that are null or contain null");
        }
    }
}
