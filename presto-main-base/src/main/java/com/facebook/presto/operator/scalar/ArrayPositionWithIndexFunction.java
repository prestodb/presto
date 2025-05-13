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
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.Failures.internalError;

@Description("Returns the position of the first occurrence of the given value in array (or 0 if not found)")
@ScalarFunction("array_position")
public class ArrayPositionWithIndexFunction
{
    protected ArrayPositionWithIndexFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long arrayPositionWithIndex(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") boolean element,
            @SqlType(StandardTypes.BIGINT) long instance)
    {
        int size = array.getPositionCount();
        int instancesFound = 0;

        if (instance == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "0 is an invalid instance position for array_position.");
        }

        int startIndex = instance > 0 ? 0 : size - 1;
        int stopIndex = instance > 0 ? size : -1;
        int stepSize = instance > 0 ? 1 : -1;
        instance = Math.abs(instance);

        for (int i = startIndex; i != stopIndex; i += stepSize) {
            if (!array.isNull(i)) {
                try {
                    boolean arrayValue = type.getBoolean(array, i);
                    Boolean result = (Boolean) equalMethodHandle.invoke(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        instancesFound++;
                        if (instancesFound == instance) {
                            return i + 1; // result is 1-based (instead of 0)
                        }
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
    public static long arrayPositionWithIndex(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") long element,
            @SqlType(StandardTypes.BIGINT) long instance)
    {
        int size = array.getPositionCount();
        int instancesFound = 0;

        if (instance == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "array_position cannot take a 0-valued instance argument.");
        }

        int startIndex = instance > 0 ? 0 : size - 1;
        int stopIndex = instance > 0 ? size : -1;
        int stepSize = instance > 0 ? 1 : -1;
        instance = Math.abs(instance);

        for (int i = startIndex; i != stopIndex; i += stepSize) {
            if (!array.isNull(i)) {
                try {
                    long arrayValue = type.getLong(array, i);
                    Boolean result = (Boolean) equalMethodHandle.invoke(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        instancesFound++;
                        if (instancesFound == instance) {
                            return i + 1; // result is 1-based (instead of 0)
                        }
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
    public static long arrayPositionWithIndex(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") double element,
            @SqlType(StandardTypes.BIGINT) long instance)
    {
        int size = array.getPositionCount();
        int instancesFound = 0;

        if (instance == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "array_position cannot take a 0-valued instance argument.");
        }

        int startIndex = instance > 0 ? 0 : size - 1;
        int stopIndex = instance > 0 ? size : -1;
        int stepSize = instance > 0 ? 1 : -1;
        instance = Math.abs(instance);

        for (int i = startIndex; i != stopIndex; i += stepSize) {
            if (!array.isNull(i)) {
                try {
                    double arrayValue = type.getDouble(array, i);
                    Boolean result = (Boolean) equalMethodHandle.invoke(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        instancesFound++;
                        if (instancesFound == instance) {
                            return i + 1; // result is 1-based (instead of 0)
                        }
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
    public static long arrayPositionWithIndex(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") Slice element,
            @SqlType(StandardTypes.BIGINT) long instance)
    {
        int size = array.getPositionCount();
        int instancesFound = 0;

        if (instance == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "array_position cannot take a 0-valued instance argument.");
        }

        int startIndex = instance > 0 ? 0 : size - 1;
        int stopIndex = instance > 0 ? size : -1;
        int stepSize = instance > 0 ? 1 : -1;
        instance = Math.abs(instance);

        for (int i = startIndex; i != stopIndex; i += stepSize) {
            if (!array.isNull(i)) {
                try {
                    Slice arrayValue = type.getSlice(array, i);
                    Boolean result = (Boolean) equalMethodHandle.invoke(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        instancesFound++;
                        if (instancesFound == instance) {
                            return i + 1; // result is 1-based (instead of 0)
                        }
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
    public static long arrayPositionWithIndex(
            @TypeParameter("T") Type type,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"T", "T"}) MethodHandle equalMethodHandle,
            @SqlType("array(T)") Block array,
            @SqlType("T") Block element,
            @SqlType(StandardTypes.BIGINT) long instance)
    {
        int size = array.getPositionCount();
        int instancesFound = 0;

        if (instance == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "array_position cannot take a 0-valued instance argument.");
        }

        int startIndex = instance > 0 ? 0 : size - 1;
        int stopIndex = instance > 0 ? size : -1;
        int stepSize = instance > 0 ? 1 : -1;
        instance = Math.abs(instance);

        for (int i = startIndex; i != stopIndex; i += stepSize) {
            if (!array.isNull(i)) {
                try {
                    Object arrayValue = type.getObject(array, i);
                    Boolean result = (Boolean) equalMethodHandle.invoke(arrayValue, element);
                    checkNotIndeterminate(result);
                    if (result) {
                        instancesFound++;
                        if (instancesFound == instance) {
                            return i + 1; // result is 1-based (instead of 0)
                        }
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
