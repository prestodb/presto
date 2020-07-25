package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.util.Failures.internalError;

@ScalarOperator(EQUAL)
public final class ArrayEqualOperator
{
    private ArrayEqualOperator() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equals(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        boolean indeterminate = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i) || rightArray.isNull(i)) {
                indeterminate = true;
                continue;
            }
            Object leftElement = readNativeValue(type, leftArray, i);
            Object rightElement = readNativeValue(type, rightArray, i);
            try {
                Boolean result = (Boolean) equalsFunction.invoke(leftElement, rightElement);
                if (result == null) {
                    indeterminate = true;
                }
                else if (!result) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }

        if (indeterminate) {
            return null;
        }
        return true;
    }

    @TypeParameter("E")
    @TypeParameterSpecialization(name = "E", nativeContainerType = long.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equalsLong(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        boolean indeterminate = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i) || rightArray.isNull(i)) {
                indeterminate = true;
                continue;
            }
            long leftElement = type.getLong(leftArray, i);
            long rightElement = type.getLong(rightArray, i);
            try {
                Boolean result = (Boolean) equalsFunction.invokeExact(leftElement, rightElement);
                if (result == null) {
                    indeterminate = true;
                }
                else if (!result) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }

        if (indeterminate) {
            return null;
        }
        return true;
    }

    @TypeParameter("E")
    @TypeParameterSpecialization(name = "E", nativeContainerType = double.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equalsDouble(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        boolean indeterminate = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i) || rightArray.isNull(i)) {
                indeterminate = true;
                continue;
            }
            double leftElement = type.getDouble(leftArray, i);
            double rightElement = type.getDouble(rightArray, i);
            try {
                Boolean result = (Boolean) equalsFunction.invokeExact(leftElement, rightElement);
                if (result == null) {
                    indeterminate = true;
                }
                else if (!result) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }

        if (indeterminate) {
            return null;
        }
        return true;
    }
}
