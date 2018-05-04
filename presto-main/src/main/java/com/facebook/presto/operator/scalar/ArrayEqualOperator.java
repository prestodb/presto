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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.util.Failures.internalError;

@ScalarOperator(EQUAL)
public final class ArrayEqualOperator
{
    private ArrayEqualOperator() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equals(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            checkElementNotNull(leftArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            Object leftElement = readNativeValue(type, leftArray, i);
            Object rightElement = readNativeValue(type, rightArray, i);
            try {
                if (!(boolean) equalsFunction.invoke(leftElement, rightElement)) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        return true;
    }

    @TypeParameter("E")
    @TypeParameterSpecialization(name = "E", nativeContainerType = long.class)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equalsLong(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            checkElementNotNull(leftArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            long leftElement = type.getLong(leftArray, i);
            long rightElement = type.getLong(rightArray, i);
            try {
                if (!(boolean) equalsFunction.invokeExact(leftElement, rightElement)) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        return true;
    }

    @TypeParameter("E")
    @TypeParameterSpecialization(name = "E", nativeContainerType = double.class)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equalsDouble(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            checkElementNotNull(leftArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            double leftElement = type.getDouble(leftArray, i);
            double rightElement = type.getDouble(rightArray, i);
            try {
                if (!(boolean) equalsFunction.invokeExact(leftElement, rightElement)) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        return true;
    }
}
