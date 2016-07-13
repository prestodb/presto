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

import com.facebook.presto.operator.scalar.annotations.OperatorDependency;
import com.facebook.presto.operator.scalar.annotations.ScalarOperator;
import com.facebook.presto.operator.scalar.annotations.TypeParameter;
import com.facebook.presto.operator.scalar.annotations.TypeParameterSpecialization;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;

@ScalarOperator(LESS_THAN_OR_EQUAL)
public final class ArrayLessThanOrEqualOperator
{
    private ArrayLessThanOrEqualOperator() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(
                    @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle lessThanFunction,
                    @TypeParameter("E") Type type,
                    @SqlType("array(E)") Block leftArray,
                    @SqlType("array(E)") Block rightArray)
    {
        int len = Math.min(leftArray.getPositionCount(), rightArray.getPositionCount());
        int index = 0;
        while (index < len) {
            checkElementNotNull(leftArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            Object leftElement = readNativeValue(type, leftArray, index);
            Object rightElement = readNativeValue(type, rightArray, index);
            try {
                if ((boolean) lessThanFunction.invoke(leftElement, rightElement)) {
                    return true;
                }
                if ((boolean) lessThanFunction.invoke(rightElement, leftElement)) {
                    return false;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
            index++;
        }

        return leftArray.getPositionCount() <= rightArray.getPositionCount();
    }

    @TypeParameter("E")
    @TypeParameterSpecialization(name = "E", nativeContainerType = long.class)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqualLong(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int len = Math.min(leftArray.getPositionCount(), rightArray.getPositionCount());
        int index = 0;
        while (index < len) {
            checkElementNotNull(leftArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            long leftElement = type.getLong(leftArray, index);
            long rightElement = type.getLong(rightArray, index);
            try {
                if ((boolean) lessThanFunction.invokeExact(leftElement, rightElement)) {
                    return true;
                }
                if ((boolean) lessThanFunction.invokeExact(rightElement, leftElement)) {
                    return false;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
            index++;
        }

        return leftArray.getPositionCount() <= rightArray.getPositionCount();
    }
}
