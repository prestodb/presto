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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
@ScalarOperator(LESS_THAN)
public final class ArrayLessThanOperator
{
    private ArrayLessThanOperator() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle lessThanFunction,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equalsFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block leftArray,
            @SqlType("array(T)") Block rightArray)
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
                if (!(boolean) equalsFunction.invoke(leftElement, rightElement)) {
                    return false;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
            index++;
        }

        return leftArray.getPositionCount() < rightArray.getPositionCount();
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    public static boolean lessThanLong(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle lessThanFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block leftArray,
            @SqlType("array(T)") Block rightArray)
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

                throw new PrestoException(INTERNAL_ERROR, t);
            }
            index++;
        }

        return leftArray.getPositionCount() < rightArray.getPositionCount();
    }
}
