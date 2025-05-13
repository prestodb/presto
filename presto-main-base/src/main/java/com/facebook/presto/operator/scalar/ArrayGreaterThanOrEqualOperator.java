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
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.util.Failures.internalError;

@ScalarOperator(GREATER_THAN_OR_EQUAL)
public final class ArrayGreaterThanOrEqualOperator
{
    private ArrayGreaterThanOrEqualOperator() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"T", "T"}) MethodHandle greaterThanFunction,
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
                if ((boolean) greaterThanFunction.invoke(leftElement, rightElement)) {
                    return true;
                }
                if ((boolean) greaterThanFunction.invoke(rightElement, leftElement)) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
            index++;
        }

        return leftArray.getPositionCount() >= rightArray.getPositionCount();
    }
}
