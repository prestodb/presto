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
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;

@ScalarOperator(EQUAL)
public final class ArrayEqualOperator
{
    private ArrayEqualOperator() {}

    @TypeParameter("E")
    @Nullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean equals(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }
        boolean foundNull = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i) || rightArray.isNull(i)) {
                foundNull = true;
                continue;
            }
            Object leftElement = readNativeValue(type, leftArray, i);
            Object rightElement = readNativeValue(type, rightArray, i);
            try {
                Boolean result = (Boolean) equalsFunction.invoke(leftElement, rightElement);
                if (result == null) {
                    foundNull = true;
                    continue;
                }
                if (!result) {
                    return false;
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
        return true;
    }
}
