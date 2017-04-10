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
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.google.common.base.Defaults.defaultValue;

@ScalarOperator(IS_DISTINCT_FROM)
public final class ArrayDistinctFromOperator
{
    private ArrayDistinctFromOperator() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @OperatorDependency(operator = IS_DISTINCT_FROM, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle function,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block left,
            @IsNull boolean leftNull,
            @SqlType("array(E)") Block right,
            @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        if (left.getPositionCount() != right.getPositionCount()) {
            return true;
        }
        for (int i = 0; i < left.getPositionCount(); i++) {
            Object leftValue = readNativeValue(type, left, i);
            boolean leftValueNull = leftValue == null;
            if (leftValueNull) {
                leftValue = defaultValue(type.getJavaType());
            }
            Object rightValue = readNativeValue(type, right, i);
            boolean rightValueNull = rightValue == null;
            if (rightValueNull) {
                rightValue = defaultValue(type.getJavaType());
            }
            try {
                if ((boolean) function.invoke(
                        leftValue,
                        leftValueNull,
                        rightValue,
                        rightValueNull)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
        return false;
    }
}
