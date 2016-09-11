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
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.google.common.base.Defaults.defaultValue;

@ScalarOperator(IS_DISTINCT_FROM)
public final class MapDistinctFromOperator
{
    private MapDistinctFromOperator() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"K", "K"})
                    MethodHandle keyEqualsFunction,
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"K"})
                    MethodHandle keyHashcodeFunction,
            @OperatorDependency(operator = IS_DISTINCT_FROM, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"})
                    MethodHandle valueDistinctFromFunction,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType,
            @SqlNullable @SqlType("map(K,V)") Block leftMapBlock,
            @SqlNullable @SqlType("map(K,V)") Block rightMapBlock)
    {
        boolean leftMapNull = leftMapBlock == null;
        boolean rightMapNull = rightMapBlock == null;
        if (leftMapNull != rightMapNull) {
            return true;
        }
        if (leftMapNull) {
            return false;
        }
        // Note that we compare to NOT distinct here and so negate the result.
        return !MapGenericEquality.genericEqual(
                keyEqualsFunction,
                keyHashcodeFunction,
                keyType,
                leftMapBlock,
                rightMapBlock,
                (leftMapIndex, rightMapIndex) -> {
                    Object leftValue = readNativeValue(valueType, leftMapBlock, leftMapIndex);
                    boolean leftNull = leftValue == null;
                    if (leftNull) {
                        leftValue = defaultValue(valueType.getJavaType());
                    }
                    Object rightValue = readNativeValue(valueType, rightMapBlock, rightMapIndex);
                    boolean rightNull = rightValue == null;
                    if (rightNull) {
                        rightValue = defaultValue(valueType.getJavaType());
                    }
                    return !(boolean) valueDistinctFromFunction.invoke(leftValue, leftNull, rightValue, rightNull);
                }
        );
    }
}
