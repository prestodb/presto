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
import static com.facebook.presto.spi.function.OperatorType.IS_NOT_DISTINCT_FROM;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.google.common.base.Defaults.defaultValue;

public final class MapDistinctFromOperator
{
    private MapDistinctFromOperator() {}

    @ScalarOperator(IS_DISTINCT_FROM)
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"K", "K"})
                    MethodHandle keyEqualsFunction,
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"K"})
                    MethodHandle keyHashcodeFunction,
            @OperatorDependency(operator = IS_NOT_DISTINCT_FROM, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"})
                    MethodHandle valueNotDistinctFromFunction,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType,
            @SqlNullable @SqlType("map(K,V)") Block leftBlock,
            @SqlNullable @SqlType("map(K,V)") Block rightBlock)
    {
        return !isNotDistinctFrom(keyEqualsFunction, keyHashcodeFunction, valueNotDistinctFromFunction, keyType, valueType, leftBlock, rightBlock);
    }

    @ScalarOperator(IS_NOT_DISTINCT_FROM)
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNotDistinctFrom(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"K", "K"})
                    MethodHandle keyEqualsFunction,
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"K"})
                    MethodHandle keyHashcodeFunction,
            @OperatorDependency(operator = IS_NOT_DISTINCT_FROM, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"})
                    MethodHandle valueNotDistinctFromFunction,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType,
            @SqlNullable @SqlType("map(K,V)") Block leftBlock,
            @SqlNullable @SqlType("map(K,V)") Block rightBlock)
    {
        boolean leftNull = leftBlock == null;
        boolean rightNull = rightBlock == null;
        if (leftNull != rightNull) {
            return false;
        }
        if (leftNull) {
            return true;
        }
        // Note that we compare to NOT distinct here and so negate the result.
        Boolean result = MapGenericEquality.genericEqual(
                keyEqualsFunction,
                keyHashcodeFunction,
                keyType,
                leftBlock,
                rightBlock,
                (leftMapIndex, rightMapIndex) -> {
                    Object leftValue = readNativeValue(valueType, leftBlock, leftMapIndex);
                    boolean leftValueNull = leftValue == null;
                    if (leftValueNull) {
                        leftValue = defaultValue(valueType.getJavaType());
                    }
                    Object rightValue = readNativeValue(valueType, rightBlock, rightMapIndex);
                    boolean rightValueNull = rightValue == null;
                    if (rightValueNull) {
                        rightValue = defaultValue(valueType.getJavaType());
                    }
                    return (boolean) valueNotDistinctFromFunction.invoke(leftValue, leftNull, rightValue, rightNull);
                }
        );
        // Should not be null, but make IntelliJ happy.
        return result != null && result;
    }
}
