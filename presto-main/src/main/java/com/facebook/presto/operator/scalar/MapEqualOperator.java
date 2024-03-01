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

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;

@ScalarOperator(EQUAL)
public final class MapEqualOperator
{
    private MapEqualOperator() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean equals(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"K", "K"}) MethodHandle keyEqualsFunction,
            @OperatorDependency(operator = HASH_CODE, argumentTypes = {"K"}) MethodHandle keyHashcodeFunction,
            @OperatorDependency(operator = EQUAL, argumentTypes = {"V", "V"}) MethodHandle valueEqualsFunction,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType,
            @SqlType("map(K,V)") Block leftMapBlock,
            @SqlType("map(K,V)") Block rightMapBlock)
    {
        MethodHandle keyBlockEqualsFunction = compose(keyEqualsFunction, nativeValueGetter(keyType));
        MethodHandle keyBlockHashCode = compose(keyHashcodeFunction, nativeValueGetter(keyType));
        return MapGenericEquality.genericEqual(
                keyType,
                keyHashcodeFunction,
                keyBlockEqualsFunction,
                keyBlockHashCode,
                leftMapBlock,
                rightMapBlock,
                (leftMapIndex, rightMapIndex) -> {
                    Object leftValue = readNativeValue(valueType, leftMapBlock, leftMapIndex);
                    if (leftValue == null) {
                        return null;
                    }
                    Object rightValue = readNativeValue(valueType, rightMapBlock, rightMapIndex);
                    if (rightValue == null) {
                        return null;
                    }
                    return (Boolean) valueEqualsFunction.invoke(leftValue, rightValue);
                });
    }
}
