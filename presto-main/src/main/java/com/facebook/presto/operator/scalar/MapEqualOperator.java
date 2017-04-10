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
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;

@ScalarOperator(EQUAL)
public final class MapEqualOperator
{
    private MapEqualOperator() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlNullable
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean equals(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"K", "K"}) MethodHandle keyEqualsFunction,
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"K"}) MethodHandle keyHashcodeFunction,
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"}) MethodHandle valueEqualsFunction,
            @TypeParameter("K") Type keyType,
            @TypeParameter("V") Type valueType,
            @SqlType("map(K,V)") Block leftMapBlock,
            @SqlType("map(K,V)") Block rightMapBlock)
    {
        return MapGenericEquality.genericEqual(
                keyEqualsFunction,
                keyHashcodeFunction,
                keyType,
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
                }
        );
    }
}
