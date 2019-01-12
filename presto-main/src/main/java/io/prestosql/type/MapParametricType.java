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
package io.prestosql.type;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.ParameterKind;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeParameter;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.block.MethodHandleUtil.compose;
import static io.prestosql.spi.block.MethodHandleUtil.nativeValueGetter;

public final class MapParametricType
        implements ParametricType
{
    public static final MapParametricType MAP = new MapParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.MAP;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() == 2, "Expected two parameters, got %s", parameters);
        TypeParameter firstParameter = parameters.get(0);
        TypeParameter secondParameter = parameters.get(1);
        checkArgument(
                firstParameter.getKind() == ParameterKind.TYPE && secondParameter.getKind() == ParameterKind.TYPE,
                "Expected key and type to be types, got %s",
                parameters);

        Type keyType = firstParameter.getType();
        Type valueType = secondParameter.getType();
        MethodHandle keyNativeEquals = typeManager.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
        MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = typeManager.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
        return new MapType(
                keyType,
                valueType,
                keyBlockNativeEquals,
                keyBlockEquals,
                keyNativeHashCode,
                keyBlockHashCode);
    }
}
