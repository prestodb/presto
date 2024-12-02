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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.MethodHandleUtil;

import java.util.List;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.lang.String.format;

public final class NoOpMapType
        implements ParametricType
{
    public static final NoOpMapType NO_OP_MAP = new NoOpMapType();

    @Override
    public String getName()
    {
        return StandardTypes.MAP;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() == 2, format("Expected two parameters, got %s", parameters));
        TypeParameter firstParameter = parameters.get(0);
        TypeParameter secondParameter = parameters.get(1);
        checkArgument(
                firstParameter.getKind() == ParameterKind.TYPE && secondParameter.getKind() == ParameterKind.TYPE,
                format("Expected key and type to be types, got %s",
                parameters));

        Type keyType = firstParameter.getType();
        Type valueType = secondParameter.getType();
        return new MapType(
                keyType,
                valueType,
                MethodHandleUtil.methodHandle(NoOpMapType.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(NoOpMapType.class, "throwUnsupportedOperation"));
    }
}
