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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.LongEnumType.LongEnumMap;

import java.util.List;

import static java.lang.String.format;

public final class LongEnumParametricType
        implements ParametricType
{
    private final QualifiedObjectName name;
    private final LongEnumMap enumMap;

    public LongEnumParametricType(QualifiedObjectName name, LongEnumMap enumMap)
    {
        this.name = name;
        this.enumMap = enumMap;
    }

    @Override
    public String getName()
    {
        return name.toString();
    }

    @Override
    public TypeSignatureBase getTypeSignatureBase()
    {
        return TypeSignatureBase.of(name);
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return new LongEnumType(name, enumMap);
        }
        checkArgument(parameters.size() == 1, "Enum type expects exactly one parameter, got %s", parameters);
        checkArgument(
                parameters.get(0).getKind() == ParameterKind.LONG_ENUM,
                "Enum definition expected, got %s",
                parameters);
        return new LongEnumType(name, parameters.get(0).getLongEnumMap());
    }

    public LongEnumMap getEnumMap()
    {
        return enumMap;
    }

    private static void checkArgument(boolean argument, String format, Object... args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format(format, args));
        }
    }
}
