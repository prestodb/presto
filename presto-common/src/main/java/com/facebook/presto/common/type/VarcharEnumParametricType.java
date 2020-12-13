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
import com.facebook.presto.common.type.VarcharEnumType.VarcharEnumMap;

import java.util.List;

import static java.lang.String.format;

public final class VarcharEnumParametricType
        implements ParametricType
{
    private final QualifiedObjectName name;
    private final VarcharEnumMap enumMap;

    public VarcharEnumParametricType(QualifiedObjectName name, VarcharEnumMap enumMap)
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
            return new VarcharEnumType(name, enumMap);
        }
        checkArgument(parameters.size() == 1, "Enum type expects exactly one parameter, got %s", parameters);
        checkArgument(
                parameters.get(0).getKind() == ParameterKind.VARCHAR_ENUM,
                "Enum definition expected, got %s",
                parameters);
        return new VarcharEnumType(name, parameters.get(0).getVarcharEnumMap());
    }

    public VarcharEnumMap getEnumMap()
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
