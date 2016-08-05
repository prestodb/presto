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
package com.facebook.presto.type;

import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.spi.type.VarcharType;

import java.util.List;

public class VarcharParametricType
        implements ParametricType
{
    public static final VarcharParametricType VARCHAR = new VarcharParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.VARCHAR;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return VarcharType.createVarcharType(VarcharType.MAX_LENGTH);
        }
        if (parameters.size() != 1) {
            throw new IllegalArgumentException("Expected at most one parameter for VARCHAR");
        }

        TypeParameter parameter = parameters.get(0);

        if (!parameter.isLongLiteral()) {
            throw new IllegalArgumentException("VARCHAR length must be a number");
        }

        long length = parameter.getLongLiteral();
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        return VarcharType.createVarcharType((int) length);
    }
}
