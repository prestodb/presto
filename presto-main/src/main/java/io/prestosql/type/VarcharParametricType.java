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

import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeParameter;
import io.prestosql.spi.type.VarcharType;

import java.util.List;

import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;

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
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return createUnboundedVarcharType();
        }
        if (parameters.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one parameter for VARCHAR");
        }

        TypeParameter parameter = parameters.get(0);

        if (!parameter.isLongLiteral()) {
            throw new IllegalArgumentException("VARCHAR length must be a number");
        }

        long length = parameter.getLongLiteral();

        if (length == VarcharType.UNBOUNDED_LENGTH) {
            return VarcharType.createUnboundedVarcharType();
        }

        if (length < 0 || length > VarcharType.MAX_LENGTH) {
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }

        return VarcharType.createVarcharType((int) length);
    }
}
