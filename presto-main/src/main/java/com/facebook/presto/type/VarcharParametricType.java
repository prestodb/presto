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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.common.type.semantic.SemanticType;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;

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
    public SemanticType createType(Optional<QualifiedObjectName> name, List<TypeParameter> parameters)
    {
        VarcharType varcharType;
        if (parameters.isEmpty()) {
            varcharType = createUnboundedVarcharType();
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
            varcharType = VarcharType.createUnboundedVarcharType();
        }
        else if (length < 0 || length > VarcharType.MAX_LENGTH) {
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        else {
            varcharType = VarcharType.createVarcharType((int) length);
        }

        return SemanticType.from(name, varcharType);
    }
}
