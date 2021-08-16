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

import com.facebook.presto.common.InvalidFunctionArgumentException;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.common.type.semantic.SemanticType;
import com.facebook.presto.spi.PrestoException;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class CharParametricType
        implements ParametricType
{
    public static final CharParametricType CHAR = new CharParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.CHAR;
    }

    @Override
    public SemanticType createType(Optional<QualifiedObjectName> name, List<TypeParameter> parameters)
    {
        CharType charType;
        if (parameters.isEmpty()) {
            charType = createCharType(1);
        }
        else if (parameters.size() != 1) {
            throw new IllegalArgumentException("Expected at most one parameter for CHAR");
        }
        else {
            TypeParameter parameter = parameters.get(0);

            if (!parameter.isLongLiteral()) {
                throw new IllegalArgumentException("CHAR length must be a number");
            }

            try {
                charType = createCharType(parameter.getLongLiteral());
            }
            catch (InvalidFunctionArgumentException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
            }
        }
        return SemanticType.from(name, charType);
    }
}
