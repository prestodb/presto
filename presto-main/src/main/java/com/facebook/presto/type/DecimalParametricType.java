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
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.common.type.semantic.SemanticType;
import com.facebook.presto.spi.PrestoException;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class DecimalParametricType
        implements ParametricType
{
    public static final DecimalParametricType DECIMAL = new DecimalParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.DECIMAL;
    }

    @Override
    public SemanticType createType(Optional<QualifiedObjectName> name, List<TypeParameter> parameters)
    {
        DecimalType decimalType;
        try {
            switch (parameters.size()) {
                case 0:
                    decimalType = DecimalType.createDecimalType();
                    break;
                case 1:
                    decimalType = DecimalType.createDecimalType(parameters.get(0).getLongLiteral().intValue());
                    break;
                case 2:
                    decimalType = DecimalType.createDecimalType(parameters.get(0).getLongLiteral().intValue(), parameters.get(1).getLongLiteral().intValue());
                    break;
                default:
                    throw new IllegalArgumentException("Expected 0, 1 or 2 parameters for DECIMAL type constructor.");
            }
        }
        catch (InvalidFunctionArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        return SemanticType.from(name, decimalType);
    }
}
