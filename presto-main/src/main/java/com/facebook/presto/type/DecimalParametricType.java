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

import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;

import java.util.List;

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
    public Type createType(List<TypeParameter> parameters)
    {
        switch (parameters.size()) {
            case 0:
                return DecimalType.createDecimalType();
            case 1:
                return DecimalType.createDecimalType(parameters.get(0).getLongLiteral().intValue());
            case 2:
                return DecimalType.createDecimalType(parameters.get(0).getLongLiteral().intValue(), parameters.get(1).getLongLiteral().intValue());
            default:
                throw new IllegalArgumentException("Expected 0, 1 or 2 parameters for DECIMAL type constructor.");
        }
    }
}
