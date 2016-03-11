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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

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
        checkArgument(parameters.size() == 2, "Expected 2 literal parameters for DECIMAL");
        TypeParameter precision = parameters.get(0);
        TypeParameter scale = parameters.get(1);
        checkArgument(precision.isLongLiteral() &&
                scale.isLongLiteral(), "Expected both literal parameters for DECIMAL to be numbers");

        return DecimalType.createDecimalType(precision.getLongLiteral().intValue(), scale.getLongLiteral().intValue());
    }
}
