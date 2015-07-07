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
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import org.jetbrains.annotations.NotNull;

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
    public Type createType(List<Type> types, List<Object> literals)
    {
        checkArgument(types.isEmpty(), "Type parameters not allowed for DECIMAL");
        if (literals.size() == 0) {
            // we need support for that for support of unparametrized TypeSignatures which are
            // used for defining DECIMAL related functions. See e.g. DecimalOperators class.
            return ShortDecimalType.createUnparametrizedDecimal();
        }

        return createParametrizedDecimal(literals);
    }

    @NotNull
    private Type createParametrizedDecimal(List<Object> literals)
    {
        checkArgument(literals.size() == 2, "Expected 2 literal parameters for DECIMAL");
        checkArgument(literals.get(0) instanceof Long &&
                literals.get(1) instanceof Long, "Expected both literal parameters for DECIMAL to be numbers");
        long precision = (long) literals.get(0);
        long scale = (long) literals.get(1);
        return DecimalType.createDecimalType((int) precision, (int) scale);
    }
}
