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
package io.prestosql.spi.type;

import java.util.List;

import static java.lang.String.format;

public class QuantileDigestParametricType
        implements ParametricType
{
    public static final QuantileDigestParametricType QDIGEST = new QuantileDigestParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.QDIGEST;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() == 1, "QDIGEST type expects exactly one type as a parameter, got %s", parameters);
        checkArgument(
                parameters.get(0).getKind() == ParameterKind.TYPE,
                "QDIGEST expects type as a parameter, got %s",
                parameters);
        // Validation check on the acceptable type (bigint, real, double) intentionally omitted
        // because this is validated in each function and to allow for consistent error messaging
        return new QuantileDigestType(parameters.get(0).getType());
    }

    private static void checkArgument(boolean argument, String format, Object... args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format(format, args));
        }
    }
}
