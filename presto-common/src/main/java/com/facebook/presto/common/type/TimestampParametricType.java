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

import java.util.List;

import static com.facebook.presto.common.type.TimestampType.DEFAULT_PRECISION;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;

// Resolves "timestamp(p)" type signatures to TimestampType instances.
// Registered alongside the bare TIMESTAMP singleton so the type registry can
// handle both "timestamp" (→ p=3) and "timestamp(p)" (→ any precision in [0,12]).
public class TimestampParametricType
        implements ParametricType
{
    public static final TimestampParametricType TIMESTAMP = new TimestampParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.TIMESTAMP;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return createTimestampType(DEFAULT_PRECISION);
        }
        if (parameters.size() != 1) {
            throw new IllegalArgumentException("TIMESTAMP requires exactly one precision parameter, got: " + parameters.size());
        }
        TypeParameter parameter = parameters.get(0);
        if (!parameter.isLongLiteral()) {
            throw new IllegalArgumentException("TIMESTAMP precision must be an integer literal");
        }
        long precision = parameter.getLongLiteral();
        if (precision < 0 || precision > TimestampType.MAX_PRECISION) {
            throw new IllegalArgumentException(
                    "TIMESTAMP precision must be in range [0, " + TimestampType.MAX_PRECISION + "]: " + precision);
        }
        return createTimestampType((int) precision);
    }
}
