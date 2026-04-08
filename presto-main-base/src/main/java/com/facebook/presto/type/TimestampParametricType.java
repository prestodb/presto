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

import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;

import java.util.List;

/**
 * Parametric type handler for TIMESTAMP(p) where p is the precision (0..12).
 * <p>
 * When no parameter is specified, defaults to TIMESTAMP(3) for backward compatibility
 * with existing millisecond-precision behavior.
 * <p>
 * Precision 0-6: Short timestamps, stored as a single long
 * Precision 7-12: Long timestamps, stored as 12 bytes (epoch micros + picos of micro)
 */
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
            return TimestampType.TIMESTAMP;
        }
        if (parameters.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one parameter for TIMESTAMP");
        }

        TypeParameter parameter = parameters.get(0);

        if (!parameter.isLongLiteral()) {
            throw new IllegalArgumentException("TIMESTAMP precision must be a number");
        }

        long precision = parameter.getLongLiteral();

        if (precision < 0 || precision > 12) {
            throw new IllegalArgumentException("Invalid TIMESTAMP precision " + precision);
        }

        return TimestampType.createTimestampType((int) precision);
    }
}
