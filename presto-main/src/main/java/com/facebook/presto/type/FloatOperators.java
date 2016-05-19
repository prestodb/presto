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

import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.metadata.OperatorType.NEGATION;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

public final class FloatOperators
{
    private FloatOperators()
    {
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.FLOAT)
    public static long negate(@SqlType(StandardTypes.FLOAT) long value)
    {
        return floatToRawIntBits(-intBitsToFloat((int) value));
    }
}
