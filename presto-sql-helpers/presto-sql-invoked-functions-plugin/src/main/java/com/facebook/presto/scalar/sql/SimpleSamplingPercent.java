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
package com.facebook.presto.scalar.sql;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlParameter;
import com.facebook.presto.spi.function.SqlType;

import static java.lang.String.format;

public class SimpleSamplingPercent
{
    // The body of key_sampling_percent, kept in one place because the guard below has to evaluate
    // it twice: a SQL-invoked function body is a single expression, so there is no way to bind an
    // intermediate name. The planner eliminates the common subexpression.
    private static final String SAMPLING_PERCENT_EXPRESSION =
            "(abs(from_ieee754_64(xxhash64(cast(input as varbinary)))) % 100) / 100.";

    private SimpleSamplingPercent() {}

    // from_ieee754_64 of a hash decodes to NaN or +/-infinity whenever the exponent bits are all
    // ones, and `mod(infinity(), 100)` is itself NaN, so ~0.05% of keys used to leave this
    // function as NaN -- outside the documented [0, 1] range. The guard tests the final result
    // rather than the decoded double, because is_nan on the decoded value would miss the
    // infinity case.
    @SqlInvokedScalarFunction(value = "key_sampling_percent", deterministic = true, calledOnNullInput = false)
    @Description("Returns a value between 0.0 and 1.0 using the hash of the given input string")
    @SqlParameter(name = "input", type = "varchar")
    @SqlType("double")
    public static String keySamplingPercent()
    {
        return format("return if(is_nan(%s), 0.0, %s) ", SAMPLING_PERCENT_EXPRESSION, SAMPLING_PERCENT_EXPRESSION);
    }
}
