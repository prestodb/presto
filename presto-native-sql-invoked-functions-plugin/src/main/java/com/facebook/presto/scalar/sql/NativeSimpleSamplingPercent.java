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

public class NativeSimpleSamplingPercent
{
    private NativeSimpleSamplingPercent() {}

    @SqlInvokedScalarFunction(value = "key_sampling_percent", deterministic = true, calledOnNullInput = false)
    @Description("Returns a value between 0.0 and 1.0 using the hash of the given input string")
    @SqlParameter(name = "input", type = "varchar")
    @SqlType("double")
    public static String keySamplingPercent()
    {
        return "return (abs(from_ieee754_64(xxhash64(cast(input as varbinary)))) % 100) / 100. ";
    }
}
