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
    // 53 is the width a double represents exactly, so masking a hash down to 53 bits gives 2^53
    // outcomes that each map to a distinct value, and dividing by 2^53 is lossless.
    private static final long MANTISSA_MASK = (1L << 53) - 1;
    private static final long MANTISSA_SCALE = 1L << 53;

    private SimpleSamplingPercent() {}

    @SqlInvokedScalarFunction(value = "key_sampling_percent", deterministic = true, calledOnNullInput = false)
    @Description("Returns a value between 0.0 and 1.0 using the hash of the given input string")
    @SqlParameter(name = "input", type = "varchar")
    @SqlType("double")
    public static String keySamplingPercent()
    {
        return "return (abs(from_ieee754_64(xxhash64(cast(input as varbinary)))) % 100) / 100. ";
    }

    // deterministic_random is what key_sampling_percent was always meant to be. key_sampling_percent
    // decodes the hash as an IEEE-754 double, which puts the random bits in the exponent: 47.4%
    // of keys come back below 1e-30 and another 47.5% collapse onto the 25 multiples of 0.04, so
    // `key_sampling_percent(k) < p` selects ~51% of rows for any small p. That is a bug, not a
    // design choice; it survives only because its exact values are relied on as a stable
    // sampling key by existing workloads, and key_sampling_percent may be deprecated once those
    // callers have migrated here.
    //
    // Masking rather than decoding leaves no exponent field to randomise, so the result is
    // uniform on [0, 1) and can never be NaN.
    @SqlInvokedScalarFunction(value = "deterministic_random", deterministic = true, calledOnNullInput = false)
    @Description("Returns a uniformly distributed value in [0, 1) using the hash of the given input string")
    @SqlParameter(name = "input", type = "varchar")
    @SqlType("double")
    public static String deterministicRandom()
    {
        return format(
                "return bitwise_and(from_big_endian_64(xxhash64(cast(input as varbinary))), %s) / %sE0 ",
                MANTISSA_MASK,
                MANTISSA_SCALE);
    }
}
