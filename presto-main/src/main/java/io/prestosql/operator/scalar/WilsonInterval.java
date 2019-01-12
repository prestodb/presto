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
package io.prestosql.operator.scalar;

import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.util.Failures.checkCondition;

public class WilsonInterval
{
    private WilsonInterval()
    {
    }

    @ScalarFunction
    @Description("binomial confidence interval lower bound using Wilson score")
    @SqlType(StandardTypes.DOUBLE)
    public static double wilsonIntervalLower(@SqlType(StandardTypes.BIGINT) long successes, @SqlType(StandardTypes.BIGINT) long trials, @SqlType(StandardTypes.DOUBLE) double z)
    {
        return wilsonInterval(successes, trials, z, -1);
    }

    @ScalarFunction
    @Description("binomial confidence interval upper bound using Wilson score")
    @SqlType(StandardTypes.DOUBLE)
    public static double wilsonIntervalUpper(@SqlType(StandardTypes.BIGINT) long successes, @SqlType(StandardTypes.BIGINT) long trials, @SqlType(StandardTypes.DOUBLE) double z)
    {
        return wilsonInterval(successes, trials, z, 1);
    }

    private static double wilsonInterval(long successes, long trials, double z, int bound)
    {
        checkCondition(successes >= 0, INVALID_FUNCTION_ARGUMENT, "number of successes must not be negative");
        checkCondition(trials > 0, INVALID_FUNCTION_ARGUMENT, "number of trials must be positive");
        checkCondition(successes <= trials, INVALID_FUNCTION_ARGUMENT, "number of successes must not be larger than number of trials");
        checkCondition(z >= 0, INVALID_FUNCTION_ARGUMENT, "z-score must not be negative");

        double p = successes * 1.0 / trials;
        double n = trials;

        return (p + (z * z) / (2 * n) + bound * z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n))) / (1 + (z * z) / n);
    }
}
