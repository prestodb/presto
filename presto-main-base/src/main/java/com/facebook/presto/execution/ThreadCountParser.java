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
package com.facebook.presto.execution;

import com.google.common.annotations.VisibleForTesting;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Math.max;
import static java.lang.Math.round;
import static java.lang.Math.toIntExact;

public class ThreadCountParser
{
    private static final String PER_CORE_SUFFIX = "C";

    private ThreadCountParser() {}

    public static int parse(String value)
    {
        return parse(value, Runtime.getRuntime().availableProcessors());
    }

    @VisibleForTesting
    static int parse(String value, int availableProcessors)
    {
        int threads;
        if (value.endsWith(PER_CORE_SUFFIX)) {
            double multiplier = parseDouble(value.substring(0, value.length() - PER_CORE_SUFFIX.length()).trim());
            checkArgument(multiplier > 0, "Thread multiplier must be positive: %s", multiplier);
            threads = toIntExact(round(multiplier * availableProcessors));
            threads = max(threads, 1);
        }
        else {
            threads = parseInt(value);
        }

        checkArgument(threads > 0, "Thread count must be positive: %s", threads);
        return threads;
    }
}
