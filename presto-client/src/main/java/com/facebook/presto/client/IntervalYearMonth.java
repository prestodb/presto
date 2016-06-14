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
package com.facebook.presto.client;

import static java.lang.String.format;

public final class IntervalYearMonth
{
    private IntervalYearMonth() {}

    public static long toMonths(long year, long months)
    {
        return (year * 12) + months;
    }

    public static String formatMonths(long months)
    {
        if (months == Long.MIN_VALUE) {
            return "-768614336404564650-8";
        }

        String sign = "";
        if (months < 0) {
            sign = "-";
            months = -months;
        }

        return format("%s%d-%d", sign, months / 12, months % 12);
    }
}
