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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Long.parseLong;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;

public final class IntervalYearMonth
{
    private static final String LONG_MIN_VALUE = "-768614336404564650-8";

    private static final Pattern FORMAT = Pattern.compile("(\\d+)-(\\d+)");

    private IntervalYearMonth() {}

    public static long toMonths(long year, long months)
    {
        try {
            return addExact(multiplyExact(year, 12), months);
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String formatMonths(long months)
    {
        if (months == Long.MIN_VALUE) {
            return LONG_MIN_VALUE;
        }

        String sign = "";
        if (months < 0) {
            sign = "-";
            months = -months;
        }

        return format("%s%d-%d", sign, months / 12, months % 12);
    }

    public static long parseMonths(String value)
    {
        if (value.equals(LONG_MIN_VALUE)) {
            return Long.MIN_VALUE;
        }

        long signum = 1;
        if (value.startsWith("-")) {
            signum = -1;
            value = value.substring(1);
        }

        Matcher matcher = FORMAT.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid year-month interval: " + value);
        }

        long years = parseLong(matcher.group(1));
        long months = parseLong(matcher.group(2));

        return toMonths(years, months) * signum;
    }
}
