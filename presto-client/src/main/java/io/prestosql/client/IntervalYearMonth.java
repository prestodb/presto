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
package io.prestosql.client;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;

public final class IntervalYearMonth
{
    private static final String INT_MIN_VALUE = "-178956970-8";

    private static final Pattern FORMAT = Pattern.compile("(\\d+)-(\\d+)");

    private IntervalYearMonth() {}

    public static int toMonths(int year, int months)
    {
        try {
            return addExact(multiplyExact(year, 12), months);
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String formatMonths(int months)
    {
        if (months == Integer.MIN_VALUE) {
            return INT_MIN_VALUE;
        }

        String sign = "";
        if (months < 0) {
            sign = "-";
            months = -months;
        }

        return format("%s%d-%d", sign, months / 12, months % 12);
    }

    public static int parseMonths(String value)
    {
        if (value.equals(INT_MIN_VALUE)) {
            return Integer.MIN_VALUE;
        }

        int signum = 1;
        if (value.startsWith("-")) {
            signum = -1;
            value = value.substring(1);
        }

        Matcher matcher = FORMAT.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid year-month interval: " + value);
        }

        int years = parseInt(matcher.group(1));
        int months = parseInt(matcher.group(2));

        return toMonths(years, months) * signum;
    }
}
