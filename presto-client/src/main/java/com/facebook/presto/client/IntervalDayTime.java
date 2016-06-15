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

public final class IntervalDayTime
{
    private static final long MILLIS_IN_SECOND = 1000;
    private static final long MILLIS_IN_MINUTE = 60 * MILLIS_IN_SECOND;
    private static final long MILLIS_IN_HOUR = 60 * MILLIS_IN_MINUTE;
    private static final long MILLIS_IN_DAY = 24 * MILLIS_IN_HOUR;

    private static final String LONG_MIN_VALUE = "-106751991167 07:12:55.808";

    private static final Pattern FORMAT = Pattern.compile("(\\d+) (\\d+):(\\d+):(\\d+).(\\d+)");

    private IntervalDayTime() {}

    public static long toMillis(long day, long hour, long minute, long second, long millis)
    {
        try {
            long value = millis;
            value = addExact(value, multiplyExact(day, MILLIS_IN_DAY));
            value = addExact(value, multiplyExact(hour, MILLIS_IN_HOUR));
            value = addExact(value, multiplyExact(minute, MILLIS_IN_MINUTE));
            value = addExact(value, multiplyExact(second, MILLIS_IN_SECOND));
            return value;
        }
        catch (ArithmeticException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String formatMillis(long millis)
    {
        if (millis == Long.MIN_VALUE) {
            return LONG_MIN_VALUE;
        }
        String sign = "";
        if (millis < 0) {
            sign = "-";
            millis = -millis;
        }

        long day = millis / MILLIS_IN_DAY;
        millis %= MILLIS_IN_DAY;
        long hour = millis / MILLIS_IN_HOUR;
        millis %= MILLIS_IN_HOUR;
        long minute = millis / MILLIS_IN_MINUTE;
        millis %= MILLIS_IN_MINUTE;
        long second = millis / MILLIS_IN_SECOND;
        millis %= MILLIS_IN_SECOND;

        return format("%s%d %02d:%02d:%02d.%03d", sign, day, hour, minute, second, millis);
    }

    public static long parseMillis(String value)
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
            throw new IllegalArgumentException("Invalid day-time interval: " + value);
        }

        long days = parseLong(matcher.group(1));
        long hours = parseLong(matcher.group(2));
        long minutes = parseLong(matcher.group(3));
        long seconds = parseLong(matcher.group(4));
        long millis = parseLong(matcher.group(5));

        return toMillis(days, hours, minutes, seconds, millis) * signum;
    }
}
