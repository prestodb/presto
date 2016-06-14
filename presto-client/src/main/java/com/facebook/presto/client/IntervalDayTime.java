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

public final class IntervalDayTime
{
    private static final long MILLIS_IN_SECOND = 1000;
    private static final long MILLIS_IN_MINUTE = 60 * MILLIS_IN_SECOND;
    private static final long MILLIS_IN_HOUR = 60 * MILLIS_IN_MINUTE;
    private static final long MILLIS_IN_DAY = 24 * MILLIS_IN_HOUR;

    private IntervalDayTime() {}

    public static long toMillis(long day, long hour, long minute, long second, long millis)
    {
        return (day * MILLIS_IN_DAY) +
                (hour * MILLIS_IN_HOUR) +
                (minute * MILLIS_IN_MINUTE) +
                (second * MILLIS_IN_SECOND) +
                millis;
    }

    public static String formatMillis(long millis)
    {
        if (millis == Long.MIN_VALUE) {
            return "-106751991167 07:12:55.808";
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
}
