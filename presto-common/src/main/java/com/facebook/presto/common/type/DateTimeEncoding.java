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
package com.facebook.presto.common.type;

import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class DateTimeEncoding
{
    private DateTimeEncoding()
    {
    }

    private static final int TIME_ZONE_MASK = 0xFFF;
    private static final int MILLIS_SHIFT = 12;
    private static final long MAX_MILLIS = 0x7FFFFFFFFFFFFL;
    private static final long MIN_MILLIS = (MAX_MILLIS + 1) * -1;

    private static long pack(long millisUtc, short timeZoneKey)
    {
        if (millisUtc > MAX_MILLIS || millisUtc < MIN_MILLIS) {
            throw new ArithmeticException(format("TimestampWithTimeZone overflow: %s ms", millisUtc));
        }
        return (millisUtc << MILLIS_SHIFT) | (timeZoneKey & TIME_ZONE_MASK);
    }

    public static long packDateTimeWithZone(long millisUtc, String zoneId)
    {
        return packDateTimeWithZone(millisUtc, getTimeZoneKey(zoneId));
    }

    public static long packDateTimeWithZone(long millisUtc, int offsetMinutes)
    {
        return packDateTimeWithZone(millisUtc, getTimeZoneKeyForOffset(offsetMinutes));
    }

    public static long packDateTimeWithZone(long millisUtc, TimeZoneKey timeZoneKey)
    {
        requireNonNull(timeZoneKey, "timeZoneKey is null");
        return pack(millisUtc, timeZoneKey.getKey());
    }

    public static long unpackMillisUtc(long dateTimeWithTimeZone)
    {
        return dateTimeWithTimeZone >> MILLIS_SHIFT;
    }

    public static TimeZoneKey unpackZoneKey(long dateTimeWithTimeZone)
    {
        return getTimeZoneKey((short) (dateTimeWithTimeZone & TIME_ZONE_MASK));
    }

    public static long updateMillisUtc(long newMillsUtc, long dateTimeWithTimeZone)
    {
        return pack(newMillsUtc, (short) (dateTimeWithTimeZone & TIME_ZONE_MASK));
    }
}
