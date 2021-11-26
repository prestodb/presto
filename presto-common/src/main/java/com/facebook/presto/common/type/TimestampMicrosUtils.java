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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.type.SqlTimestamp.JSON_FORMATTER;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static java.lang.Math.floorDiv;

public final class TimestampMicrosUtils
{
    public static final Pattern DATETIME_PATTERN = Pattern.compile("" +
            "(?<year>\\d\\d\\d\\d)-(?<month>\\d{1,2})-(?<day>\\d{1,2})" +
            "(?: (?<hour>\\d{1,2}):(?<minute>\\d{1,2})(?::(?<second>\\d{1,2})(?:\\.(?<fraction>\\d+))?)?)?" +
            "\\s*(?<timezone>.+)?");

    public static final long MICROS_PER_SECOND = 1000_000;
    public static final long MICROS_PER_MILLI = 1000;
    public static final long NANOS_PER_MICRO = 1000;
    public static final int PRECISION_MICROSECONDS = 6;

    private static final long[] POWERS_OF_TEN = {
            100_000L,
            10_000L,
            1_000L,
            100L,
            10L,
            1L
    };

    private TimestampMicrosUtils() {}

    public static double unixTimeToMicros(double unixTime)
    {
        return unixTime * MICROS_PER_SECOND;
    }

    public static long microsToUnixTime(long micros)
    {
        return micros / MICROS_PER_SECOND;
    }

    public static long microsToMillis(long micros)
    {
        return Math.floorDiv(micros, MICROS_PER_MILLI);
    }

    public static long millisToMicros(long millis)
    {
        return millis * MICROS_PER_MILLI;
    }

    public static Timestamp microsToTimestamp(long micros)
    {
        long seconds = micros / MICROS_PER_SECOND;
        long nanoAdj = (micros % MICROS_PER_SECOND) * NANOS_PER_MICRO;
        return Timestamp.from(Instant.ofEpochSecond(seconds, nanoAdj));
    }

    public static long timestampToMicros(Timestamp timestamp)
    {
        long millis = timestamp.getTime();
        int nanos = timestamp.getNanos();
        // For example, if nanos is 123_456_789, we would get 456
        int micros = (nanos / 1000) % 1000;
        return millis * MICROS_PER_MILLI + micros;
    }

    // Learn from Instant#ofEpochMilli
    public static Instant microsToInstant(long epochMicros)
    {
        long seconds = floorDiv(epochMicros, 1000_000);
        int micros = (int) Math.floorMod(epochMicros, 1000_000);
        return Instant.ofEpochSecond(seconds, micros * 1000L);
    }

    public static long instantToMicros(Instant instant)
    {
        long micros = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);
        long adjustment = Math.floorDiv(instant.getNano(), NANOS_PER_MICRO);
        return Math.addExact(micros, adjustment);
    }

    public static String printTimestampMicros(long timestamp)
    {
        return microsToInstant(timestamp).atZone(ZoneId.of(UTC_KEY.getId())).format(JSON_FORMATTER);
    }

    public static String printTimestampMicros(long timestamp, TimeZoneKey timeZoneKey)
    {
        return microsToInstant(timestamp).atZone(ZoneId.of(timeZoneKey.getId())).format(JSON_FORMATTER);
    }

    public static long truncateEpochMicrosToMillis(long epochMicros)
    {
        return floorDiv(epochMicros, MICROS_PER_MILLI) * MICROS_PER_MILLI;
    }

    public static long parseTimestampMicrosLiteral(String value, TimeZoneKey timeZoneKey)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches() || matcher.group("timezone") != null) {
            throw new IllegalArgumentException("Invalid timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");

        long epochSecond = LocalDateTime.of(
                Integer.parseInt(year),
                Integer.parseInt(month),
                Integer.parseInt(day),
                hour == null ? 0 : Integer.parseInt(hour),
                minute == null ? 0 : Integer.parseInt(minute),
                second == null ? 0 : Integer.parseInt(second),
                0)
                .atZone(ZoneId.of(timeZoneKey.getId()))
                .toEpochSecond();

        return ofMicros(epochSecond, fraction);
    }

    private static long toEpochSecond(String year, String month, String day, String hour, String minute, String second, ZoneId zoneId)
    {
        LocalDateTime timestamp = LocalDateTime.of(
                Integer.parseInt(year),
                Integer.parseInt(month),
                Integer.parseInt(day),
                hour == null ? 0 : Integer.parseInt(hour),
                minute == null ? 0 : Integer.parseInt(minute),
                second == null ? 0 : Integer.parseInt(second),
                0);

        // Only relevant for legacy timestamps. New timestamps are parsed using UTC, which doesn't
        // have daylight savings transitions.
        // TODO: remove once legacy timestamps are gone
        List<ZoneOffset> offsets = zoneId.getRules().getValidOffsets(timestamp);
        if (offsets.isEmpty()) {
            throw new IllegalArgumentException("Invalid timestamp due to daylight savings transition");
        }

        return timestamp.toEpochSecond(offsets.get(0));
    }

    public static long castToTimestamp(String value, Function<String, ZoneId> zoneId)
    {
        Matcher matcher = DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        long epochSecond = ZonedDateTime.of(
                Integer.parseInt(year),
                Integer.parseInt(month),
                Integer.parseInt(day),
                hour == null ? 0 : Integer.parseInt(hour),
                minute == null ? 0 : Integer.parseInt(minute),
                second == null ? 0 : Integer.parseInt(second),
                0,
                zoneId.apply(timezone))
                .toEpochSecond();

        return ofMicros(epochSecond, fraction);
    }

    private static long ofMicros(long epochSecond, String fraction)
    {
        long micros = 0;
        if (fraction != null) {
            int length = fraction.length();
            for (int i = 0; i < length && i < PRECISION_MICROSECONDS; i++) {
                int numericValue = Character.getNumericValue(fraction.charAt(i));
                micros += numericValue * POWERS_OF_TEN[i];
            }
        }
        return epochSecond * MICROS_PER_SECOND + micros;
    }
}
