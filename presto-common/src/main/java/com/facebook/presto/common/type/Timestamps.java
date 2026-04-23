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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;

/**
 * Utility class for timestamp precision handling, rescaling, and formatting.
 */
public final class Timestamps
{
    private Timestamps() {}

    public static final int MAX_PRECISION = 12;
    public static final int MAX_SHORT_PRECISION = 6;
    public static final int DEFAULT_PRECISION = 3;

    public static final long MILLISECONDS_PER_SECOND = 1_000L;
    public static final long MICROSECONDS_PER_SECOND = 1_000_000L;
    public static final long NANOSECONDS_PER_SECOND = 1_000_000_000L;
    public static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;

    public static final long MICROSECONDS_PER_MILLISECOND = 1_000L;
    public static final long NANOSECONDS_PER_MILLISECOND = 1_000_000L;
    public static final long PICOSECONDS_PER_MILLISECOND = 1_000_000_000L;

    public static final long NANOSECONDS_PER_MICROSECOND = 1_000L;
    public static final long PICOSECONDS_PER_MICROSECOND = 1_000_000L;

    public static final long PICOSECONDS_PER_NANOSECOND = 1_000L;

    public static final long MILLISECONDS_PER_DAY = 86_400_000L;

    public static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1_000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1_000_000_000_000L,
    };

    /**
     * Rescales a timestamp value from one precision to another.
     * <p>
     * For example, rescaling 123456 from precision 6 (microseconds) to precision 3 (milliseconds)
     * produces 123. Rescaling 123 from precision 3 to precision 6 produces 123000.
     * <p>
     * When downscaling, uses floor-based division (rounds toward negative infinity) to maintain
     * monotonic, consistent behavior for negative timestamps (pre-epoch values).
     */
    public static long rescale(long value, int fromPrecision, int toPrecision)
    {
        if (fromPrecision == toPrecision) {
            return value;
        }
        if (fromPrecision < toPrecision) {
            // Scale up
            int scaleFactor = toPrecision - fromPrecision;
            return value * POWERS_OF_TEN[scaleFactor];
        }
        else {
            // Scale down using floorDiv for consistent behavior with negative values
            int scaleFactor = fromPrecision - toPrecision;
            return floorDiv(value, POWERS_OF_TEN[scaleFactor]);
        }
    }

    /**
     * Divides and truncates toward zero (Java's default integer division behavior).
     * Note: this is NOT floor division — for negative values, the result is
     * truncated toward zero rather than toward negative infinity. For floor
     * division, use {@link Math#floorDiv(long, long)}.
     */
    public static long roundDiv(long value, long divisor)
    {
        return value / divisor;
    }

    /**
     * Round a value by truncating toward zero.
     */
    public static long round(long value, int precision)
    {
        if (precision >= MAX_PRECISION) {
            return value;
        }
        long factor = POWERS_OF_TEN[MAX_PRECISION - precision];
        return (value / factor) * factor;
    }

    /**
     * Truncate a long timestamp (epoch micros + picos) to just epoch micros.
     */
    public static long truncateToMicros(long epochMicros, int picosOfMicro)
    {
        return epochMicros;
    }

    /**
     * Returns true if the given precision is a short timestamp (fits in a long).
     */
    public static boolean isShortTimestamp(int precision)
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    /**
     * Formats a timestamp value at the given precision.
     * <p>
     * For precision 0-6, epochMicros contains the full value and picosOfMicro should be 0.
     * For precision 7-12, epochMicros and picosOfMicro together represent the full value.
     */
    public static String formatTimestamp(int precision, long epochMicros, int picosOfMicro)
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int microsFraction = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_SECOND));

        // Calculate total picoseconds in the fractional second
        long totalPicosInSecond = microsFraction * PICOSECONDS_PER_MICROSECOND + picosOfMicro;

        Instant instant = Instant.ofEpochSecond(epochSecond);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);

        // Calculate nanoseconds for Java's formatter
        int nanos = toIntExact(totalPicosInSecond / PICOSECONDS_PER_NANOSECOND);
        dateTime = dateTime.withNano(nanos);

        // Format up to nanosecond precision using Java's built-in formatting
        DateTimeFormatterBuilder formatterBuilder = new DateTimeFormatterBuilder()
                .appendPattern("uuuu-MM-dd HH:mm:ss");
        if (precision > 0) {
            int minWidth = Math.min(precision, 9);
            int maxWidth = Math.min(precision, 9);
            formatterBuilder.appendFraction(NANO_OF_SECOND, minWidth, maxWidth, true);
        }
        DateTimeFormatter formatter = formatterBuilder.toFormatter();

        String result = dateTime.format(formatter);

        // For precision > 9 (sub-nanosecond), append additional digits
        if (precision > 9) {
            long subNanoPicos = totalPicosInSecond % PICOSECONDS_PER_NANOSECOND;
            String subNanoStr = format("%03d", subNanoPicos);
            int extraDigits = precision - 9;
            result = result + subNanoStr.substring(0, extraDigits);
        }

        return result;
    }

    /**
     * Converts milliseconds to microseconds.
     */
    public static long millisToMicros(long millis)
    {
        return millis * MICROSECONDS_PER_MILLISECOND;
    }

    /**
     * Converts microseconds to milliseconds (truncation).
     */
    public static long microsToMillis(long micros)
    {
        return floorDiv(micros, MICROSECONDS_PER_MILLISECOND);
    }

    /**
     * Converts microseconds to nanoseconds.
     */
    public static long microsToNanos(long micros)
    {
        return micros * NANOSECONDS_PER_MICROSECOND;
    }

    /**
     * Converts nanoseconds to microseconds (truncation).
     */
    public static long nanosToMicros(long nanos)
    {
        return floorDiv(nanos, NANOSECONDS_PER_MICROSECOND);
    }
}
