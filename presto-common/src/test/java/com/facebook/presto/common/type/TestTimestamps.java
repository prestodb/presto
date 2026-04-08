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

import org.testng.annotations.Test;

import static com.facebook.presto.common.type.Timestamps.DEFAULT_PRECISION;
import static com.facebook.presto.common.type.Timestamps.MAX_PRECISION;
import static com.facebook.presto.common.type.Timestamps.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static com.facebook.presto.common.type.Timestamps.MICROSECONDS_PER_SECOND;
import static com.facebook.presto.common.type.Timestamps.MILLISECONDS_PER_DAY;
import static com.facebook.presto.common.type.Timestamps.MILLISECONDS_PER_SECOND;
import static com.facebook.presto.common.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static com.facebook.presto.common.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static com.facebook.presto.common.type.Timestamps.NANOSECONDS_PER_SECOND;
import static com.facebook.presto.common.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static com.facebook.presto.common.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static com.facebook.presto.common.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static com.facebook.presto.common.type.Timestamps.PICOSECONDS_PER_SECOND;
import static com.facebook.presto.common.type.Timestamps.POWERS_OF_TEN;
import static com.facebook.presto.common.type.Timestamps.formatTimestamp;
import static com.facebook.presto.common.type.Timestamps.isShortTimestamp;
import static com.facebook.presto.common.type.Timestamps.microsToMillis;
import static com.facebook.presto.common.type.Timestamps.microsToNanos;
import static com.facebook.presto.common.type.Timestamps.millisToMicros;
import static com.facebook.presto.common.type.Timestamps.nanosToMicros;
import static com.facebook.presto.common.type.Timestamps.rescale;
import static com.facebook.presto.common.type.Timestamps.round;
import static com.facebook.presto.common.type.Timestamps.roundDiv;
import static com.facebook.presto.common.type.Timestamps.truncateToMicros;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTimestamps
{
    @Test
    public void testConstants()
    {
        assertEquals(MAX_PRECISION, 12);
        assertEquals(MAX_SHORT_PRECISION, 6);
        assertEquals(DEFAULT_PRECISION, 3);

        assertEquals(MILLISECONDS_PER_SECOND, 1_000L);
        assertEquals(MICROSECONDS_PER_SECOND, 1_000_000L);
        assertEquals(NANOSECONDS_PER_SECOND, 1_000_000_000L);
        assertEquals(PICOSECONDS_PER_SECOND, 1_000_000_000_000L);

        assertEquals(MICROSECONDS_PER_MILLISECOND, 1_000L);
        assertEquals(NANOSECONDS_PER_MILLISECOND, 1_000_000L);
        assertEquals(PICOSECONDS_PER_MILLISECOND, 1_000_000_000L);

        assertEquals(NANOSECONDS_PER_MICROSECOND, 1_000L);
        assertEquals(PICOSECONDS_PER_MICROSECOND, 1_000_000L);

        assertEquals(PICOSECONDS_PER_NANOSECOND, 1_000L);

        assertEquals(MILLISECONDS_PER_DAY, 86_400_000L);
    }

    @Test
    public void testPowersOfTen()
    {
        assertEquals(POWERS_OF_TEN.length, 13);
        assertEquals(POWERS_OF_TEN[0], 1L);
        assertEquals(POWERS_OF_TEN[1], 10L);
        assertEquals(POWERS_OF_TEN[3], 1_000L);
        assertEquals(POWERS_OF_TEN[6], 1_000_000L);
        assertEquals(POWERS_OF_TEN[9], 1_000_000_000L);
        assertEquals(POWERS_OF_TEN[12], 1_000_000_000_000L);
    }

    @Test
    public void testRescaleSamePrecision()
    {
        assertEquals(rescale(123456L, 6, 6), 123456L);
        assertEquals(rescale(0L, 3, 3), 0L);
    }

    @Test
    public void testRescaleUp()
    {
        // From ms to µs: multiply by 1000
        assertEquals(rescale(123L, 3, 6), 123000L);
        // From seconds to ms: multiply by 1000
        assertEquals(rescale(5L, 0, 3), 5000L);
        // From ms to ns: multiply by 1_000_000
        assertEquals(rescale(1L, 3, 9), 1_000_000L);
    }

    @Test
    public void testRescaleDown()
    {
        // From µs to ms: divide by 1000
        assertEquals(rescale(123456L, 6, 3), 123L);
        // From ns to ms: divide by 1_000_000
        assertEquals(rescale(1_234_567_890L, 9, 3), 1234L);
        // From µs to seconds
        assertEquals(rescale(5_000_000L, 6, 0), 5L);
    }

    @Test
    public void testRescaleDownTruncation()
    {
        // Truncation behavior (toward zero)
        assertEquals(rescale(1999L, 6, 3), 1L);
        assertEquals(rescale(-1999L, 6, 3), -1L);
    }

    @Test
    public void testRoundDiv()
    {
        assertEquals(roundDiv(10, 3), 3);
        assertEquals(roundDiv(-10, 3), -3);
        assertEquals(roundDiv(0, 3), 0);
        assertEquals(roundDiv(9, 3), 3);
    }

    @Test
    public void testRound()
    {
        // At max precision, no rounding
        assertEquals(round(123456789012L, 12), 123456789012L);

        // Round to precision 9 (truncate last 3 digits of picos)
        assertEquals(round(123456789012L, 9), 123456789000L);

        // Round to precision 6
        assertEquals(round(123456789012L, 6), 123456000000L);

        // Round to precision 3
        assertEquals(round(123456789012L, 3), 123000000000L);

        // Round to precision 0
        assertEquals(round(123456789012L, 0), 0L);
    }

    @Test
    public void testTruncateToMicros()
    {
        assertEquals(truncateToMicros(12345L, 678), 12345L);
        assertEquals(truncateToMicros(-1L, 999999), -1L);
    }

    @Test
    public void testIsShortTimestamp()
    {
        for (int p = 0; p <= 6; p++) {
            assertTrue(isShortTimestamp(p), "Precision " + p + " should be short");
        }
        for (int p = 7; p <= 12; p++) {
            assertFalse(isShortTimestamp(p), "Precision " + p + " should not be short");
        }
    }

    @Test
    public void testFormatTimestampEpoch()
    {
        // Epoch at precision 3
        assertEquals(formatTimestamp(3, 0L, 0), "1970-01-01 00:00:00.000");
    }

    @Test
    public void testFormatTimestampPrecision0()
    {
        assertEquals(formatTimestamp(0, 0L, 0), "1970-01-01 00:00:00");
    }

    @Test
    public void testFormatTimestampPrecision6()
    {
        // 1 second in microseconds
        assertEquals(formatTimestamp(6, 1_000_000L, 0), "1970-01-01 00:00:01.000000");
    }

    @Test
    public void testFormatTimestampPrecision9()
    {
        // 1 second and 1 microsecond
        assertEquals(formatTimestamp(9, 1_000_001L, 0), "1970-01-01 00:00:01.000001000");
    }

    @Test
    public void testFormatTimestampPrecision12()
    {
        // 1 second, 1 microsecond, 123 picoseconds of micro
        assertEquals(formatTimestamp(12, 1_000_001L, 123), "1970-01-01 00:00:01.000001000123");
    }

    @Test
    public void testFormatTimestampNegative()
    {
        // -1 microsecond from epoch
        String result = formatTimestamp(6, -1L, 0);
        assertTrue(result.contains("1969-12-31"), "Expected 1969-12-31 for negative timestamp: " + result);
    }

    @Test
    public void testMillisToMicros()
    {
        assertEquals(millisToMicros(0L), 0L);
        assertEquals(millisToMicros(1L), 1_000L);
        assertEquals(millisToMicros(-1L), -1_000L);
        assertEquals(millisToMicros(1000L), 1_000_000L);
    }

    @Test
    public void testMicrosToMillis()
    {
        assertEquals(microsToMillis(0L), 0L);
        assertEquals(microsToMillis(1_000L), 1L);
        assertEquals(microsToMillis(1_500L), 1L);
        assertEquals(microsToMillis(-1L), -1L); // floorDiv behavior
        assertEquals(microsToMillis(-1_000L), -1L);
    }

    @Test
    public void testMicrosToNanos()
    {
        assertEquals(microsToNanos(0L), 0L);
        assertEquals(microsToNanos(1L), 1_000L);
        assertEquals(microsToNanos(-1L), -1_000L);
    }

    @Test
    public void testNanosToMicros()
    {
        assertEquals(nanosToMicros(0L), 0L);
        assertEquals(nanosToMicros(1_000L), 1L);
        assertEquals(nanosToMicros(1_500L), 1L);
        assertEquals(nanosToMicros(-1L), -1L); // floorDiv behavior
    }
}
