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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import org.testng.annotations.Test;

import java.util.Locale;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestTimestampType
{
    @Test
    public void testInterningReturnsSameReference()
    {
        for (int p = 0; p <= TimestampType.MAX_PRECISION; p++) {
            assertSame(createTimestampType(p), createTimestampType(p),
                    "createTimestampType(" + p + ") must return the same reference on repeated calls");
        }
    }

    @Test
    public void testConstantsAreInterned()
    {
        assertSame(createTimestampType(3), TIMESTAMP);
        assertSame(createTimestampType(6), TIMESTAMP_MICROSECONDS);
    }

    @Test
    public void testIsShortBoundary()
    {
        for (int p = 0; p <= TimestampType.MAX_SHORT_PRECISION; p++) {
            assertTrue(createTimestampType(p).isShort(), "isShort() must return true for p=" + p);
        }
        for (int p = TimestampType.MAX_SHORT_PRECISION + 1; p <= TimestampType.MAX_PRECISION; p++) {
            assertFalse(createTimestampType(p).isShort(), "isShort() must return false for p=" + p);
        }
    }

    @Test
    public void testInvalidPrecision()
    {
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(-1));
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(13));
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(Integer.MIN_VALUE));
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(Integer.MAX_VALUE));

        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> createTimestampType(-1));
        assertTrue(e1.getMessage().contains("-1"));
        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> createTimestampType(13));
        assertTrue(e2.getMessage().contains("13"));
    }

    @Test
    public void testReferenceEqualityIsTypeEquality()
    {
        for (int p1 = 0; p1 <= TimestampType.MAX_PRECISION; p1++) {
            for (int p2 = 0; p2 <= TimestampType.MAX_PRECISION; p2++) {
                if (p1 == p2) {
                    assertEquals(createTimestampType(p1), createTimestampType(p2));
                }
                else {
                    assertFalse(createTimestampType(p1).equals(createTimestampType(p2)),
                            "Types with different precision must not be equal");
                }
            }
        }
    }

    @Test
    public void testEpochComponentsMillis()
    {
        TimestampType ts = createTimestampType(3);
        assertEquals(ts.getEpochSecond(1_000L), 1L);
        assertEquals(ts.getNanos(1_000L), 0);
        assertEquals(ts.getEpochSecond(1_500L), 1L);
        assertEquals(ts.getNanos(1_500L), 500_000_000);
        // Pre-1970 timestamps: floor division gives the correct epoch-second.
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_000_000);
    }

    @Test
    public void testEpochComponentsMicros()
    {
        TimestampType ts = createTimestampType(6);
        assertEquals(ts.getEpochSecond(1_000_000L), 1L);
        assertEquals(ts.getNanos(1_000_000L), 0);
        assertEquals(ts.getEpochSecond(1_500_000L), 1L);
        assertEquals(ts.getNanos(1_500_000L), 500_000_000);
        // Floor division: pre-epoch correctness.
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_999_000);
    }

    @Test
    public void testEpochComponentsSeconds()
    {
        TimestampType ts = createTimestampType(0);
        assertEquals(ts.getEpochSecond(5L), 5L);
        assertEquals(ts.getNanos(5L), 0);
        assertEquals(ts.toEpochMillis(5L), 5_000L);
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 0);
        assertEquals(ts.toEpochMillis(-1L), -1_000L);
    }

    @Test
    public void testEpochComponentsIntermediatePrecision()
    {
        // p=4: unit = 100µs (1/10,000 of a second)
        TimestampType ts = createTimestampType(4);
        assertEquals(ts.getEpochSecond(10_000L), 1L);
        assertEquals(ts.getNanos(10_000L), 0);
        assertEquals(ts.getEpochSecond(15_000L), 1L);
        assertEquals(ts.getNanos(15_000L), 500_000_000);
        assertEquals(ts.toEpochMillis(10_000L), 1_000L);
        assertEquals(ts.toEpochMillis(15_000L), 1_500L);
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_900_000);
    }

    @Test
    public void testToEpochMillis()
    {
        assertEquals(createTimestampType(3).toEpochMillis(1_500L), 1_500L);
        assertEquals(createTimestampType(6).toEpochMillis(1_500_000L), 1_500L);
        // Pre-epoch: floor division — old MICROSECONDS.toMillis(-1µs) returned 0, not -1.
        assertEquals(createTimestampType(3).toEpochMillis(-1L), -1L);
        assertEquals(createTimestampType(6).toEpochMillis(-1L), -1L);
        assertEquals(createTimestampType(6).toEpochMillis(-999L), -1L);
        assertEquals(createTimestampType(6).toEpochMillis(-1_000L), -1L);
        assertEquals(createTimestampType(6).toEpochMillis(-1_001L), -2L);
        assertEquals(createTimestampType(3).toEpochMillis(Long.MIN_VALUE / 1_000 * 1_000), Long.MIN_VALUE / 1_000 * 1_000);
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(7).toEpochMillis(0L));
    }

    @Test
    public void testToEpochMicros()
    {
        assertEquals(createTimestampType(0).toEpochMicros(1L), 1_000_000L);
        assertEquals(createTimestampType(0).toEpochMicros(0L), 0L);
        assertEquals(createTimestampType(0).toEpochMicros(-1L), -1_000_000L);
        assertEquals(createTimestampType(3).toEpochMicros(1_500L), 1_500_000L);
        assertEquals(createTimestampType(3).toEpochMicros(0L), 0L);
        // Pre-epoch: IcebergPageSink uses toEpochMicros for p=3 (millis → micros).
        assertEquals(createTimestampType(3).toEpochMicros(-1L), -1_000L);
        assertEquals(createTimestampType(3).toEpochMicros(-1_000L), -1_000_000L);
        assertEquals(createTimestampType(3).toEpochMicros(-1_001L), -1_001_000L);
        assertEquals(createTimestampType(6).toEpochMicros(1_500_000L), 1_500_000L);
        assertEquals(createTimestampType(6).toEpochMicros(-1L), -1L);
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(7).toEpochMicros(0L));
    }

    @Test
    public void testFromEpochComponents()
    {
        TimestampType millis = createTimestampType(3);
        assertEquals(millis.fromEpochComponents(1L, 500_000_000), 1_500L);
        assertEquals(millis.fromEpochComponents(0L, 0), 0L);
        assertEquals(millis.fromEpochComponents(-1L, 999_000_000), -1L);

        TimestampType micros = createTimestampType(6);
        assertEquals(micros.fromEpochComponents(1L, 500_000_000), 1_500_000L);

        expectThrows(IllegalArgumentException.class, () -> millis.fromEpochComponents(0L, -1));
        expectThrows(IllegalArgumentException.class, () -> millis.fromEpochComponents(0L, 1_000_000_000));
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(7).fromEpochComponents(0L, 0));
    }

    @Test
    public void testLongPrecisionEpochHelpersThrow()
    {
        // toEpochMillis, toEpochMicros, and fromEpochComponents require a single-long representation
        // and throw for p > MAX_SHORT_PRECISION until LongTimestamp is wired in.
        // getEpochSecond and getNanos do not guard on precision — those guards arrive with LongTimestamp.
        for (int p = TimestampType.MAX_SHORT_PRECISION + 1; p <= TimestampType.MAX_PRECISION; p++) {
            TimestampType ts = createTimestampType(p);
            expectThrows(UnsupportedOperationException.class, () -> ts.toEpochMillis(0L));
            expectThrows(UnsupportedOperationException.class, () -> ts.toEpochMicros(0L));
            expectThrows(UnsupportedOperationException.class, () -> ts.fromEpochComponents(0L, 0));
        }
    }

    @Test
    public void testGetObjectValue()
    {
        SqlFunctionProperties nonLegacy = SqlFunctionProperties.builder()
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLegacyTimestamp(false)
                .setSessionLocale(Locale.ENGLISH)
                .setSessionUser("test")
                .build();
        SqlFunctionProperties legacy = SqlFunctionProperties.builder()
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLegacyTimestamp(true)
                .setSessionLocale(Locale.ENGLISH)
                .setSessionUser("test")
                .build();

        BlockBuilder millisBuilder = TIMESTAMP.createBlockBuilder(null, 1);
        TIMESTAMP.writeLong(millisBuilder, 1_000L);
        Block millisBlock = millisBuilder.build();
        assertEquals(TIMESTAMP.getObjectValue(nonLegacy, millisBlock, 0), new SqlTimestamp(1_000L, MILLISECONDS));
        assertEquals(TIMESTAMP.getObjectValue(legacy, millisBlock, 0), new SqlTimestamp(1_000L, TimeZoneKey.UTC_KEY, MILLISECONDS));

        BlockBuilder microsBuilder = TIMESTAMP_MICROSECONDS.createBlockBuilder(null, 1);
        TIMESTAMP_MICROSECONDS.writeLong(microsBuilder, 1_000_000L);
        Block microsBlock = microsBuilder.build();
        assertEquals(TIMESTAMP_MICROSECONDS.getObjectValue(nonLegacy, microsBlock, 0), new SqlTimestamp(1_000_000L, MICROSECONDS));
        assertEquals(TIMESTAMP_MICROSECONDS.getObjectValue(legacy, microsBlock, 0), new SqlTimestamp(1_000_000L, TimeZoneKey.UTC_KEY, MICROSECONDS));

        // Build a non-null block so the precision check is reached (null position returns early).
        BlockBuilder builder = TIMESTAMP.createBlockBuilder(null, 1);
        TIMESTAMP.writeLong(builder, 1_000L);
        Block block = builder.build();
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(1).getObjectValue(null, block, 0));
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(4).getObjectValue(null, block, 0));
        expectThrows(UnsupportedOperationException.class, () -> createTimestampType(7).getObjectValue(null, block, 0));
    }

    @Test
    public void testRoundTripFromEpochComponents()
    {
        // fromEpochComponents(getEpochSecond(v), getNanos(v)) must reconstruct v exactly.
        // IcebergPageSink relies on this invariant when converting legacy-timezone timestamps.
        long[] milliValues = {
                0L, 1L, 1_000L, 1_500L, 999L,
                -1L, -1_000L, -1_001L, -1_500L,
                Long.MAX_VALUE / 1_000 * 1_000,
        };
        TimestampType millis = createTimestampType(3);
        for (long v : milliValues) {
            assertEquals(millis.fromEpochComponents(millis.getEpochSecond(v), (int) millis.getNanos(v)), v,
                    "round-trip failed for p=3, v=" + v);
        }

        long[] microValues = {
                0L, 1L, 1_000_000L, 1_500_000L, 999_999L,
                -1L, -1_000_000L, -1_000_001L, -1_500_000L,
        };
        TimestampType micros = createTimestampType(6);
        for (long v : microValues) {
            assertEquals(micros.fromEpochComponents(micros.getEpochSecond(v), (int) micros.getNanos(v)), v,
                    "round-trip failed for p=6, v=" + v);
        }
    }
}
