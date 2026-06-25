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

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TimestampType.createTimestampType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestTimestampType
{
    @Test
    public void testInterningReturnsSameReference()
    {
        for (int p = 0; p <= 12; p++) {
            assertSame(createTimestampType(p), createTimestampType(p),
                    "createTimestampType(" + p + ") must return same reference on repeated calls");
        }
    }

    @Test
    public void testAllInstancesPreAllocated()
    {
        for (int p = 0; p <= 12; p++) {
            assertNotNull(createTimestampType(p), "Instance for precision " + p + " must be pre-allocated");
        }
    }

    @Test
    public void testTimestampConstantBackwardCompatibility()
    {
        assertSame(TIMESTAMP, createTimestampType(3), "TIMESTAMP must be same reference as createTimestampType(3)");
        assertEquals(TIMESTAMP.getPrecision(), 3, "TIMESTAMP.getPrecision() must return 3");
    }

    @Test
    public void testGetPrecisionReturnsInt()
    {
        for (int p = 0; p <= 12; p++) {
            assertEquals(createTimestampType(p).getPrecision(), p,
                    "getPrecision() must return " + p + " for createTimestampType(" + p + ")");
        }
    }

    @Test
    public void testIsShortBoundary()
    {
        for (int p = 0; p <= 6; p++) {
            assertTrue(createTimestampType(p).isShort(), "isShort() must return true for p=" + p);
        }
        for (int p = 7; p <= 12; p++) {
            assertFalse(createTimestampType(p).isShort(), "isShort() must return false for p=" + p);
        }
    }

    @Test
    public void testInvalidPrecisionThrows()
    {
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(-1));
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(13));
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(Integer.MIN_VALUE));
        expectThrows(IllegalArgumentException.class, () -> createTimestampType(Integer.MAX_VALUE));
    }

    @Test
    public void testInvalidPrecisionErrorMessage()
    {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createTimestampType(-1));
        assertTrue(e.getMessage().contains("-1"), "Error message must contain the invalid value");

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> createTimestampType(13));
        assertTrue(e2.getMessage().contains("13"), "Error message must contain the invalid value");
    }

    // Additional: TIMESTAMP_MICROSECONDS = createTimestampType(6) with precision 6
    @Test
    public void testTimestampMicrosecondsIsInterned()
    {
        assertSame(TIMESTAMP_MICROSECONDS, createTimestampType(6),
                "TIMESTAMP_MICROSECONDS must be same reference as createTimestampType(6)");
        assertEquals(TIMESTAMP_MICROSECONDS.getPrecision(), 6);
    }

    // Additional: reference equality serves as type equality (interning guarantee)
    @Test
    public void testReferenceEqualityIsTypeEquality()
    {
        for (int p1 = 0; p1 <= 12; p1++) {
            for (int p2 = 0; p2 <= 12; p2++) {
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

    // Additional: getEpochSecond and getNanos correctness for p=3 (millis)
    @Test
    public void testEpochComponentsMillis()
    {
        TimestampType ts = createTimestampType(3);
        // 1 second = 1000 ms; epochSecond=1, nanos=0
        assertEquals(ts.getEpochSecond(1_000L), 1L);
        assertEquals(ts.getNanos(1_000L), 0);
        // 1500 ms = 1s + 500ms
        assertEquals(ts.getEpochSecond(1_500L), 1L);
        assertEquals(ts.getNanos(1_500L), 500_000_000);
        // Negative: -1 ms = -1s + 999ms
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_000_000);
    }

    // Additional: getEpochSecond and getNanos correctness for p=6 (micros)
    @Test
    public void testEpochComponentsMicros()
    {
        TimestampType ts = createTimestampType(6);
        // 1 second = 1_000_000 µs
        assertEquals(ts.getEpochSecond(1_000_000L), 1L);
        assertEquals(ts.getNanos(1_000_000L), 0);
        // 1_500_000 µs = 1s + 500_000µs = 1s + 500ms
        assertEquals(ts.getEpochSecond(1_500_000L), 1L);
        assertEquals(ts.getNanos(1_500_000L), 500_000_000);
        // Negative: -1 µs → epochSecond=-1, nanos=999_999_000
        assertEquals(ts.getEpochSecond(-1L), -1L);
        assertEquals(ts.getNanos(-1L), 999_999_000);
    }

    // Additional: toEpochMillis helper
    @Test
    public void testToEpochMillis()
    {
        assertEquals(createTimestampType(3).toEpochMillis(1_500L), 1_500L);
        assertEquals(createTimestampType(6).toEpochMillis(1_500_000L), 1_500L);
    }

    @Test
    public void testToEpochMillisNegative()
    {
        TimestampType millis = createTimestampType(3);
        // -1 ms → -1 ms (epoch-millis value already is ms)
        assertEquals(millis.toEpochMillis(-1L), -1L);
        // -1001 ms → -1001 ms
        assertEquals(millis.toEpochMillis(-1_001L), -1_001L);

        TimestampType micros = createTimestampType(6);
        // -1 µs: epochSecond=-1, nanos=999_999_000 → -1*1000 + 999_999_000/1_000_000 = -1000 + 999 = -1 ms
        assertEquals(micros.toEpochMillis(-1L), -1L);
        // -1_001_000 µs → -1001 ms
        assertEquals(micros.toEpochMillis(-1_001_000L), -1_001L);
    }

    @Test
    public void testFromEpochComponentsRoundTrip()
    {
        TimestampType millis = createTimestampType(3);
        // Positive: 1s + 500ms
        long storedMillis = millis.fromEpochComponents(1L, 500_000_000);
        assertEquals(millis.getEpochSecond(storedMillis), 1L);
        assertEquals(millis.getNanos(storedMillis), 500_000_000);

        // Pre-epoch: -1s + 999ms (i.e. -1 ms)
        long storedNeg = millis.fromEpochComponents(-1L, 999_000_000);
        assertEquals(millis.getEpochSecond(storedNeg), -1L);
        assertEquals(millis.getNanos(storedNeg), 999_000_000);

        TimestampType micros = createTimestampType(6);
        // Positive: 1s + 500_000µs
        long storedMicros = micros.fromEpochComponents(1L, 500_000_000);
        assertEquals(micros.getEpochSecond(storedMicros), 1L);
        assertEquals(micros.getNanos(storedMicros), 500_000_000);

        // Pre-epoch: -1s + 999_999µs (i.e. -1 µs)
        long storedNegMicros = micros.fromEpochComponents(-1L, 999_999_000);
        assertEquals(micros.getEpochSecond(storedNegMicros), -1L);
        assertEquals(micros.getNanos(storedNegMicros), 999_999_000);
    }
}
