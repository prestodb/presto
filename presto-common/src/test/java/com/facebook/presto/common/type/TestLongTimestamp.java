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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestLongTimestamp
{
    @Test
    public void testConstruction()
    {
        LongTimestamp ts = new LongTimestamp(123456L, 789);
        assertEquals(ts.getEpochMicros(), 123456L);
        assertEquals(ts.getPicosOfMicro(), 789);
    }

    @Test
    public void testConstructionWithZeroPicos()
    {
        LongTimestamp ts = new LongTimestamp(0L, 0);
        assertEquals(ts.getEpochMicros(), 0L);
        assertEquals(ts.getPicosOfMicro(), 0);
    }

    @Test
    public void testConstructionWithMaxPicos()
    {
        LongTimestamp ts = new LongTimestamp(0L, 999999);
        assertEquals(ts.getEpochMicros(), 0L);
        assertEquals(ts.getPicosOfMicro(), 999999);
    }

    @Test
    public void testConstructionWithNegativeEpochMicros()
    {
        LongTimestamp ts = new LongTimestamp(-1L, 0);
        assertEquals(ts.getEpochMicros(), -1L);
        assertEquals(ts.getPicosOfMicro(), 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativePicosThrows()
    {
        new LongTimestamp(0L, -1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPicosExceedingMaxThrows()
    {
        new LongTimestamp(0L, 1_000_000);
    }

    @Test
    public void testEquals()
    {
        LongTimestamp ts1 = new LongTimestamp(100L, 42);
        LongTimestamp ts2 = new LongTimestamp(100L, 42);
        LongTimestamp ts3 = new LongTimestamp(100L, 43);
        LongTimestamp ts4 = new LongTimestamp(101L, 42);

        assertTrue(ts1.equals(ts2));
        assertFalse(ts1.equals(ts3));
        assertFalse(ts1.equals(ts4));
        assertFalse(ts1.equals(null));
        assertFalse(ts1.equals("not a timestamp"));
    }

    @Test
    public void testHashCode()
    {
        LongTimestamp ts1 = new LongTimestamp(100L, 42);
        LongTimestamp ts2 = new LongTimestamp(100L, 42);
        assertEquals(ts1.hashCode(), ts2.hashCode());

        // Different values should typically have different hash codes
        LongTimestamp ts3 = new LongTimestamp(200L, 42);
        assertNotEquals(ts1.hashCode(), ts3.hashCode());
    }

    @Test
    public void testCompareTo()
    {
        LongTimestamp ts1 = new LongTimestamp(100L, 42);
        LongTimestamp ts2 = new LongTimestamp(100L, 42);
        LongTimestamp ts3 = new LongTimestamp(100L, 43);
        LongTimestamp ts4 = new LongTimestamp(101L, 0);
        LongTimestamp ts5 = new LongTimestamp(99L, 999999);

        assertEquals(ts1.compareTo(ts2), 0);
        assertTrue(ts1.compareTo(ts3) < 0);
        assertTrue(ts3.compareTo(ts1) > 0);
        assertTrue(ts1.compareTo(ts4) < 0);
        assertTrue(ts4.compareTo(ts1) > 0);
        assertTrue(ts5.compareTo(ts1) < 0);
        assertTrue(ts1.compareTo(ts5) > 0);
    }

    @Test
    public void testCompareToWithNegativeEpochMicros()
    {
        LongTimestamp ts1 = new LongTimestamp(-100L, 0);
        LongTimestamp ts2 = new LongTimestamp(-100L, 500000);
        LongTimestamp ts3 = new LongTimestamp(0L, 0);

        assertTrue(ts1.compareTo(ts2) < 0);
        assertTrue(ts2.compareTo(ts3) < 0);
    }

    @Test
    public void testToString()
    {
        // LongTimestamp.toString() uses Timestamps.formatTimestamp(12, epochMicros, picosOfMicro)

        // Epoch timestamp: 1970-01-01 00:00:00.000000000000
        LongTimestamp epoch = new LongTimestamp(0L, 0);
        String epochStr = epoch.toString();
        assertEquals(epochStr, "1970-01-01 00:00:00.000000000000");
        assertEquals(epochStr.substring(epochStr.indexOf('.') + 1).length(), 12, "Expected 12 fractional digits for epoch timestamp");

        // Negative timestamp just before the epoch:
        // -1 micro = 1969-12-31 23:59:59.999999000000
        LongTimestamp negative = new LongTimestamp(-1L, 0);
        String negativeStr = negative.toString();
        assertEquals(negativeStr, "1969-12-31 23:59:59.999999000000");
        assertEquals(negativeStr.substring(negativeStr.indexOf('.') + 1).length(), 12, "Expected 12 fractional digits for negative timestamp");
    }

    @Test
    public void testToStringWithPositiveTimestamp()
    {
        // 2020-01-01 00:00:00.000000123456 = 1577836800 seconds = 1577836800000000 micros, picosOfMicro = 123456
        LongTimestamp ts = new LongTimestamp(1577836800000000L, 123456);
        String str = ts.toString();
        assertEquals(str, "2020-01-01 00:00:00.000000123456");
        assertEquals(str.substring(str.indexOf('.') + 1).length(), 12, "Expected 12 fractional digits for positive timestamp");
    }

    @Test
    public void testSelfEquality()
    {
        LongTimestamp ts = new LongTimestamp(100L, 42);
        assertTrue(ts.equals(ts));
    }

    @Test
    public void testCompareToSelf()
    {
        LongTimestamp ts = new LongTimestamp(100L, 42);
        assertEquals(ts.compareTo(ts), 0);
    }

    @Test
    public void testBoundaryValues()
    {
        LongTimestamp minTs = new LongTimestamp(Long.MIN_VALUE, 0);
        LongTimestamp maxTs = new LongTimestamp(Long.MAX_VALUE, 999999);

        assertTrue(minTs.compareTo(maxTs) < 0);
        assertTrue(maxTs.compareTo(minTs) > 0);
        assertNotEquals(minTs, maxTs);
    }
}
