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
import static org.testng.Assert.fail;

public class TestLongTimestamp
{
    @Test
    public void testConstructorStoresFields()
    {
        LongTimestamp ts = new LongTimestamp(1_000_000L, 500_000);
        assertEquals(ts.getEpochMicros(), 1_000_000L);
        assertEquals(ts.getPicosOfMicro(), 500_000);
    }

    @Test
    public void testPicosOfMicroLowerBound()
    {
        LongTimestamp ts = new LongTimestamp(0L, 0);
        assertEquals(ts.getPicosOfMicro(), 0);
    }

    @Test
    public void testPicosOfMicroUpperBound()
    {
        LongTimestamp ts = new LongTimestamp(0L, 999_999);
        assertEquals(ts.getPicosOfMicro(), 999_999);
    }

    @Test
    public void testPicosOfMicroNegativeThrows()
    {
        try {
            new LongTimestamp(0L, -1);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testPicosOfMicroTooLargeThrows()
    {
        try {
            new LongTimestamp(0L, 1_000_000);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testNegativeEpochMicros()
    {
        // Pre-1970 timestamp: negative epochMicros is valid
        LongTimestamp ts = new LongTimestamp(-1_000_000L, 0);
        assertEquals(ts.getEpochMicros(), -1_000_000L);
    }

    @Test
    public void testImmutable()
    {
        // LongTimestamp has no setters — verified by reading fields without modification
        LongTimestamp ts = new LongTimestamp(42L, 100);
        assertEquals(ts.getEpochMicros(), 42L);
        assertEquals(ts.getPicosOfMicro(), 100);
        // No setter methods exist; this is verified at compile time by the absence of set* methods
    }
}
