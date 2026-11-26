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

import static com.facebook.presto.common.type.TimestampConstants.MAX_PICOS_OF_MILLI;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.expectThrows;

public class TestLongTimestampWithTimeZone
{
    @Test
    public void testConstructorStoresFields()
    {
        LongTimestampWithTimeZone timestamp = new LongTimestampWithTimeZone(-1_000L, 500_000, (short) 7);
        assertEquals(timestamp.getEpochMillis(), -1_000L);
        assertEquals(timestamp.getPicosOfMilli(), 500_000);
        assertEquals(timestamp.getTimeZoneKey(), (short) 7);
    }

    @Test
    public void testPicosOfMilliBounds()
    {
        assertEquals(new LongTimestampWithTimeZone(0L, 0, (short) 0).getPicosOfMilli(), 0);
        assertEquals(new LongTimestampWithTimeZone(0L, MAX_PICOS_OF_MILLI, (short) 0).getPicosOfMilli(), MAX_PICOS_OF_MILLI);
        expectThrows(IllegalArgumentException.class, () -> new LongTimestampWithTimeZone(0L, -1, (short) 0));
        expectThrows(IllegalArgumentException.class, () -> new LongTimestampWithTimeZone(0L, MAX_PICOS_OF_MILLI + 1, (short) 0));
    }

    @Test
    public void testEqualsHashCodeToString()
    {
        LongTimestampWithTimeZone timestamp = new LongTimestampWithTimeZone(42L, 100, (short) 1);
        LongTimestampWithTimeZone same = new LongTimestampWithTimeZone(42L, 100, (short) 1);

        assertEquals(timestamp, same);
        assertEquals(timestamp.hashCode(), same.hashCode());
        assertNotEquals(timestamp, new LongTimestampWithTimeZone(42L, 101, (short) 1));
        assertNotEquals(timestamp, new LongTimestampWithTimeZone(42L, 100, (short) 2));

        assertEquals(timestamp.toString(), "LongTimestampWithTimeZone{epochMillis=42, picosOfMilli=100, timeZoneKey=1}");
    }
}
