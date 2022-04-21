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

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestSqlTimestamp
{
    @Test
    public void testMillisToString()
    {
        assertEquals(toSqlTimestampString(0L, MILLISECONDS), "1970-01-01 00:00:00.000");
        assertEquals(toSqlTimestampString(1L, MILLISECONDS), "1970-01-01 00:00:00.001");
        assertEquals(toSqlTimestampString(999L, MILLISECONDS), "1970-01-01 00:00:00.999");
        assertEquals(toSqlTimestampString(1_000L, MILLISECONDS), "1970-01-01 00:00:01.000");

        // Some negative times.
        assertEquals(toSqlTimestampString(-1L, MILLISECONDS), "1969-12-31 23:59:59.999");
        assertEquals(toSqlTimestampString(-999L, MILLISECONDS), "1969-12-31 23:59:59.001");
        assertEquals(toSqlTimestampString(-60_000_000_000_789L, MILLISECONDS), "0068-09-03 13:19:59.211");

        // Some positive times
        assertEquals(toSqlTimestampString(1_650_483_250_507L, MILLISECONDS), "2022-04-20 19:34:10.507");
        assertEquals(toSqlTimestampString(60_000_000_000_789L, MILLISECONDS), "3871-04-29 10:40:00.789");
        assertEquals(toSqlTimestampString(230_000_000_000_999L, MILLISECONDS), "9258-05-30 00:53:20.999");
    }

    @Test
    public void testEqualsHashcodeMillis()
    {
        SqlTimestamp t1Millis = new SqlTimestamp(0, MILLISECONDS);
        assertEquals(t1Millis.toString(), "1970-01-01 00:00:00.000");

        SqlTimestamp t2Millis = new SqlTimestamp(0, MILLISECONDS);
        assertEquals(t1Millis, t2Millis);
        assertEquals(t1Millis.hashCode(), t2Millis.hashCode());

        SqlTimestamp t3Millis = new SqlTimestamp(1, MILLISECONDS);
        assertNotEquals(t1Millis, t3Millis);
    }

    private static String toSqlTimestampString(long value, TimeUnit precision)
    {
        SqlTimestamp timestamp = new SqlTimestamp(value, precision);
        return timestamp.toString();
    }
}
