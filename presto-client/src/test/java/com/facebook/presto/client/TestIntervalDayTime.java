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
package com.facebook.presto.client;

import org.testng.annotations.Test;

import static com.facebook.presto.client.IntervalDayTime.formatMillis;
import static com.facebook.presto.client.IntervalDayTime.parseMillis;
import static com.facebook.presto.client.IntervalDayTime.toMillis;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.testng.Assert.assertEquals;

public class TestIntervalDayTime
{
    @Test
    public void testFormat()
    {
        assertMillis(0, "0 00:00:00.000");
        assertMillis(1, "0 00:00:00.001");
        assertMillis(-1, "-0 00:00:00.001");

        assertMillis(toMillis(12, 13, 45, 56, 789), "12 13:45:56.789");
        assertMillis(toMillis(-12, -13, -45, -56, -789), "-12 13:45:56.789");

        assertMillis(Long.MAX_VALUE, "106751991167 07:12:55.807");
        assertMillis(Long.MIN_VALUE + 1, "-106751991167 07:12:55.807");
        assertMillis(Long.MIN_VALUE, "-106751991167 07:12:55.808");
    }

    private static void assertMillis(long millis, String formatted)
    {
        assertEquals(formatMillis(millis), formatted);
        assertEquals(parseMillis(formatted), millis);
    }

    @Test
    public void textMaxDays()
    {
        long days = Long.MAX_VALUE / DAYS.toMillis(1);
        assertEquals(toMillis(days, 0, 0, 0, 0), DAYS.toMillis(days));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testOverflow()
    {
        long days = (Long.MAX_VALUE / DAYS.toMillis(1)) + 1;
        toMillis(days, 0, 0, 0, 0);
    }
}
