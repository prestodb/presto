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
package com.facebook.presto.hive;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveUtil.parseHiveTimestamp;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestHiveUtil
{
    @Test
    public void testParseHiveTimestamp()
    {
        DateTime time = new DateTime(2011, 5, 6, 7, 8, 9, 123);
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss"), unixTime(time));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.S"), unixTime(time));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSS"), unixTime(time));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSS"), unixTime(time));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"), unixTime(time));
    }

    private static long parse(DateTime time, String pattern)
    {
        return parseHiveTimestamp(DateTimeFormat.forPattern(pattern).print(time));
    }

    private static long unixTime(DateTime time)
    {
        return MILLISECONDS.toSeconds(time.getMillis());
    }
}
