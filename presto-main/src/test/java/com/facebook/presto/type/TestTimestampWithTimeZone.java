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
package com.facebook.presto.type;

import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;

public class TestTimestampWithTimeZone
        extends TestTimestampWithTimeZoneBase
{
    public TestTimestampWithTimeZone()
    {
        super(false);
    }

    @Override
    public void testCastToTime()
    {
        super.testCastToTime();

        functionAssertions.assertFunctionString("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as time)",
                TIME,
                "03:04:05.321");
    }

    @Test
    public void testCastToTimeWithTimeZone()
    {
        super.testCastToTimeWithTimeZone();

        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000 Europe/Warsaw' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Europe/Warsaw");
        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000 Asia/Kathmandu' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Asia/Kathmandu");
    }
}
