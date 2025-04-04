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

import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimeOf;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;

public class TestTimestampWithTimeZone
        extends TestTimestampWithTimeZoneBase
{
    public TestTimestampWithTimeZone()
    {
        super(false);
    }

    @Test
    @Override
    public void testCastToTime()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as time)",
                TIME,
                sqlTimeOf(3, 4, 5, 321, session));

        functionAssertions.assertFunctionString("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as time)",
                TIME,
                "03:04:05.321");

        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321123 +07:09' as time)",
                TIME,
                sqlTimeOf(3, 4, 5, 321, session));

        functionAssertions.assertFunctionString("cast(TIMESTAMP '2001-1-22 03:04:05.321123 +07:09' as time)",
                TIME,
                "03:04:05.321");

        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321123456 +07:09' as time)",
                TIME,
                sqlTimeOf(3, 4, 5, 321, session));

        functionAssertions.assertFunctionString("cast(TIMESTAMP '2001-1-22 03:04:05.321123456 +07:09' as time)",
                TIME,
                "03:04:05.321");
    }

    @Test
    @Override
    public void testCastToTimeWithTimeZone()
    {
        super.testCastToTimeWithTimeZone();

        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000 Europe/Warsaw' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Europe/Warsaw");
        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000 Asia/Kathmandu' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Asia/Kathmandu");
        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000123 Europe/Warsaw' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Europe/Warsaw");
        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000123 Asia/Kathmandu' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Asia/Kathmandu");
        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000123456 Europe/Warsaw' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Europe/Warsaw");
        functionAssertions.assertFunctionString("cast(TIMESTAMP '2017-06-06 10:00:00.000123456 Asia/Kathmandu' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "10:00:00.000 Asia/Kathmandu");
    }

    @Test
    @Override
    public void testCastToTimestamp()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));

        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321123 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));

        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321123456 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));

        // This TZ had switch in 2014, so if we test for 2014 and used unpacked value we would use wrong shift
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 Pacific/Bougainville' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));

        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321123 Pacific/Bougainville' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));

        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321123456 Pacific/Bougainville' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));
    }
}
