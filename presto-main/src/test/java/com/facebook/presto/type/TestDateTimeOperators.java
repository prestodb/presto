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

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestingSqlTime.sqlTimestampOf;

public class TestDateTimeOperators extends TestDateTimeOperatorsBase
{
    public TestDateTimeOperators()
    {
        super(
                testSessionBuilder()
                        .setTimeZoneKey(TIME_ZONE_KEY)
                        .setSystemProperty("legacy_timestamp", "false")
                        .build()
        );
    }

    @Test
    public void testTimeZoneGapIsNotApplied()
    {
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '1' hour", TIMESTAMP, sqlTimestampOf(2013, 3, 31, 1, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '2' hour", TIMESTAMP, sqlTimestampOf(2013, 3, 31, 2, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '3' hour", TIMESTAMP, sqlTimestampOf(2013, 3, 31, 3, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));

        assertFunction("TIMESTAMP '2013-03-31 04:05' - INTERVAL '3' hour", TIMESTAMP, sqlTimestampOf(2013, 3, 31, 1, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
        assertFunction("TIMESTAMP '2013-03-31 03:05' - INTERVAL '2' hour", TIMESTAMP, sqlTimestampOf(2013, 3, 31, 1, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
        assertFunction("TIMESTAMP '2013-03-31 01:05' - INTERVAL '1' hour", TIMESTAMP, sqlTimestampOf(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
    }

    @Test
    public void testDaylightTimeSavingSwitchCrossingIsNotApplied()
    {
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '1' hour", TIMESTAMP, sqlTimestampOf(2013, 10, 27, 1, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '2' hour", TIMESTAMP, sqlTimestampOf(2013, 10, 27, 2, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));

        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '3' hour", TIMESTAMP, sqlTimestampOf(2013, 10, 27, 3, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '4' hour", TIMESTAMP, sqlTimestampOf(2013, 10, 27, 4, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '4' hour", TIMESTAMP, sqlTimestampOf(2013, 10, 26, 23, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
        assertFunction("TIMESTAMP '2013-10-27 02:05' - INTERVAL '2' hour", TIMESTAMP, sqlTimestampOf(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
        assertFunction("TIMESTAMP '2013-10-27 01:05' - INTERVAL '1' hour", TIMESTAMP, sqlTimestampOf(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '1' hour", TIMESTAMP, sqlTimestampOf(2013, 10, 27, 2, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '2' hour", TIMESTAMP, sqlTimestampOf(2013, 10, 27, 1, 5, 0, 0, TIME_ZONE, TIME_ZONE_KEY, SESSION));
    }
}
