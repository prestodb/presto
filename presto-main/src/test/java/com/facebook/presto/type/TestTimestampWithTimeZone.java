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

import com.facebook.presto.sql.query.QueryAssertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimeOf;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;

public class TestTimestampWithTimeZone
        extends TestTimestampWithTimeZoneBase
{
    public TestTimestampWithTimeZone()
    {
        super(false);
    }

    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
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

    @Test
    public void testCastInvalidTimestamp()
    {
        assertInvalidFunction("CAST('ABC' AS TIMESTAMP WITH TIME ZONE)", INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp with time zone: ABC");
        assertInvalidFunction("CAST('2022-01-00 00:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)", INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp with time zone: 2022-01-00 00:00:00 UTC");
        assertInvalidFunction("CAST('AABC' AS TIMESTAMP WITH TIME ZONE)", INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp with time zone: AABC");
        assertInvalidFunction("CAST('2022-01-00 00:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)", INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp with time zone: 2022-01-00 00:00:00 UTC");
        assertInvalidFunction("CAST('2022-01-01 00:00:00 ABC' AS TIMESTAMP WITH TIME ZONE)", INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp with time zone: 2022-01-01 00:00:00 ABC");
    }

    @Test
    public void testJoin()
    {
        // short timestamp
        assertions.assertQuery("SELECT count(*) FROM (VALUES TIMESTAMP '2020-05-10 04:00:00 America/New_York') t(v) " +
                        "JOIN (VALUES TIMESTAMP '2020-05-10 01:00:00 America/Los_Angeles') u(v) USING (v)", "VALUES BIGINT '1'");

        // long timestamp
        assertions.assertQuery("" +
                "SELECT count(*) FROM (VALUES TIMESTAMP '2020-05-10 04:00:00.000000 America/New_York') t(v) " +
                "JOIN (VALUES TIMESTAMP '2020-05-10 01:00:00.000000 America/Los_Angeles') u(v) USING (v)", "VALUES BIGINT '1'");
    }

    @Test
    public void testTimeZoneMinute()
    {
        assertFunction("timezone_minute(TIMESTAMP '2020-05-01 12:34:56 +07:09')", BIGINT, 9L);
        assertFunction("timezone_minute(TIMESTAMP '2020-05-01 12:34:56.1 +07:09')", BIGINT, 9L);
    }

    @Test
    public void testTimeZoneHour()
    {
        assertFunction("timezone_hour(TIMESTAMP '2020-05-01 12:34:56 +07:09')", BIGINT, 7L);
        assertFunction("timezone_hour(TIMESTAMP '2020-05-01 12:34:56.1 +07:09')", BIGINT, 7L);
    }
}
