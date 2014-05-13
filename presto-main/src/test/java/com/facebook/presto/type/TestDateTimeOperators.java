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

import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Locale;

import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.testng.Assert.fail;

public class TestDateTimeOperators
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    private static final DateTimeZone WEIRD_TIME_ZONE = DateTimeZone.forOffsetHoursMinutes(5, 9);
    private static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(5 * 60 + 9);

    private ConnectorSession session;
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        session = new ConnectorSession("user", "test", "catalog", "schema", TIME_ZONE_KEY, Locale.ENGLISH, null, null);
        functionAssertions = new FunctionAssertions(session);
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testDatePlusInterval()
    {
        assertFunction("DATE '2001-1-22' + INTERVAL '3' day", new SqlDate(new DateTime(2001, 1, 25, 0, 0, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + DATE '2001-1-22'", new SqlDate(new DateTime(2001, 1, 25, 0, 0, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("DATE '2001-1-22' + INTERVAL '3' month", new SqlDate(new DateTime(2001, 4, 22, 0, 0, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + DATE '2001-1-22'", new SqlDate(new DateTime(2001, 4, 22, 0, 0, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("DATE '2001-1-22' + INTERVAL '3' year", new SqlDate(new DateTime(2004, 1, 22, 0, 0, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + DATE '2001-1-22'", new SqlDate(new DateTime(2004, 1, 22, 0, 0, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        try {
            functionAssertions.selectSingleValue("DATE '2001-1-22' + INTERVAL '3' hour");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }

        try {
            functionAssertions.selectSingleValue("INTERVAL '3' hour + DATE '2001-1-22'");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testTimePlusInterval()
    {
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' hour", new SqlTime(new DateTime(1970, 1, 1, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIME '03:04:05.321'", new SqlTime(new DateTime(1970, 1, 1, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' day", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIME '03:04:05.321'", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' month", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIME '03:04:05.321'", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' year", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIME '03:04:05.321'", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321' + INTERVAL '27' hour", new SqlTime(new DateTime(1970, 1, 1, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '27' hour + TIME '03:04:05.321'", new SqlTime(new DateTime(1970, 1, 1, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' hour",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIME '03:04:05.321 +05:09'",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' day",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIME '03:04:05.321 +05:09'",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' month",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIME '03:04:05.321 +05:09'",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' year",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIME '03:04:05.321 +05:09'",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '27' hour",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '27' hour + TIME '03:04:05.321 +05:09'",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testTimestampPlusInterval()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' hour",
                new SqlTimestamp(new DateTime(2001, 1, 22, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIMESTAMP '2001-1-22 03:04:05.321'",
                new SqlTimestamp(new DateTime(2001, 1, 22, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' day",
                new SqlTimestamp(new DateTime(2001, 1, 25, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIMESTAMP '2001-1-22 03:04:05.321'",
                new SqlTimestamp(new DateTime(2001, 1, 25, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' month",
                new SqlTimestamp(new DateTime(2001, 4, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIMESTAMP '2001-1-22 03:04:05.321'",
                new SqlTimestamp(new DateTime(2001, 4, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' year",
                new SqlTimestamp(new DateTime(2004, 1, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIMESTAMP '2001-1-22 03:04:05.321'",
                new SqlTimestamp(new DateTime(2004, 1, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' hour",
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' day",
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 25, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 25, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' month",
                new SqlTimestampWithTimeZone(new DateTime(2001, 4, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                new SqlTimestampWithTimeZone(new DateTime(2001, 4, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' year",
                new SqlTimestampWithTimeZone(new DateTime(2004, 1, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                new SqlTimestampWithTimeZone(new DateTime(2004, 1, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testDateMinusInterval()
    {
        assertFunction("DATE '2001-1-22' - INTERVAL '3' day", new SqlDate(new DateTime(2001, 1, 19, 0, 0, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        try {
            functionAssertions.selectSingleValue("DATE '2001-1-22' - INTERVAL '3' hour");
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testTimeMinusInterval()
    {
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' hour", new SqlTime(new DateTime(1970, 1, 1, 0, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' day", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' month", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' year", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321' - INTERVAL '6' hour", new SqlTime(new DateTime(1970, 1, 1, 21, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' hour",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 0, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' day",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' month",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' year",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '6' hour",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 21, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testTimestampMinusInterval()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' day",
                new SqlTimestamp(new DateTime(2001, 1, 19, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' day",
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 19, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' month",
                new SqlTimestamp(new DateTime(2000, 10, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' month",
                new SqlTimestampWithTimeZone(new DateTime(2000, 10, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testTimeZoneGap()
    {
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '1' hour", new SqlTimestamp(new DateTime(2013, 3, 31, 1, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '2' hour", new SqlTimestamp(new DateTime(2013, 3, 31, 3, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '3' hour", new SqlTimestamp(new DateTime(2013, 3, 31, 4, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2013-03-31 04:05' - INTERVAL '3' hour", new SqlTimestamp(new DateTime(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 03:05' - INTERVAL '2' hour", new SqlTimestamp(new DateTime(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 01:05' - INTERVAL '1' hour", new SqlTimestamp(new DateTime(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testDateToTimestampCoercing()
    {
        assertFunction("date_format(DATE '2013-10-27', '%Y-%m-%d %H:%i:%s')", "2013-10-27 00:00:00");

        assertFunction("DATE '2013-10-27' = TIMESTAMP '2013-10-27 00:00:00'", true);
        assertFunction("DATE '2013-10-27' < TIMESTAMP '2013-10-27 00:00:01'", true);
        assertFunction("DATE '2013-10-27' > TIMESTAMP '2013-10-26 23:59:59'", true);
    }

    @Test
    public void testDateToTimestampWithZoneCoercing()
    {
        assertFunction("DATE '2013-10-27' = TIMESTAMP '2013-10-27 00:00:00 Europe/Berlin'", true);
        assertFunction("DATE '2013-10-27' < TIMESTAMP '2013-10-27 00:00:01 Europe/Berlin'", true);
        assertFunction("DATE '2013-10-27' > TIMESTAMP '2013-10-26 23:59:59 Europe/Berlin'", true);
    }

    @Test
    public void testTimeZoneDuplicate()
    {
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '1' hour",
                new SqlTimestamp(new DateTime(2013, 10, 27, 1, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '2' hour",
                new SqlTimestamp(new DateTime(2013, 10, 27, 2, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        // we need to manipulate millis directly here because 2 am has two representations in out time zone, and we need the second one
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '3' hour",
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis() + HOURS.toMillis(3), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '4' hour",
                new SqlTimestamp(new DateTime(2013, 10, 27, 3, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '4' hour",
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 02:05' - INTERVAL '2' hour",
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 01:05' - INTERVAL '1' hour",
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '1' hour",
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis() + HOURS.toMillis(3), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '2' hour",
                new SqlTimestamp(new DateTime(2013, 10, 27, 2, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }
}
