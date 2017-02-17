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

import com.facebook.presto.Session;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.fail;

public class TestDateTimeOperators
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    private static final DateTimeZone WEIRD_TIME_ZONE = DateTimeZone.forOffsetHoursMinutes(5, 9);
    private static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(5 * 60 + 9);

    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build();
        functionAssertions = new FunctionAssertions(session);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(functionAssertions);
        functionAssertions = null;
    }

    private void assertFunction(String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }

    @Test
    public void testDatePlusInterval()
    {
        assertFunction("DATE '2001-1-22' + INTERVAL '3' day", DATE, toDate(new DateTime(2001, 1, 25, 0, 0, 0, 0, UTC)));
        assertFunction("INTERVAL '3' day + DATE '2001-1-22'", DATE, toDate(new DateTime(2001, 1, 25, 0, 0, 0, 0, UTC)));
        assertFunction("DATE '2001-1-22' + INTERVAL '3' month", DATE, toDate(new DateTime(2001, 4, 22, 0, 0, 0, 0, UTC)));
        assertFunction("INTERVAL '3' month + DATE '2001-1-22'", DATE, toDate(new DateTime(2001, 4, 22, 0, 0, 0, 0, UTC)));
        assertFunction("DATE '2001-1-22' + INTERVAL '3' year", DATE, toDate(new DateTime(2004, 1, 22, 0, 0, 0, 0, UTC)));
        assertFunction("INTERVAL '3' year + DATE '2001-1-22'", DATE, toDate(new DateTime(2004, 1, 22, 0, 0, 0, 0, UTC)));

        try {
            functionAssertions.tryEvaluate("DATE '2001-1-22' + INTERVAL '3' hour", DATE);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }

        try {
            functionAssertions.tryEvaluate("INTERVAL '3' hour + DATE '2001-1-22'", DATE);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testTimePlusInterval()
    {
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' hour", TIME, new SqlTime(new DateTime(1970, 1, 1, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIME '03:04:05.321'", TIME, new SqlTime(new DateTime(1970, 1, 1, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' day", TIME, new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIME '03:04:05.321'", TIME, new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' month", TIME, new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIME '03:04:05.321'", TIME, new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' year", TIME, new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIME '03:04:05.321'", TIME, new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321' + INTERVAL '27' hour", TIME, new SqlTime(new DateTime(1970, 1, 1, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '27' hour + TIME '03:04:05.321'", TIME, new SqlTime(new DateTime(1970, 1, 1, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' hour",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIME '03:04:05.321 +05:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' day",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIME '03:04:05.321 +05:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' month",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIME '03:04:05.321 +05:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '3' year",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIME '03:04:05.321 +05:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321 +05:09' + INTERVAL '27' hour",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '27' hour + TIME '03:04:05.321 +05:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testTimestampPlusInterval()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2001, 1, 22, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2001, 1, 22, 6, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' day",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2001, 1, 25, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2001, 1, 25, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' month",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2001, 4, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2001, 4, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' year",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2004, 1, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIMESTAMP '2001-1-22 03:04:05.321'",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2004, 1, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' hour",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' hour + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 6, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' day",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 25, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' day + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 25, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' month",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 4, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' month + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 4, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' + INTERVAL '3' year",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2004, 1, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("INTERVAL '3' year + TIMESTAMP '2001-1-22 03:04:05.321 +05:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2004, 1, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testDateMinusInterval()
    {
        assertFunction("DATE '2001-1-22' - INTERVAL '3' day", DATE, toDate(new DateTime(2001, 1, 19, 0, 0, 0, 0, UTC)));

        try {
            functionAssertions.tryEvaluate("DATE '2001-1-22' - INTERVAL '3' hour", DATE);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testTimeMinusInterval()
    {
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' hour", TIME, new SqlTime(new DateTime(1970, 1, 1, 0, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' day", TIME, new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' month", TIME, new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' year", TIME, new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321' - INTERVAL '6' hour", TIME, new SqlTime(new DateTime(1970, 1, 1, 21, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' hour",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 0, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' day",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' month",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '3' year",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05.321 +05:09' - INTERVAL '6' hour",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 21, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testTimestampMinusInterval()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' day",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2001, 1, 19, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' day",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 19, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' month",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2000, 10, 22, 3, 4, 5, 321, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' month",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2000, 10, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testTimeZoneGap()
    {
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '1' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 1, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '2' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 3, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 00:05' + INTERVAL '3' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 4, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2013-03-31 04:05' - INTERVAL '3' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 03:05' - INTERVAL '2' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-31 01:05' - INTERVAL '1' hour", TIMESTAMP, new SqlTimestamp(new DateTime(2013, 3, 31, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testDateToTimestampCoercing()
    {
        assertFunction("date_format(DATE '2013-10-27', '%Y-%m-%d %H:%i:%s')", VARCHAR, "2013-10-27 00:00:00");

        assertFunction("DATE '2013-10-27' = TIMESTAMP '2013-10-27 00:00:00'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' < TIMESTAMP '2013-10-27 00:00:01'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' > TIMESTAMP '2013-10-26 23:59:59'", BOOLEAN, true);
    }

    @Test
    public void testDateToTimestampWithZoneCoercing()
    {
        assertFunction("DATE '2013-10-27' = TIMESTAMP '2013-10-27 00:00:00 Europe/Berlin'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' < TIMESTAMP '2013-10-27 00:00:01 Europe/Berlin'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' > TIMESTAMP '2013-10-26 23:59:59 Europe/Berlin'", BOOLEAN, true);
    }

    @Test
    public void testTimeZoneDuplicate()
    {
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '1' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 1, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '2' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 2, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        // we need to manipulate millis directly here because 2 am has two representations in out time zone, and we need the second one
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '3' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis() + HOURS.toMillis(3), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 00:05' + INTERVAL '4' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 3, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '4' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 02:05' - INTERVAL '2' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 01:05' - INTERVAL '1' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '1' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 0, 5, 0, 0, TIME_ZONE).getMillis() + HOURS.toMillis(3), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-10-27 03:05' - INTERVAL '2' hour",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(2013, 10, 27, 2, 5, 0, 0, TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testIsDistinctFrom()
            throws Exception
    {
        assertFunction("CAST(NULL AS DATE) IS DISTINCT FROM CAST(NULL AS DATE)", BOOLEAN, false);
        assertFunction("DATE '2013-10-27' IS DISTINCT FROM TIMESTAMP '2013-10-27 00:00:00'", BOOLEAN, false);
        assertFunction("DATE '2013-10-27' IS DISTINCT FROM TIMESTAMP '2013-10-28 00:00:00'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM DATE '2013-10-27'", BOOLEAN, true);
        assertFunction("DATE '2013-10-27' IS DISTINCT FROM NULL", BOOLEAN, true);
    }

    private static SqlDate toDate(DateTime dateTime)
    {
        return new SqlDate((int) TimeUnit.MILLISECONDS.toDays(dateTime.getMillis()));
    }
}
