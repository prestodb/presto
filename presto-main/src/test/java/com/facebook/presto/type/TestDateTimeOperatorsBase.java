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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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
import static com.facebook.presto.testing.TestingSqlTime.sqlTimeOf;
import static com.facebook.presto.testing.TestingSqlTime.sqlTimestampOf;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.fail;

public abstract class TestDateTimeOperatorsBase
{
    protected static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    protected static final DateTimeZone TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    protected static final DateTimeZone WEIRD_TIME_ZONE = DateTimeZone.forOffsetHoursMinutes(5, 9);
    protected static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(5 * 60 + 9);

    private final FunctionAssertions functionAssertions;

    protected TestDateTimeOperatorsBase(Session session)
    {
        functionAssertions = new FunctionAssertions(session);
    }

    protected void assertFunction(String projection, Type expectedType, Object expected)
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
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' hour", TIME, sqlTimeOf(6, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("INTERVAL '3' hour + TIME '03:04:05.321'", TIME, sqlTimeOf(6, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' day", TIME, sqlTimeOf(3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("INTERVAL '3' day + TIME '03:04:05.321'", TIME, sqlTimeOf(3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' month", TIME, sqlTimeOf(3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("INTERVAL '3' month + TIME '03:04:05.321'", TIME, sqlTimeOf(3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIME '03:04:05.321' + INTERVAL '3' year", TIME, sqlTimeOf(3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("INTERVAL '3' year + TIME '03:04:05.321'", TIME, sqlTimeOf(3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));

        assertFunction("TIME '03:04:05.321' + INTERVAL '27' hour", TIME, sqlTimeOf(6, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("INTERVAL '27' hour + TIME '03:04:05.321'", TIME, sqlTimeOf(6, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));

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
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' hour",  TIMESTAMP, sqlTimestampOf(2001, 1, 22, 6, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("INTERVAL '3' hour + TIMESTAMP '2001-1-22 03:04:05.321'",  TIMESTAMP, sqlTimestampOf(2001, 1, 22, 6, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' day",   TIMESTAMP, sqlTimestampOf(2001, 1, 25, 3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("INTERVAL '3' day + TIMESTAMP '2001-1-22 03:04:05.321'",   TIMESTAMP, sqlTimestampOf(2001, 1, 25, 3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' month", TIMESTAMP, sqlTimestampOf(2001, 4, 22, 3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("INTERVAL '3' month + TIMESTAMP '2001-1-22 03:04:05.321'", TIMESTAMP, sqlTimestampOf(2001, 4, 22, 3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' + INTERVAL '3' year",  TIMESTAMP, sqlTimestampOf(2004, 1, 22, 3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("INTERVAL '3' year + TIMESTAMP '2001-1-22 03:04:05.321'",  TIMESTAMP, sqlTimestampOf(2004, 1, 22, 3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));

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
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' hour", TIME, sqlTimeOf(0, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' day", TIME, sqlTimeOf(3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' month", TIME, sqlTimeOf(3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIME '03:04:05.321' - INTERVAL '3' year", TIME, sqlTimeOf(3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));

        assertFunction("TIME '03:04:05.321' - INTERVAL '6' hour", TIME, sqlTimeOf(21, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));

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

    private ConnectorSession getConnectorSession()
    {
        return functionAssertions.getSession().toConnectorSession();
    }

    @Test
    public void testTimestampMinusInterval()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' day", TIMESTAMP, sqlTimestampOf(2001, 1, 19, 3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' day",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 19, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' - INTERVAL '3' month", TIMESTAMP, sqlTimestampOf(2000, 10, 22, 3, 4, 5, 321, TIME_ZONE, TIME_ZONE_KEY, getConnectorSession()));
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +05:09' - INTERVAL '3' month",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2000, 10, 22, 3, 4, 5, 321, WEIRD_TIME_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
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
