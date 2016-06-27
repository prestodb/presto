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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import org.joda.time.ReadableInstant;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.scalar.DateTimeFunctions.currentDate;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeUtils.getInstantChronology;
import static org.joda.time.Days.daysBetween;
import static org.joda.time.DurationFieldType.millis;
import static org.joda.time.Hours.hoursBetween;
import static org.joda.time.Minutes.minutesBetween;
import static org.joda.time.Months.monthsBetween;
import static org.joda.time.Seconds.secondsBetween;
import static org.joda.time.Weeks.weeksBetween;
import static org.joda.time.Years.yearsBetween;
import static org.testng.Assert.assertEquals;

public class TestDateTimeFunctions
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Asia/Kathmandu");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    private static final DateTimeZone DATE_TIME_ZONE_NUMERICAL = getDateTimeZone(getTimeZoneKey("+05:45"));
    private static final TimeZoneKey WEIRD_ZONE_KEY = getTimeZoneKey("+07:09");
    private static final DateTimeZone WEIRD_ZONE = getDateTimeZone(WEIRD_ZONE_KEY);

    private static final DateTime DATE = new DateTime(2001, 8, 22, 0, 0, 0, 0, DateTimeZone.UTC);
    private static final String DATE_LITERAL = "DATE '2001-08-22'";
    private static final String DATE_ISO8601_STRING = "2001-08-22";

    private static final DateTime TIME = new DateTime(1970, 1, 1, 3, 4, 5, 321, DATE_TIME_ZONE);
    private static final String TIME_LITERAL = "TIME '03:04:05.321'";
    private static final DateTime WEIRD_TIME = new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_ZONE);
    private static final String WEIRD_TIME_LITERAL = "TIME '03:04:05.321 +07:09'";

    private static final DateTime TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, DATE_TIME_ZONE);
    private static final DateTime TIMESTAMP_WITH_NUMERICAL_ZONE = new DateTime(2001, 8, 22, 3, 4, 5, 321, DATE_TIME_ZONE_NUMERICAL);
    private static final String TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321'";
    private static final String TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321+05:45";
    private static final DateTime WEIRD_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, WEIRD_ZONE);
    private static final String WEIRD_TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321 +07:09'";
    private static final String WEIRD_TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321+07:09";

    private static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(7 * 60 + 9);
    private Session session;
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        session = testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build();
        functionAssertions = new FunctionAssertions(session);
    }

    @Test
    public void testCurrentDate()
            throws Exception
    {
        // current date is the time at midnight in the session time zone
        assertFunction("CURRENT_DATE", DateType.DATE, new SqlDate((int) epochDaysInZone(TIME_ZONE_KEY, session.getStartTime())));
    }

    @Test
    public void testCurrentDateTimezone()
            throws Exception
    {
        TimeZoneKey kievTimeZoneKey = getTimeZoneKey("Europe/Kiev");
        for (long instant = new DateTime(2000, 6, 15, 0, 0).getMillis(); instant < new DateTime(2016, 6, 15, 0, 0).getMillis(); instant += TimeUnit.HOURS.toMillis(1)) {
            assertCurrentDateAtInstant(kievTimeZoneKey, instant);
            assertCurrentDateAtInstant(TIME_ZONE_KEY, instant);
        }
    }

    private static void assertCurrentDateAtInstant(TimeZoneKey timeZoneKey, long instant)
    {
        long expectedDays = epochDaysInZone(timeZoneKey, instant);
        long dateTimeCalculation = currentDate(new TestingConnectorSession("test", timeZoneKey, US, instant, ImmutableList.of(), ImmutableMap.of()));
        assertEquals(dateTimeCalculation, expectedDays);
    }

    private static long epochDaysInZone(TimeZoneKey timeZoneKey, long instant)
    {
        return LocalDate.from(Instant.ofEpochMilli(instant).atZone(ZoneId.of(timeZoneKey.getId()))).toEpochDay();
    }

    @Test
    public void testLocalTime()
            throws Exception
    {
        long millis = new LocalTime(session.getStartTime(), DATE_TIME_ZONE).getMillisOfDay();
        functionAssertions.assertFunction("LOCALTIME", TimeType.TIME, toTime(millis));
    }

    @Test
    public void testCurrentTime()
            throws Exception
    {
        long millis = new LocalTime(session.getStartTime(), DATE_TIME_ZONE).getMillisOfDay();
        functionAssertions.assertFunction("CURRENT_TIME", TIME_WITH_TIME_ZONE, new SqlTimeWithTimeZone(millis, session.getTimeZoneKey()));
    }

    @Test
    public void testLocalTimestamp()
    {
        functionAssertions.assertFunction("localtimestamp", TimestampType.TIMESTAMP, toTimestamp(session.getStartTime()));
    }

    @Test
    public void testCurrentTimestamp()
    {
        functionAssertions.assertFunction("current_timestamp", TIMESTAMP_WITH_TIME_ZONE, new SqlTimestampWithTimeZone(session.getStartTime(), session.getTimeZoneKey()));
        functionAssertions.assertFunction("now()", TIMESTAMP_WITH_TIME_ZONE, new SqlTimestampWithTimeZone(session.getStartTime(), session.getTimeZoneKey()));
    }

    @Test
    public void testFromUnixTime()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE);
        double seconds = dateTime.getMillis() / 1000.0;
        assertFunction("from_unixtime(" + seconds + ")", TimestampType.TIMESTAMP, toTimestamp(dateTime));

        dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 888, DATE_TIME_ZONE);
        seconds = dateTime.getMillis() / 1000.0;
        assertFunction("from_unixtime(" + seconds + ")", TimestampType.TIMESTAMP, toTimestamp(dateTime));
    }

    @Test
    public void testFromUnixTimeWithOffset()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE);
        double seconds = dateTime.getMillis() / 1000.0;

        int timeZoneHoursOffset = 1;
        int timezoneMinutesOffset = 10;

        DateTime expected = new DateTime(dateTime, getDateTimeZone(getTimeZoneKeyForOffset(timeZoneHoursOffset * 60 + timezoneMinutesOffset)));
        assertFunction("from_unixtime(" + seconds + ", " + timeZoneHoursOffset + ", " + timezoneMinutesOffset + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));
    }

    @Test
    public void testToUnixTime()
    {
        assertFunction("to_unixtime(" + TIMESTAMP_LITERAL + ")", DOUBLE, TIMESTAMP.getMillis() / 1000.0);
        assertFunction("to_unixtime(" + WEIRD_TIMESTAMP_LITERAL + ")", DOUBLE, WEIRD_TIMESTAMP.getMillis() / 1000.0);
    }

    @Test
    public void testDate()
    {
        assertFunction("date('" + DATE_ISO8601_STRING + "')", DateType.DATE, toDate(DATE));
        assertFunction("date(" + WEIRD_TIMESTAMP_LITERAL + ")", DateType.DATE, toDate(DATE));
        assertFunction("date(" + TIMESTAMP_LITERAL + ")", DateType.DATE, toDate(DATE));
    }

    @Test
    public void testFromISO8601()
    {
        assertFunction("from_iso8601_timestamp('" + TIMESTAMP_ISO8601_STRING + "')", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(TIMESTAMP_WITH_NUMERICAL_ZONE));
        assertFunction("from_iso8601_timestamp('" + WEIRD_TIMESTAMP_ISO8601_STRING + "')", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP));
        assertFunction("from_iso8601_date('" + DATE_ISO8601_STRING + "')", DateType.DATE, toDate(DATE));
    }

    @Test
    public void testToISO8601()
    {
        assertFunction("to_iso8601(" + TIMESTAMP_LITERAL + ")", VARCHAR, TIMESTAMP_ISO8601_STRING);
        assertFunction("to_iso8601(" + WEIRD_TIMESTAMP_LITERAL + ")", VARCHAR, WEIRD_TIMESTAMP_ISO8601_STRING);
        assertFunction("to_iso8601(" + DATE_LITERAL + ")", VARCHAR, DATE_ISO8601_STRING);
    }

    @Test
    public void testTimeZone()
    {
        assertFunction("hour(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getHourOfDay());
        assertFunction("minute(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMinuteOfHour());
        assertFunction("hour(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction("minute(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction("current_timezone()", VARCHAR, TIME_ZONE_KEY.getId());
    }

    @Test
    public void testPartFunctions()
    {
        assertFunction("second(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getSecondOfMinute());
        assertFunction("minute(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMinuteOfHour());
        assertFunction("hour(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getHourOfDay());
        assertFunction("day_of_week(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.dayOfWeek().get());
        assertFunction("dow(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.dayOfWeek().get());
        assertFunction("day(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_month(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_year(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.dayOfYear().get());
        assertFunction("doy(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.dayOfYear().get());
        assertFunction("week(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.weekOfWeekyear().get());
        assertFunction("week_of_year(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.weekOfWeekyear().get());
        assertFunction("month(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMonthOfYear());
        assertFunction("quarter(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getYear());
        assertFunction("timezone_hour(" + TIMESTAMP_LITERAL + ")", BIGINT, 5L);
        assertFunction("timezone_hour(localtimestamp)", BIGINT, 5L);
        assertFunction("timezone_hour(current_timestamp)", BIGINT, 5L);

        assertFunction("second(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getSecondOfMinute());
        assertFunction("minute(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction("hour(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction("day_of_week(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.dayOfWeek().get());
        assertFunction("dow(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.dayOfWeek().get());
        assertFunction("day(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_month(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_year(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.dayOfYear().get());
        assertFunction("doy(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.dayOfYear().get());
        assertFunction("week(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.weekOfWeekyear().get());
        assertFunction("week_of_year(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.weekOfWeekyear().get());
        assertFunction("month(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear());
        assertFunction("quarter(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getYear());
        assertFunction("timezone_minute(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, 9L);
        assertFunction("timezone_hour(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, 7L);
    }

    @Test
    public void testYearOfWeek()
    {
        assertFunction("year_of_week(DATE '2001-08-22')", BIGINT, 2001L);
        assertFunction("yow(DATE '2001-08-22')", BIGINT, 2001L);
        assertFunction("year_of_week(DATE '2005-01-02')", BIGINT, 2004L);
        assertFunction("year_of_week(DATE '2008-12-28')", BIGINT, 2008L);
        assertFunction("year_of_week(DATE '2008-12-29')", BIGINT, 2009L);
        assertFunction("year_of_week(DATE '2009-12-31')", BIGINT, 2009L);
        assertFunction("year_of_week(DATE '2010-01-03')", BIGINT, 2009L);
        assertFunction("year_of_week(TIMESTAMP '2001-08-22 03:04:05.321 +07:09')", BIGINT, 2001L);
        assertFunction("year_of_week(TIMESTAMP '2010-01-03 03:04:05.321')", BIGINT, 2009L);
    }

    @Test
    public void testExtractFromTimestamp()
    {
        assertFunction("extract(second FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getSecondOfMinute());
        assertFunction("extract(minute FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMinuteOfHour());
        assertFunction("extract(hour FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getHourOfDay());
        assertFunction("extract(day_of_week FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfWeek());
        assertFunction("extract(dow FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfWeek());
        assertFunction("extract(day FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_month FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_year FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfYear());
        assertFunction("extract(year_of_week FROM " + TIMESTAMP_LITERAL + ")", BIGINT, 2001L);
        assertFunction("extract(doy FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfYear());
        assertFunction("extract(week FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getWeekOfWeekyear());
        assertFunction("extract(month FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMonthOfYear());
        assertFunction("extract(quarter FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("extract(year FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getYear());

        assertFunction("extract(second FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getSecondOfMinute());
        assertFunction("extract(minute FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction("extract(hour FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction("extract(day_of_week FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfWeek());
        assertFunction("extract(dow FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfWeek());
        assertFunction("extract(day FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_month FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_year FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfYear());
        assertFunction("extract(doy FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfYear());
        assertFunction("extract(week FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getWeekOfWeekyear());
        assertFunction("extract(month FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear());
        assertFunction("extract(quarter FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("extract(year FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getYear());
        assertFunction("extract(timezone_minute FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, 9L);
        assertFunction("extract(timezone_hour FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, 7L);
    }

    @Test
    public void testExtractFromTime()
    {
        assertFunction("extract(second FROM " + TIME_LITERAL + ")", BIGINT, 5L);
        assertFunction("extract(minute FROM " + TIME_LITERAL + ")", BIGINT, 4L);
        assertFunction("extract(hour FROM " + TIME_LITERAL + ")", BIGINT, 3L);

        assertFunction("extract(second FROM " + WEIRD_TIME_LITERAL + ")", BIGINT, 5L);
        assertFunction("extract(minute FROM " + WEIRD_TIME_LITERAL + ")", BIGINT, 4L);
        assertFunction("extract(hour FROM " + WEIRD_TIME_LITERAL + ")", BIGINT, 3L);
    }

    @Test
    public void testExtractFromDate()
    {
        assertFunction("extract(day_of_week FROM " + DATE_LITERAL + ")", BIGINT, 3L);
        assertFunction("extract(dow FROM " + DATE_LITERAL + ")", BIGINT, 3L);
        assertFunction("extract(day FROM " + DATE_LITERAL + ")", BIGINT, 22L);
        assertFunction("extract(day_of_month FROM " + DATE_LITERAL + ")", BIGINT, 22L);
        assertFunction("extract(day_of_year FROM " + DATE_LITERAL + ")", BIGINT, 234L);
        assertFunction("extract(doy FROM " + DATE_LITERAL + ")", BIGINT, 234L);
        assertFunction("extract(year_of_week FROM " + DATE_LITERAL + ")", BIGINT, 2001L);
        assertFunction("extract(yow FROM " + DATE_LITERAL + ")", BIGINT, 2001L);
        assertFunction("extract(week FROM " + DATE_LITERAL + ")", BIGINT, 34L);
        assertFunction("extract(month FROM " + DATE_LITERAL + ")", BIGINT, 8L);
        assertFunction("extract(quarter FROM " + DATE_LITERAL + ")", BIGINT, 3L);
        assertFunction("extract(year FROM " + DATE_LITERAL + ")", BIGINT, 2001L);

        assertFunction("extract(quarter FROM DATE '2001-01-01')", BIGINT, 1L);
        assertFunction("extract(quarter FROM DATE '2001-03-31')", BIGINT, 1L);
        assertFunction("extract(quarter FROM DATE '2001-04-01')", BIGINT, 2L);
        assertFunction("extract(quarter FROM DATE '2001-06-30')", BIGINT, 2L);
        assertFunction("extract(quarter FROM DATE '2001-07-01')", BIGINT, 3L);
        assertFunction("extract(quarter FROM DATE '2001-09-30')", BIGINT, 3L);
        assertFunction("extract(quarter FROM DATE '2001-10-01')", BIGINT, 4L);
        assertFunction("extract(quarter FROM DATE '2001-12-31')", BIGINT, 4L);

        assertFunction("extract(quarter FROM TIMESTAMP '2001-01-01 00:00:00.000')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-03-31 23:59:59.999')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-04-01 00:00:00.000')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-06-30 23:59:59.999')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-07-01 00:00:00.000')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-09-30 23:59:59.999')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-10-01 00:00:00.000')", BIGINT, 4L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-12-31 23:59:59.999')", BIGINT, 4L);

        assertFunction("extract(quarter FROM TIMESTAMP '2001-01-01 00:00:00.000 +06:00')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-03-31 23:59:59.999 +06:00')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-04-01 00:00:00.000 +06:00')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-06-30 23:59:59.999 +06:00')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-07-01 00:00:00.000 +06:00')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-09-30 23:59:59.999 +06:00')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-10-01 00:00:00.000 +06:00')", BIGINT, 4L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-12-31 23:59:59.999 +06:00')", BIGINT, 4L);
    }

    @Test
    public void testExtractFromInterval()
    {
        assertFunction("extract(second FROM INTERVAL '5' SECOND)", BIGINT, 5L);
        assertFunction("extract(second FROM INTERVAL '65' SECOND)", BIGINT, 5L);

        assertFunction("extract(minute FROM INTERVAL '4' MINUTE)", BIGINT, 4L);
        assertFunction("extract(minute FROM INTERVAL '64' MINUTE)", BIGINT, 4L);
        assertFunction("extract(minute FROM INTERVAL '247' SECOND)", BIGINT, 4L);

        assertFunction("extract(hour FROM INTERVAL '3' HOUR)", BIGINT, 3L);
        assertFunction("extract(hour FROM INTERVAL '27' HOUR)", BIGINT, 3L);
        assertFunction("extract(hour FROM INTERVAL '187' MINUTE)", BIGINT, 3L);

        assertFunction("extract(day FROM INTERVAL '2' DAY)", BIGINT, 2L);
        assertFunction("extract(day FROM INTERVAL '55' HOUR)", BIGINT, 2L);

        assertFunction("extract(month FROM INTERVAL '3' MONTH)", BIGINT, 3L);
        assertFunction("extract(month FROM INTERVAL '15' MONTH)", BIGINT, 3L);

        assertFunction("extract(year FROM INTERVAL '2' YEAR)", BIGINT, 2L);
        assertFunction("extract(year FROM INTERVAL '29' MONTH)", BIGINT, 2L);
    }

    @Test
    public void testTruncateTimestamp()
    {
        DateTime result = TIMESTAMP;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(result));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(result));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(result));

        result = result.withHourOfDay(0);
        assertFunction("date_trunc('day', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(result));

        result = result.withDayOfMonth(20);
        assertFunction("date_trunc('week', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(result));

        result = result.withDayOfMonth(1);
        assertFunction("date_trunc('month', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(result));

        result = result.withMonthOfYear(7);
        assertFunction("date_trunc('quarter', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(result));

        result = result.withMonthOfYear(1);
        assertFunction("date_trunc('year', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(result));

        result = WEIRD_TIMESTAMP;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withHourOfDay(0);
        assertFunction("date_trunc('day', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withDayOfMonth(20);
        assertFunction("date_trunc('week', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withDayOfMonth(1);
        assertFunction("date_trunc('month', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withMonthOfYear(7);
        assertFunction("date_trunc('quarter', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withMonthOfYear(1);
        assertFunction("date_trunc('year', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));
    }

    @Test
    public void testTruncateTime()
    {
        DateTime result = TIME;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + TIME_LITERAL + ")", TimeType.TIME, toTime(result));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + TIME_LITERAL + ")", TimeType.TIME, toTime(result));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + TIME_LITERAL + ")", TimeType.TIME, toTime(result));

        result = WEIRD_TIME;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(result));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(result));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(result));
    }

    @Test
    public void testTruncateDate()
    {
        DateTime result = DATE;
        assertFunction("date_trunc('day', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));

        result = result.withDayOfMonth(20);
        assertFunction("date_trunc('week', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));

        result = result.withDayOfMonth(1);
        assertFunction("date_trunc('month', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));

        result = result.withMonthOfYear(7);
        assertFunction("date_trunc('quarter', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));

        result = result.withMonthOfYear(1);
        assertFunction("date_trunc('year', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));
    }

    @Test
    public void testAddFieldToTimestamp()
    {
        assertFunction("date_add('millisecond', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(TIMESTAMP.plusMillis(3)));
        assertFunction("date_add('second', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(TIMESTAMP.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(TIMESTAMP.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(TIMESTAMP.plusHours(3)));
        assertFunction("date_add('day', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(TIMESTAMP.plusDays(3)));
        assertFunction("date_add('week', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(TIMESTAMP.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(TIMESTAMP.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(TIMESTAMP.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, toTimestamp(TIMESTAMP.plusYears(3)));

        assertFunction("date_add('millisecond', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMillis(3)));
        assertFunction("date_add('second', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusHours(3)));
        assertFunction("date_add('day', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusDays(3)));
        assertFunction("date_add('week', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusYears(3)));
    }

    @Test
    public void testAddFieldToDate()
    {
        assertFunction("date_add('day', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusDays(3)));
        assertFunction("date_add('week', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusYears(3)));
    }

    @Test
    public void testAddFieldToTime()
    {
        assertFunction("date_add('millisecond', 3, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.plusMillis(3)));
        assertFunction("date_add('second', 3, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.plusHours(3)));

        assertFunction("date_add('millisecond', 3, " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusMillis(3)));
        assertFunction("date_add('second', 3, " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusHours(3)));
    }

    @Test
    public void testDateDiffTimestamp()
    {
        DateTime baseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, DATE_TIME_ZONE);
        String baseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678'";

        assertFunction("date_diff('millisecond', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, millisBetween(baseDateTime, TIMESTAMP));
        assertFunction("date_diff('second', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) secondsBetween(baseDateTime, TIMESTAMP).getSeconds());
        assertFunction("date_diff('minute', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) minutesBetween(baseDateTime, TIMESTAMP).getMinutes());
        assertFunction("date_diff('hour', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) hoursBetween(baseDateTime, TIMESTAMP).getHours());
        assertFunction("date_diff('day', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) daysBetween(baseDateTime, TIMESTAMP).getDays());
        assertFunction("date_diff('week', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) weeksBetween(baseDateTime, TIMESTAMP).getWeeks());
        assertFunction("date_diff('month', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) monthsBetween(baseDateTime, TIMESTAMP).getMonths());
        assertFunction("date_diff('quarter', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) monthsBetween(baseDateTime, TIMESTAMP).getMonths() / 3);
        assertFunction("date_diff('year', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) yearsBetween(baseDateTime, TIMESTAMP).getYears());

        DateTime weirdBaseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, WEIRD_ZONE);
        String weirdBaseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678 +07:09'";

        assertFunction("date_diff('millisecond', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                millisBetween(weirdBaseDateTime, WEIRD_TIMESTAMP));
        assertFunction("date_diff('second', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) secondsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getSeconds());
        assertFunction("date_diff('minute', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) minutesBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMinutes());
        assertFunction("date_diff('hour', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) hoursBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getHours());
        assertFunction("date_diff('day', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) daysBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getDays());
        assertFunction("date_diff('week', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) weeksBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getWeeks());
        assertFunction("date_diff('month', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) monthsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMonths());
        assertFunction("date_diff('quarter', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) monthsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMonths() / 3);
        assertFunction("date_diff('year', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) yearsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getYears());
    }

    @Test
    public void testDateDiffDate()
    {
        DateTime baseDateTime = new DateTime(1960, 5, 3, 0, 0, 0, 0, DateTimeZone.UTC);
        String baseDateTimeLiteral = "DATE '1960-05-03'";

        assertFunction("date_diff('day', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) daysBetween(baseDateTime, DATE).getDays());
        assertFunction("date_diff('week', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) weeksBetween(baseDateTime, DATE).getWeeks());
        assertFunction("date_diff('month', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) monthsBetween(baseDateTime, DATE).getMonths());
        assertFunction("date_diff('quarter', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) monthsBetween(baseDateTime, DATE).getMonths() / 3);
        assertFunction("date_diff('year', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) yearsBetween(baseDateTime, DATE).getYears());
    }

    @Test
    public void testDateDiffTime()
    {
        DateTime baseDateTime = new DateTime(1970, 1, 1, 7, 2, 9, 678, DATE_TIME_ZONE);
        String baseDateTimeLiteral = "TIME '07:02:09.678'";

        assertFunction("date_diff('millisecond', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", BIGINT, millisBetween(baseDateTime, TIME));
        assertFunction("date_diff('second', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", BIGINT, (long) secondsBetween(baseDateTime, TIME).getSeconds());
        assertFunction("date_diff('minute', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", BIGINT, (long) minutesBetween(baseDateTime, TIME).getMinutes());
        assertFunction("date_diff('hour', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", BIGINT, (long) hoursBetween(baseDateTime, TIME).getHours());

        DateTime weirdBaseDateTime = new DateTime(1970, 1, 1, 7, 2, 9, 678, WEIRD_ZONE);
        String weirdBaseDateTimeLiteral = "TIME '07:02:09.678 +07:09'";

        assertFunction("date_diff('millisecond', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", BIGINT, millisBetween(weirdBaseDateTime, WEIRD_TIME));
        assertFunction("date_diff('second', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", BIGINT, (long) secondsBetween(weirdBaseDateTime, WEIRD_TIME).getSeconds());
        assertFunction("date_diff('minute', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", BIGINT, (long) minutesBetween(weirdBaseDateTime, WEIRD_TIME).getMinutes());
        assertFunction("date_diff('hour', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", BIGINT, (long) hoursBetween(weirdBaseDateTime, WEIRD_TIME).getHours());
    }

    @Test
    public void testParseDatetime()
    {
        assertFunction("parse_datetime('1960/01/22 03:04', 'YYYY/MM/DD HH:mm')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DATE_TIME_ZONE)));
        assertFunction("parse_datetime('1960/01/22 03:04 Asia/Oral', 'YYYY/MM/DD HH:mm ZZZZZ')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.forID("Asia/Oral"))));
        assertFunction("parse_datetime('1960/01/22 03:04 +0500', 'YYYY/MM/DD HH:mm Z')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.forOffsetHours(5))));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Both printing and parsing not supported")
    public void testInvalidDateParseFormat()
    {
        assertFunction("date_parse('%Y-%M-%d', '')", BIGINT, 0);
    }

    @Test
    public void testFormatDatetime()
    {
        assertFunction("format_datetime(" + TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm')", VARCHAR, "2001/08/22 03:04");
        assertFunction("format_datetime(" + TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')", VARCHAR, "2001/08/22 03:04 Asia/Kathmandu");
        assertFunction("format_datetime(" + WEIRD_TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm')", VARCHAR, "2001/08/22 03:04");
        assertFunction("format_datetime(" + WEIRD_TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')", VARCHAR, "2001/08/22 03:04 +07:09");
    }

    @Test
    public void testDateFormat()
    {
        String dateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321'";

        assertFunction("date_format(" + dateTimeLiteral + ", '%a')", VARCHAR, "Tue");
        assertFunction("date_format(" + dateTimeLiteral + ", '%b')", VARCHAR, "Jan");
        assertFunction("date_format(" + dateTimeLiteral + ", '%c')", VARCHAR, "1");
        assertFunction("date_format(" + dateTimeLiteral + ", '%d')", VARCHAR, "09");
        assertFunction("date_format(" + dateTimeLiteral + ", '%e')", VARCHAR, "9");
        assertFunction("date_format(" + dateTimeLiteral + ", '%f')", VARCHAR, "321000");
        assertFunction("date_format(" + dateTimeLiteral + ", '%H')", VARCHAR, "13");
        assertFunction("date_format(" + dateTimeLiteral + ", '%h')", VARCHAR, "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%I')", VARCHAR, "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%i')", VARCHAR, "04");
        assertFunction("date_format(" + dateTimeLiteral + ", '%j')", VARCHAR, "009");
        assertFunction("date_format(" + dateTimeLiteral + ", '%k')", VARCHAR, "13");
        assertFunction("date_format(" + dateTimeLiteral + ", '%l')", VARCHAR, "1");
        assertFunction("date_format(" + dateTimeLiteral + ", '%M')", VARCHAR, "January");
        assertFunction("date_format(" + dateTimeLiteral + ", '%m')", VARCHAR, "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%p')", VARCHAR, "PM");
        assertFunction("date_format(" + dateTimeLiteral + ", '%r')", VARCHAR, "01:04:05 PM");
        assertFunction("date_format(" + dateTimeLiteral + ", '%S')", VARCHAR, "05");
        assertFunction("date_format(" + dateTimeLiteral + ", '%s')", VARCHAR, "05");
        assertFunction("date_format(" + dateTimeLiteral + ", '%T')", VARCHAR, "13:04:05");
        assertFunction("date_format(" + dateTimeLiteral + ", '%v')", VARCHAR, "02");
        assertFunction("date_format(" + dateTimeLiteral + ", '%W')", VARCHAR, "Tuesday");
        assertFunction("date_format(" + dateTimeLiteral + ", '%w')", VARCHAR, "2");
        assertFunction("date_format(" + dateTimeLiteral + ", '%Y')", VARCHAR, "2001");
        assertFunction("date_format(" + dateTimeLiteral + ", '%y')", VARCHAR, "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%%')", VARCHAR, "%");
        assertFunction("date_format(" + dateTimeLiteral + ", 'foo')", VARCHAR, "foo");
        assertFunction("date_format(" + dateTimeLiteral + ", '%g')", VARCHAR, "g");
        assertFunction("date_format(" + dateTimeLiteral + ", '%4')", VARCHAR, "4");
        assertFunction("date_format(" + dateTimeLiteral + ", '%x %v')", VARCHAR, "2001 02");

        String wierdDateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'";

        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%a')", VARCHAR, "Tue");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%b')", VARCHAR, "Jan");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%c')", VARCHAR, "1");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%d')", VARCHAR, "09");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%e')", VARCHAR, "9");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%f')", VARCHAR, "321000");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%H')", VARCHAR, "13");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%h')", VARCHAR, "01");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%I')", VARCHAR, "01");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%i')", VARCHAR, "04");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%j')", VARCHAR, "009");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%k')", VARCHAR, "13");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%l')", VARCHAR, "1");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%M')", VARCHAR, "January");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%m')", VARCHAR, "01");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%p')", VARCHAR, "PM");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%r')", VARCHAR, "01:04:05 PM");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%S')", VARCHAR, "05");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%s')", VARCHAR, "05");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%T')", VARCHAR, "13:04:05");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%v')", VARCHAR, "02");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%W')", VARCHAR, "Tuesday");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%w')", VARCHAR, "2");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%Y')", VARCHAR, "2001");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%y')", VARCHAR, "01");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%%')", VARCHAR, "%");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", 'foo')", VARCHAR, "foo");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%g')", VARCHAR, "g");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%4')", VARCHAR, "4");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%x %v')", VARCHAR, "2001 02");

        assertFunction("date_format(TIMESTAMP '2001-01-09 13:04:05.32', '%f')", VARCHAR, "320000");
        assertFunction("date_format(TIMESTAMP '2001-01-09 00:04:05.32', '%k')", VARCHAR, "0");
    }

    @Test
    public void testDateParse()
    {
        assertFunction("date_parse('2013', '%Y')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 1, 1, 0, 0, 0, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05', '%Y-%m')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 1, 0, 0, 0, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17', '%Y-%m-%d')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 17, 0, 0, 0, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17 12:35:10', '%Y-%m-%d %h:%i:%s')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17 12:35:10 PM', '%Y-%m-%d %h:%i:%s %p')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 17, 12, 35, 10, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17 12:35:10 AM', '%Y-%m-%d %h:%i:%s %p')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE)));

        assertFunction("date_parse('2013-05-17 00:35:10', '%Y-%m-%d %H:%i:%s')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17 23:35:10', '%Y-%m-%d %H:%i:%s')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 17, 23, 35, 10, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('abc 2013-05-17 fff 23:35:10 xyz', 'abc %Y-%m-%d fff %H:%i:%s xyz')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 17, 23, 35, 10, 0, DATE_TIME_ZONE)));

        assertFunction("date_parse('2013 14', '%Y %y')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2014, 1, 1, 0, 0, 0, 0, DATE_TIME_ZONE)));

        assertFunction("date_parse('1998 53', '%x %v')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(1998, 12, 28, 0, 0, 0, 0, DATE_TIME_ZONE)));

        assertFunction("date_parse('1.1', '%s.%f')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(1970, 1, 1, 0, 0, 1, 100, DATE_TIME_ZONE)));
        assertFunction("date_parse('1.01', '%s.%f')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(1970, 1, 1, 0, 0, 1, 10, DATE_TIME_ZONE)));
        assertFunction("date_parse('1.2006', '%s.%f')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(1970, 1, 1, 0, 0, 1, 200, DATE_TIME_ZONE)));
        assertFunction("date_parse('59.123456789', '%s.%f')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(1970, 1, 1, 0, 0, 59, 123, DATE_TIME_ZONE)));

        assertFunction("date_parse('0', '%k')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(1970, 1, 1, 0, 0, 0, 0, DATE_TIME_ZONE)));

        assertFunction("date_parse('28-JAN-16 11.45.46.421000 PM','%d-%b-%y %l.%i.%s.%f %p')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2016, 1, 28, 23, 45, 46, 421, DATE_TIME_ZONE)));
        assertFunction("date_parse('11-DEC-70 11.12.13.456000 AM','%d-%b-%y %l.%i.%s.%f %p')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(1970, 12, 11, 11, 12, 13, 456, DATE_TIME_ZONE)));
        assertFunction("date_parse('31-MAY-69 04.59.59.999000 AM','%d-%b-%y %l.%i.%s.%f %p')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2069, 5, 31, 4, 59, 59, 999, DATE_TIME_ZONE)));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Invalid format: \"3.0123456789\" is malformed at \"9\"")
    public void testTooManyFractionsInSeconds()
    {
        assertFunction("date_parse('3.0123456789', '%s.%f')",
                TimestampType.TIMESTAMP,
                null);
    }

    @Test
    public void testLocale()
    {
        Locale locale = Locale.JAPANESE;
        Session localeSession = testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .setLocale(locale)
                .build();

        FunctionAssertions localeAssertions = new FunctionAssertions(localeSession);

        String dateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321'";

        localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%a')", VARCHAR, "火");
        localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%W')", VARCHAR, "火曜日");
        localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%p')", VARCHAR, "午後");
        localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%r')", VARCHAR, "01:04:05 午後");
        localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%b')", VARCHAR, "1");
        localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%M')", VARCHAR, "1月");

        localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'EEE')", VARCHAR, "火");
        localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'EEEE')", VARCHAR, "火曜日");
        localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'a')", VARCHAR, "午後");
        localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'MMM')", VARCHAR, "1");
        localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'MMMM')", VARCHAR, "1月");

        localeAssertions.assertFunction("date_parse('2013-05-17 12:35:10 午後', '%Y-%m-%d %h:%i:%s %p')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 17, 12, 35, 10, 0, DATE_TIME_ZONE), localeSession));
        localeAssertions.assertFunction("date_parse('2013-05-17 12:35:10 午前', '%Y-%m-%d %h:%i:%s %p')",
                TimestampType.TIMESTAMP,
                toTimestamp(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE), localeSession));

        localeAssertions.assertFunction("parse_datetime('2013-05-17 12:35:10 午後', 'yyyy-MM-dd hh:mm:ss a')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(2013, 5, 17, 12, 35, 10, 0, DATE_TIME_ZONE)));
        localeAssertions.assertFunction("parse_datetime('2013-05-17 12:35:10 午前', 'yyyy-MM-dd hh:mm:ss aaa')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE)));
    }

    @Test
    public void testDateTimeOutputString()
    {
        // SqlDate
        assertFunctionString("date '2012-12-31'", DateType.DATE, "2012-12-31");
        assertFunctionString("date '0000-12-31'", DateType.DATE, "0000-12-31");
        assertFunctionString("date '0000-09-23'", DateType.DATE, "0000-09-23");
        assertFunctionString("date '0001-10-25'", DateType.DATE, "0001-10-25");
        assertFunctionString("date '1560-04-29'", DateType.DATE, "1560-04-29");

        // SqlTime
        assertFunctionString("time '00:00:00'", TimeType.TIME, "00:00:00.000");
        assertFunctionString("time '01:02:03'", TimeType.TIME, "01:02:03.000");
        assertFunctionString("time '23:23:23.233'", TimeType.TIME, "23:23:23.233");
        assertFunctionString("time '23:59:59.999'", TimeType.TIME, "23:59:59.999");

        // SqlTimeWithTimeZone
        assertFunctionString("time '00:00:00 UTC'", TIME_WITH_TIME_ZONE, "00:00:00.000 UTC");
        assertFunctionString("time '01:02:03 Asia/Shanghai'", TIME_WITH_TIME_ZONE, "01:02:03.000 Asia/Shanghai");
        assertFunctionString("time '23:23:23.233 America/Los_Angeles'", TIME_WITH_TIME_ZONE, "23:23:23.233 America/Los_Angeles");
        assertFunctionString(WEIRD_TIME_LITERAL, TIME_WITH_TIME_ZONE, "03:04:05.321 +07:09");

        // SqlTimestamp
        assertFunctionString("timestamp '0000-01-02 01:02:03'", TimestampType.TIMESTAMP, "0000-01-02 01:02:03.000");
        assertFunctionString("timestamp '2012-12-31 00:00:00'", TimestampType.TIMESTAMP, "2012-12-31 00:00:00.000");
        assertFunctionString("timestamp '1234-05-06 23:23:23.233'", TimestampType.TIMESTAMP, "1234-05-06 23:23:23.233");
        assertFunctionString("timestamp '2333-02-23 23:59:59.999'", TimestampType.TIMESTAMP, "2333-02-23 23:59:59.999");

        // SqlTimestampWithTimeZone
        assertFunctionString("timestamp '2012-12-31 00:00:00 UTC'", TIMESTAMP_WITH_TIME_ZONE, "2012-12-31 00:00:00.000 UTC");
        assertFunctionString("timestamp '0000-01-02 01:02:03 Asia/Shanghai'", TIMESTAMP_WITH_TIME_ZONE, "0000-01-02 01:02:03.000 Asia/Shanghai");
        assertFunctionString("timestamp '1234-05-06 23:23:23.233 America/Los_Angeles'", TIMESTAMP_WITH_TIME_ZONE, "1234-05-06 23:23:23.233 America/Los_Angeles");
        assertFunctionString("timestamp '2333-02-23 23:59:59.999 Asia/Tokyo'", TIMESTAMP_WITH_TIME_ZONE, "2333-02-23 23:59:59.999 Asia/Tokyo");
    }

    private void assertFunction(String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }

    private void assertFunctionString(String projection, Type expectedType, String expected)
    {
        functionAssertions.assertFunctionString(projection, expectedType, expected);
    }

    private SqlDate toDate(DateTime dateDate)
    {
        long millis = dateDate.getMillis();
        return new SqlDate((int) TimeUnit.MILLISECONDS.toDays(millis));
    }

    private static long millisBetween(ReadableInstant start, ReadableInstant end)
    {
        requireNonNull(start, "start is null");
        requireNonNull(end, "end is null");
        return millis().getField(getInstantChronology(start)).getDifferenceAsLong(end.getMillis(), start.getMillis());
    }

    private SqlTime toTime(long milliseconds)
    {
        return new SqlTime(milliseconds, session.getTimeZoneKey());
    }

    private SqlTime toTime(DateTime dateTime)
    {
        return new SqlTime(dateTime.getMillis(), session.getTimeZoneKey());
    }

    private SqlTimeWithTimeZone toTimeWithTimeZone(DateTime dateTime)
    {
        return new SqlTimeWithTimeZone(dateTime.getMillis(), dateTime.getZone().toTimeZone());
    }

    private SqlTimestamp toTimestamp(long milliseconds)
    {
        return new SqlTimestamp(milliseconds, session.getTimeZoneKey());
    }

    private SqlTimestamp toTimestamp(DateTime dateTime)
    {
        return new SqlTimestamp(dateTime.getMillis(), session.getTimeZoneKey());
    }

    private SqlTimestamp toTimestamp(DateTime dateTime, Session session)
    {
        return new SqlTimestamp(dateTime.getMillis(), session.getTimeZoneKey());
    }

    private SqlTimestampWithTimeZone toTimestampWithTimeZone(DateTime dateTime)
    {
        return new SqlTimestampWithTimeZone(dateTime.getMillis(), dateTime.getZone().toTimeZone());
    }
}
