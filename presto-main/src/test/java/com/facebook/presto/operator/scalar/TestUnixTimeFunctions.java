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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.TimeZone;

import static com.facebook.presto.spi.Session.DEFAULT_CATALOG;
import static com.facebook.presto.spi.Session.DEFAULT_SCHEMA;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static org.joda.time.Days.daysBetween;
import static org.joda.time.Hours.hoursBetween;
import static org.joda.time.Minutes.minutesBetween;
import static org.joda.time.Months.monthsBetween;
import static org.joda.time.Seconds.secondsBetween;
import static org.joda.time.Weeks.weeksBetween;
import static org.joda.time.Years.yearsBetween;
import static org.testng.Assert.assertEquals;

public class TestUnixTimeFunctions
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Asia/Kathmandu");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    private static final TimeZoneKey WEIRD_ZONE_KEY = getTimeZoneKey("+07:09");
    private static final DateTimeZone WEIRD_ZONE = getDateTimeZone(WEIRD_ZONE_KEY);

    private static final DateTime DATE = new DateTime(2001, 8, 22, 0, 0, 0, 0, DATE_TIME_ZONE);
    private static final String DATE_LITERAL = "DATE '2001-08-22'";

    private static final DateTime TIME = new DateTime(1970, 1, 1, 3, 4, 5, 321, DATE_TIME_ZONE);
    private static final String TIME_LITERAL = "TIME '03:04:05.321'";
    private static final DateTime WEIRD_TIME = new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_ZONE);
    private static final String WEIRD_TIME_LITERAL = "TIME '03:04:05.321 +07:09'";

    private static final DateTime TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, DATE_TIME_ZONE);
    private static final String TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321'";
    private static final DateTime WEIRD_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, WEIRD_ZONE);
    private static final String WEIRD_TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321 +07:09'";

    private static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(7 * 60 + 9);
    private Session session;
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        session = new Session("user", "test", DEFAULT_CATALOG, DEFAULT_SCHEMA, TIME_ZONE_KEY, Locale.ENGLISH, null, null);
        functionAssertions = new FunctionAssertions(session);
    }

    @Test
    public void testCurrentDate()
            throws Exception
    {
        // current date is the time at midnight in the session time zone
        DateMidnight dateMidnight = new DateMidnight(session.getStartTime(), DateTimeZone.UTC).withZoneRetainFields(DATE_TIME_ZONE);
        assertFunction("CURRENT_DATE", toDate(dateMidnight.getMillis()));
    }

    @Test
    public void testLocalTime()
            throws Exception
    {
        long millis = new LocalTime(session.getStartTime(), DATE_TIME_ZONE).getMillisOfDay();
        functionAssertions.assertFunction("LOCALTIME", toTime(millis));
    }

    @Test
    public void testCurrentTime()
            throws Exception
    {
        long millis = new LocalTime(session.getStartTime(), DATE_TIME_ZONE).getMillisOfDay();
        functionAssertions.assertFunction("CURRENT_TIME", new SqlTimeWithTimeZone(millis, session.getTimeZoneKey()));
    }

    @Test
    public void testLocalTimestamp()
    {
        assertEquals(functionAssertions.selectSingleValue("localtimestamp"), toTimestamp(session.getStartTime()));
    }

    @Test
    public void testCurrentTimestamp()
    {
        assertEquals(functionAssertions.selectSingleValue("current_timestamp"), new SqlTimestampWithTimeZone(session.getStartTime(), session.getTimeZoneKey()));
        assertEquals(functionAssertions.selectSingleValue("now()"), new SqlTimestampWithTimeZone(session.getStartTime(), session.getTimeZoneKey()));
    }

    @Test
    public void testFromUnixTime()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE);
        double seconds = dateTime.getMillis() / 1000.0;
        assertFunction("from_unixtime(" + seconds + ")", toTimestamp(dateTime));

        dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 888, DATE_TIME_ZONE);
        seconds = dateTime.getMillis() / 1000.0;
        assertFunction("from_unixtime(" + seconds + ")", toTimestamp(dateTime));
    }

    @Test
    public void testToUnixTime()
    {
        assertFunction("to_unixtime(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getMillis() / 1000.0);
        assertFunction("to_unixtime(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getMillis() / 1000.0);
    }

    @Test
    public void testTimeZone()
    {
        assertFunction("hour(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getHourOfDay());
        assertFunction("minute(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getMinuteOfHour());
        assertFunction("hour(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction("minute(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getMinuteOfHour());
    }

    @Test
    public void testAtTimeZone()
    {
        assertEquals(functionAssertions.selectSingleValue("current_timestamp at time zone interval '07:09' hour to minute"),
                new SqlTimestampWithTimeZone(session.getStartTime(), WEIRD_TIME_ZONE_KEY));

        assertEquals(functionAssertions.selectSingleValue("current_timestamp at time zone 'Asia/Oral'"), new SqlTimestampWithTimeZone(session.getStartTime(), TimeZone.getTimeZone("Asia/Oral")));
        assertEquals(functionAssertions.selectSingleValue("now() at time zone 'Asia/Oral'"), new SqlTimestampWithTimeZone(session.getStartTime(), TimeZone.getTimeZone("Asia/Oral")));
        assertEquals(functionAssertions.selectSingleValue("current_timestamp at time zone '+07:09'"), new SqlTimestampWithTimeZone(session.getStartTime(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testPartFunctions()
    {
        assertFunction("second(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getSecondOfMinute());
        assertFunction("minute(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getMinuteOfHour());
        assertFunction("hour(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getHourOfDay());
        assertFunction("day_of_week(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.dayOfWeek().get());
        assertFunction("dow(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.dayOfWeek().get());
        assertFunction("day(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_month(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_year(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.dayOfYear().get());
        assertFunction("doy(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.dayOfYear().get());
        assertFunction("week(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.weekOfWeekyear().get());
        assertFunction("week_of_year(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.weekOfWeekyear().get());
        assertFunction("month(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getMonthOfYear());
        assertFunction("quarter(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + TIMESTAMP_LITERAL + ")", TIMESTAMP.getYear());

        assertFunction("second(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getSecondOfMinute());
        assertFunction("minute(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction("hour(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction("day_of_week(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.dayOfWeek().get());
        assertFunction("dow(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.dayOfWeek().get());
        assertFunction("day(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_month(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_year(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.dayOfYear().get());
        assertFunction("doy(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.dayOfYear().get());
        assertFunction("week(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.weekOfWeekyear().get());
        assertFunction("week_of_year(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.weekOfWeekyear().get());
        assertFunction("month(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getMonthOfYear());
        assertFunction("quarter(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getYear());
        assertFunction("timezone_minute(" + WEIRD_TIMESTAMP_LITERAL + ")", 9);
        assertFunction("timezone_hour(" + WEIRD_TIMESTAMP_LITERAL + ")", 7);
    }

    @Test
    public void testExtractFromTimestamp()
    {
        assertFunction("extract(second FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getSecondOfMinute());
        assertFunction("extract(minute FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getMinuteOfHour());
        assertFunction("extract(hour FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getHourOfDay());
        assertFunction("extract(day_of_week FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getDayOfWeek());
        assertFunction("extract(dow FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getDayOfWeek());
        assertFunction("extract(day FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_month FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_year FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getDayOfYear());
        assertFunction("extract(doy FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getDayOfYear());
        assertFunction("extract(week FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getWeekOfWeekyear());
        assertFunction("extract(month FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getMonthOfYear());
        assertFunction("extract(quarter FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("extract(year FROM " + TIMESTAMP_LITERAL + ")", TIMESTAMP.getYear());

        assertFunction("extract(second FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getSecondOfMinute());
        assertFunction("extract(minute FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction("extract(hour FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction("extract(day_of_week FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getDayOfWeek());
        assertFunction("extract(dow FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getDayOfWeek());
        assertFunction("extract(day FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_month FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_year FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getDayOfYear());
        assertFunction("extract(doy FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getDayOfYear());
        assertFunction("extract(week FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getWeekOfWeekyear());
        assertFunction("extract(month FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getMonthOfYear());
        assertFunction("extract(quarter FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("extract(year FROM " + WEIRD_TIMESTAMP_LITERAL + ")", WEIRD_TIMESTAMP.getYear());
        assertFunction("extract(timezone_minute FROM " + WEIRD_TIMESTAMP_LITERAL + ")", 9);
        assertFunction("extract(timezone_hour FROM " + WEIRD_TIMESTAMP_LITERAL + ")", 7);
    }

    @Test
    public void testExtractFromTime()
    {
        assertFunction("extract(second FROM " + TIME_LITERAL + ")", 5);
        assertFunction("extract(minute FROM " + TIME_LITERAL + ")", 4);
        assertFunction("extract(hour FROM " + TIME_LITERAL + ")", 3);

        assertFunction("extract(second FROM " + WEIRD_TIME_LITERAL + ")", 5);
        assertFunction("extract(minute FROM " + WEIRD_TIME_LITERAL + ")", 4);
        assertFunction("extract(hour FROM " + WEIRD_TIME_LITERAL + ")", 3);
    }

    @Test
    public void testExtractFromDate()
    {
        assertFunction("extract(day_of_week FROM " + DATE_LITERAL + ")", 3);
        assertFunction("extract(dow FROM " + DATE_LITERAL + ")", 3);
        assertFunction("extract(day FROM " + DATE_LITERAL + ")", 22);
        assertFunction("extract(day_of_month FROM " + DATE_LITERAL + ")", 22);
        assertFunction("extract(day_of_year FROM " + DATE_LITERAL + ")", 234);
        assertFunction("extract(doy FROM " + DATE_LITERAL + ")", 234);
        assertFunction("extract(week FROM " + DATE_LITERAL + ")", 34);
        assertFunction("extract(month FROM " + DATE_LITERAL + ")", 8);
        assertFunction("extract(quarter FROM " + DATE_LITERAL + ")", 3);
        assertFunction("extract(year FROM " + DATE_LITERAL + ")", 2001);
    }

    @Test
    public void testExtractFromInterval()
    {
        assertFunction("extract(second FROM INTERVAL '5' SECOND)", 5);
        assertFunction("extract(second FROM INTERVAL '65' SECOND)", 5);

        assertFunction("extract(minute FROM INTERVAL '4' MINUTE)", 4);
        assertFunction("extract(minute FROM INTERVAL '64' MINUTE)", 4);
        assertFunction("extract(minute FROM INTERVAL '247' SECOND)", 4);

        assertFunction("extract(hour FROM INTERVAL '3' HOUR)", 3);
        assertFunction("extract(hour FROM INTERVAL '27' HOUR)", 3);
        assertFunction("extract(hour FROM INTERVAL '187' MINUTE)", 3);

        assertFunction("extract(day FROM INTERVAL '2' DAY)", 2);
        assertFunction("extract(day FROM INTERVAL '55' HOUR)", 2);

        assertFunction("extract(month FROM INTERVAL '3' MONTH)", 3);
        assertFunction("extract(month FROM INTERVAL '15' MONTH)", 3);

        assertFunction("extract(year FROM INTERVAL '2' YEAR)", 2);
        assertFunction("extract(year FROM INTERVAL '29' MONTH)", 2);
    }

    @Test
    public void testTruncateTimestamp()
    {
        DateTime result = TIMESTAMP;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + TIMESTAMP_LITERAL + ")", toTimestamp(result));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + TIMESTAMP_LITERAL + ")", toTimestamp(result));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + TIMESTAMP_LITERAL + ")", toTimestamp(result));

        result = result.withHourOfDay(0);
        assertFunction("date_trunc('day', " + TIMESTAMP_LITERAL + ")", toTimestamp(result));

        result = result.withDayOfMonth(20);
        assertFunction("date_trunc('week', " + TIMESTAMP_LITERAL + ")", toTimestamp(result));

        result = result.withDayOfMonth(1);
        assertFunction("date_trunc('month', " + TIMESTAMP_LITERAL + ")", toTimestamp(result));

        result = result.withMonthOfYear(7);
        assertFunction("date_trunc('quarter', " + TIMESTAMP_LITERAL + ")", toTimestamp(result));

        result = result.withMonthOfYear(1);
        assertFunction("date_trunc('year', " + TIMESTAMP_LITERAL + ")", toTimestamp(result));

        result = WEIRD_TIMESTAMP;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(result));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(result));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(result));

        result = result.withHourOfDay(0);
        assertFunction("date_trunc('day', " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(result));

        result = result.withDayOfMonth(20);
        assertFunction("date_trunc('week', " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(result));

        result = result.withDayOfMonth(1);
        assertFunction("date_trunc('month', " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(result));

        result = result.withMonthOfYear(7);
        assertFunction("date_trunc('quarter', " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(result));

        result = result.withMonthOfYear(1);
        assertFunction("date_trunc('year', " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(result));
    }

    @Test
    public void testTruncateTime()
    {
        DateTime result = TIME;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + TIME_LITERAL + ")", toTime(result));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + TIME_LITERAL + ")", toTime(result));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + TIME_LITERAL + ")", toTime(result));

        result = WEIRD_TIME;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + WEIRD_TIME_LITERAL + ")", toTimeWithTimeZone(result));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + WEIRD_TIME_LITERAL + ")", toTimeWithTimeZone(result));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + WEIRD_TIME_LITERAL + ")", toTimeWithTimeZone(result));
    }

    @Test
    public void testTruncateDate()
    {
        DateTime result = DATE;
        assertFunction("date_trunc('day', " + DATE_LITERAL + ")", toDate(result));

        result = result.withDayOfMonth(20);
        assertFunction("date_trunc('week', " + DATE_LITERAL + ")", toDate(result));

        result = result.withDayOfMonth(1);
        assertFunction("date_trunc('month', " + DATE_LITERAL + ")", toDate(result));

        result = result.withMonthOfYear(7);
        assertFunction("date_trunc('quarter', " + DATE_LITERAL + ")", toDate(result));

        result = result.withMonthOfYear(1);
        assertFunction("date_trunc('year', " + DATE_LITERAL + ")", toDate(result));
    }

    @Test
    public void testAddFieldToTimestamp()
    {
        assertFunction("date_add('second', 3, " + TIMESTAMP_LITERAL + ")", toTimestamp(TIMESTAMP.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + TIMESTAMP_LITERAL + ")", toTimestamp(TIMESTAMP.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + TIMESTAMP_LITERAL + ")", toTimestamp(TIMESTAMP.plusHours(3)));
        assertFunction("date_add('day', 3, " + TIMESTAMP_LITERAL + ")", toTimestamp(TIMESTAMP.plusDays(3)));
        assertFunction("date_add('week', 3, " + TIMESTAMP_LITERAL + ")", toTimestamp(TIMESTAMP.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + TIMESTAMP_LITERAL + ")", toTimestamp(TIMESTAMP.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + TIMESTAMP_LITERAL + ")", toTimestamp(TIMESTAMP.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + TIMESTAMP_LITERAL + ")", toTimestamp(TIMESTAMP.plusYears(3)));

        assertFunction("date_add('second', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusHours(3)));
        assertFunction("date_add('day', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusDays(3)));
        assertFunction("date_add('week', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusYears(3)));
    }

    @Test
    public void testAddFieldToDate()
    {
        assertFunction("date_add('day', 3, " + DATE_LITERAL + ")", toDate(DATE.plusDays(3)));
        assertFunction("date_add('week', 3, " + DATE_LITERAL + ")", toDate(DATE.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + DATE_LITERAL + ")", toDate(DATE.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + DATE_LITERAL + ")", toDate(DATE.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + DATE_LITERAL + ")", toDate(DATE.plusYears(3)));
    }

    @Test
    public void testAddFieldToTime()
    {
        assertFunction("date_add('second', 3, " + TIME_LITERAL + ")", toTime(TIME.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + TIME_LITERAL + ")", toTime(TIME.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + TIME_LITERAL + ")", toTime(TIME.plusHours(3)));

        assertFunction("date_add('second', 3, " + WEIRD_TIME_LITERAL + ")", toTimeWithTimeZone(WEIRD_TIME.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + WEIRD_TIME_LITERAL + ")", toTimeWithTimeZone(WEIRD_TIME.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + WEIRD_TIME_LITERAL + ")", toTimeWithTimeZone(WEIRD_TIME.plusHours(3)));
    }

    @Test
    public void testDateDiffTimestamp()
    {
        DateTime baseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, DATE_TIME_ZONE);
        String baseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678'";

        assertFunction("date_diff('second', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", secondsBetween(baseDateTime, TIMESTAMP).getSeconds());
        assertFunction("date_diff('minute', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", minutesBetween(baseDateTime, TIMESTAMP).getMinutes());
        assertFunction("date_diff('hour', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", hoursBetween(baseDateTime, TIMESTAMP).getHours());
        assertFunction("date_diff('day', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", daysBetween(baseDateTime, TIMESTAMP).getDays());
        assertFunction("date_diff('week', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", weeksBetween(baseDateTime, TIMESTAMP).getWeeks());
        assertFunction("date_diff('month', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", monthsBetween(baseDateTime, TIMESTAMP).getMonths());
        assertFunction("date_diff('quarter', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", monthsBetween(baseDateTime, TIMESTAMP).getMonths() / 3);
        assertFunction("date_diff('year', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", yearsBetween(baseDateTime, TIMESTAMP).getYears());

        DateTime weirdBaseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, WEIRD_ZONE);
        String weirdBaseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678 +07:09'";

        assertFunction("date_diff('second', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")", secondsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getSeconds());
        assertFunction("date_diff('minute', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")", minutesBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMinutes());
        assertFunction("date_diff('hour', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")", hoursBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getHours());
        assertFunction("date_diff('day', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")", daysBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getDays());
        assertFunction("date_diff('week', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")", weeksBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getWeeks());
        assertFunction("date_diff('month', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")", monthsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMonths());
        assertFunction("date_diff('quarter', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                monthsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMonths() / 3);
        assertFunction("date_diff('year', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")", yearsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getYears());
    }

    @Test
    public void testDateDiffDate()
    {
        DateTime baseDateTime = new DateTime(1960, 5, 3, 0, 0, 0, 0, DATE_TIME_ZONE);
        String baseDateTimeLiteral = "DATE '1960-05-03'";

        assertFunction("date_diff('day', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", daysBetween(baseDateTime, DATE).getDays());
        assertFunction("date_diff('week', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", weeksBetween(baseDateTime, DATE).getWeeks());
        assertFunction("date_diff('month', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", monthsBetween(baseDateTime, DATE).getMonths());
        assertFunction("date_diff('quarter', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", monthsBetween(baseDateTime, DATE).getMonths() / 3);
        assertFunction("date_diff('year', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", yearsBetween(baseDateTime, DATE).getYears());
    }

    @Test
    public void testDateDiffTime()
    {
        DateTime baseDateTime = new DateTime(1970, 1, 1, 7, 2, 9, 678, DATE_TIME_ZONE);
        String baseDateTimeLiteral = "TIME '07:02:09.678'";

        assertFunction("date_diff('second', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", secondsBetween(baseDateTime, TIME).getSeconds());
        assertFunction("date_diff('minute', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", minutesBetween(baseDateTime, TIME).getMinutes());
        assertFunction("date_diff('hour', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", hoursBetween(baseDateTime, TIME).getHours());

        DateTime weirdBaseDateTime = new DateTime(1970, 1, 1, 7, 2, 9, 678, WEIRD_ZONE);
        String weirdBaseDateTimeLiteral = "TIME '07:02:09.678 +07:09'";

        assertFunction("date_diff('second', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", secondsBetween(weirdBaseDateTime, WEIRD_TIME).getSeconds());
        assertFunction("date_diff('minute', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", minutesBetween(weirdBaseDateTime, WEIRD_TIME).getMinutes());
        assertFunction("date_diff('hour', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", hoursBetween(weirdBaseDateTime, WEIRD_TIME).getHours());
    }

    @Test
    public void testParseDatetime()
    {
        assertFunction("parse_datetime('1960/01/22 03:04', 'YYYY/MM/DD HH:mm')", toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DATE_TIME_ZONE)));
        assertFunction("parse_datetime('1960/01/22 03:04 Asia/Oral', 'YYYY/MM/DD HH:mm ZZZZZ')",
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.forID("Asia/Oral"))));
        assertFunction("parse_datetime('1960/01/22 03:04 +0500', 'YYYY/MM/DD HH:mm Z')",
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.forOffsetHours(5))));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Both printing and parsing not supported")
    public void testInvalidDateParseFormat()
    {
        assertFunction("date_parse('%Y-%M-%d', '')", 0);
    }

    @Test
    public void testFormatDatetime()
    {
        assertFunction("format_datetime(" + TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm')", "2001/08/22 03:04");
        assertFunction("format_datetime(" + TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')", "2001/08/22 03:04 Asia/Kathmandu");
        assertFunction("format_datetime(" + WEIRD_TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm')", "2001/08/22 03:04");
        assertFunction("format_datetime(" + WEIRD_TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')", "2001/08/22 03:04 +07:09");
    }

    @Test
    public void testDateFormat()
    {
        String dateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321'";

        assertFunction("date_format(" + dateTimeLiteral + ", '%a')", "Tue");
        assertFunction("date_format(" + dateTimeLiteral + ", '%b')", "Jan");
        assertFunction("date_format(" + dateTimeLiteral + ", '%c')", "1");
        assertFunction("date_format(" + dateTimeLiteral + ", '%d')", "09");
        assertFunction("date_format(" + dateTimeLiteral + ", '%e')", "9");
        assertFunction("date_format(" + dateTimeLiteral + ", '%f')", "000321");
        assertFunction("date_format(" + dateTimeLiteral + ", '%H')", "13");
        assertFunction("date_format(" + dateTimeLiteral + ", '%h')", "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%I')", "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%i')", "04");
        assertFunction("date_format(" + dateTimeLiteral + ", '%j')", "009");
        assertFunction("date_format(" + dateTimeLiteral + ", '%k')", "13");
        assertFunction("date_format(" + dateTimeLiteral + ", '%l')", "1");
        assertFunction("date_format(" + dateTimeLiteral + ", '%M')", "January");
        assertFunction("date_format(" + dateTimeLiteral + ", '%m')", "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%p')", "PM");
        assertFunction("date_format(" + dateTimeLiteral + ", '%r')", "01:04:05 PM");
        assertFunction("date_format(" + dateTimeLiteral + ", '%S')", "05");
        assertFunction("date_format(" + dateTimeLiteral + ", '%s')", "05");
        assertFunction("date_format(" + dateTimeLiteral + ", '%T')", "13:04:05");
        assertFunction("date_format(" + dateTimeLiteral + ", '%v')", "02");
        assertFunction("date_format(" + dateTimeLiteral + ", '%W')", "Tuesday");
        assertFunction("date_format(" + dateTimeLiteral + ", '%w')", "2");
        assertFunction("date_format(" + dateTimeLiteral + ", '%Y')", "2001");
        assertFunction("date_format(" + dateTimeLiteral + ", '%y')", "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%%')", "%");
        assertFunction("date_format(" + dateTimeLiteral + ", 'foo')", "foo");
        assertFunction("date_format(" + dateTimeLiteral + ", '%g')", "g");
        assertFunction("date_format(" + dateTimeLiteral + ", '%4')", "4");

        String wierdDateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'";

        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%a')", "Tue");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%b')", "Jan");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%c')", "1");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%d')", "09");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%e')", "9");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%f')", "000321");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%H')", "13");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%h')", "01");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%I')", "01");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%i')", "04");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%j')", "009");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%k')", "13");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%l')", "1");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%M')", "January");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%m')", "01");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%p')", "PM");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%r')", "01:04:05 PM");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%S')", "05");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%s')", "05");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%T')", "13:04:05");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%v')", "02");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%W')", "Tuesday");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%w')", "2");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%Y')", "2001");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%y')", "01");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%%')", "%");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", 'foo')", "foo");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%g')", "g");
        assertFunction("date_format(" + wierdDateTimeLiteral + ", '%4')", "4");
    }

    @Test
    public void testDateParse()
    {
        assertFunction("date_parse('2013', '%Y')", toTimestamp(new DateTime(2013, 1, 1, 0, 0, 0, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05', '%Y-%m')", toTimestamp(new DateTime(2013, 5, 1, 0, 0, 0, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17', '%Y-%m-%d')", toTimestamp(new DateTime(2013, 5, 17, 0, 0, 0, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17 12:35:10', '%Y-%m-%d %h:%i:%s')", toTimestamp(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17 12:35:10 PM', '%Y-%m-%d %h:%i:%s %p')", toTimestamp(new DateTime(2013, 5, 17, 12, 35, 10, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17 12:35:10 AM', '%Y-%m-%d %h:%i:%s %p')", toTimestamp(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE)));

        assertFunction("date_parse('2013-05-17 00:35:10', '%Y-%m-%d %H:%i:%s')", toTimestamp(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('2013-05-17 23:35:10', '%Y-%m-%d %H:%i:%s')", toTimestamp(new DateTime(2013, 5, 17, 23, 35, 10, 0, DATE_TIME_ZONE)));
        assertFunction("date_parse('abc 2013-05-17 fff 23:35:10 xyz', 'abc %Y-%m-%d fff %H:%i:%s xyz')", toTimestamp(new DateTime(2013, 5, 17, 23, 35, 10, 0, DATE_TIME_ZONE)));

        assertFunction("date_parse('2013 14', '%Y %y')", toTimestamp(new DateTime(2014, 1, 1, 0, 0, 0, 0, DATE_TIME_ZONE)));
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    private SqlDate toDate(long milliseconds)
    {
        return new SqlDate(milliseconds, session.getTimeZoneKey());
    }

    private SqlDate toDate(DateTime dateDate)
    {
        return new SqlDate(dateDate.getMillis(), session.getTimeZoneKey());
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

    private SqlTimestampWithTimeZone toTimestampWithTimeZone(DateTime dateTime)
    {
        return new SqlTimestampWithTimeZone(dateTime.getMillis(), dateTime.getZone().toTimeZone());
    }
}
