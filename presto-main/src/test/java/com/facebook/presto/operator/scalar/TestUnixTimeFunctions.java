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
import com.facebook.presto.spi.type.TimeWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimestampWithTimeZone;
import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalTime;
import org.joda.time.chrono.ISOChronology;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
    private static final DateTime DATE_TIME = new DateTime(2001, 1, 22, 3, 4, 5, 321, DATE_TIME_ZONE);
    private static final String DATE_TIME_LITERAL = "TIMESTAMP '2001-01-22 03:04:05.321'";

    private static final TimeZoneKey WEIRD_ZONE_KEY = getTimeZoneKey("+07:09");
    private static final DateTimeZone WEIRD_ZONE = getDateTimeZone(WEIRD_ZONE_KEY);
    private static final DateTime WEIRD_DATE_TIME = new DateTime(2001, 1, 22, 3, 4, 5, 321, WEIRD_ZONE);
    private static final String WEIRD_DATE_TIME_LITERAL = "TIMESTAMP '2001-01-22 03:04:05.321 +07:09'";

    private static final DateTimeField CENTURY_FIELD = ISOChronology.getInstance(DATE_TIME_ZONE).centuryOfEra();
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
        assertFunction("CURRENT_DATE", new Date(dateMidnight.getMillis()));
    }

    @Test
    public void testLocalTime()
            throws Exception
    {
        long millis = new LocalTime(session.getStartTime(), DATE_TIME_ZONE).getMillisOfDay();
        functionAssertions.assertFunction("LOCALTIME", new Time(millis));
    }

    @Test
    public void testCurrentTime()
            throws Exception
    {
        long millis = new LocalTime(session.getStartTime(), DATE_TIME_ZONE).getMillisOfDay();
        functionAssertions.assertFunction("CURRENT_TIME", new TimeWithTimeZone(millis, session.getTimeZoneKey()));
    }

    @Test
    public void testLocalTimestamp()
    {
        assertEquals(functionAssertions.selectSingleValue("localtimestamp"), toTimestamp(session.getStartTime()));
    }

    @Test
    public void testCurrentTimestamp()
    {
        assertEquals(functionAssertions.selectSingleValue("current_timestamp"), new TimestampWithTimeZone(session.getStartTime(), session.getTimeZoneKey()));
        assertEquals(functionAssertions.selectSingleValue("now()"), new TimestampWithTimeZone(session.getStartTime(), session.getTimeZoneKey()));
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
        assertFunction("to_unixtime(" + DATE_TIME_LITERAL + ")", DATE_TIME.getMillis() / 1000.0);
        assertFunction("to_unixtime(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getMillis() / 1000.0);
    }

    @Test
    public void testTimeZone()
    {
        assertFunction("hour(" + DATE_TIME_LITERAL + ")", DATE_TIME.getHourOfDay());
        assertFunction("minute(" + DATE_TIME_LITERAL + ")", DATE_TIME.getMinuteOfHour());
        assertFunction("hour(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getHourOfDay());
        assertFunction("minute(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getMinuteOfHour());
    }

    @Test
    public void testAtTimeZone()
    {
        assertEquals(functionAssertions.selectSingleValue("current_timestamp at time zone interval '07:09' hour to minute"),
                new TimestampWithTimeZone(session.getStartTime(), WEIRD_TIME_ZONE_KEY));

        assertEquals(functionAssertions.selectSingleValue("current_timestamp at time zone 'Asia/Oral'"), new TimestampWithTimeZone(session.getStartTime(), TimeZone.getTimeZone("Asia/Oral")));
        assertEquals(functionAssertions.selectSingleValue("now() at time zone 'Asia/Oral'"), new TimestampWithTimeZone(session.getStartTime(), TimeZone.getTimeZone("Asia/Oral")));
        assertEquals(functionAssertions.selectSingleValue("current_timestamp at time zone '+07:09'"), new TimestampWithTimeZone(session.getStartTime(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testPartFunctions()
    {
        assertFunction("second(" + DATE_TIME_LITERAL + ")", DATE_TIME.getSecondOfMinute());
        assertFunction("minute(" + DATE_TIME_LITERAL + ")", DATE_TIME.getMinuteOfHour());
        assertFunction("hour(" + DATE_TIME_LITERAL + ")", DATE_TIME.getHourOfDay());
        assertFunction("day_of_week(" + DATE_TIME_LITERAL + ")", DATE_TIME.dayOfWeek().get());
        assertFunction("dow(" + DATE_TIME_LITERAL + ")", DATE_TIME.dayOfWeek().get());
        assertFunction("day(" + DATE_TIME_LITERAL + ")", DATE_TIME.getDayOfMonth());
        assertFunction("day_of_month(" + DATE_TIME_LITERAL + ")", DATE_TIME.getDayOfMonth());
        assertFunction("day_of_year(" + DATE_TIME_LITERAL + ")", DATE_TIME.dayOfYear().get());
        assertFunction("doy(" + DATE_TIME_LITERAL + ")", DATE_TIME.dayOfYear().get());
        assertFunction("week(" + DATE_TIME_LITERAL + ")", DATE_TIME.weekOfWeekyear().get());
        assertFunction("week_of_year(" + DATE_TIME_LITERAL + ")", DATE_TIME.weekOfWeekyear().get());
        assertFunction("month(" + DATE_TIME_LITERAL + ")", DATE_TIME.getMonthOfYear());
        assertFunction("quarter(" + DATE_TIME_LITERAL + ")", DATE_TIME.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + DATE_TIME_LITERAL + ")", DATE_TIME.getYear());
        assertFunction("century(" + DATE_TIME_LITERAL + ")", DATE_TIME.getCenturyOfEra() + 1);

        assertFunction("second(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getSecondOfMinute());
        assertFunction("minute(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getMinuteOfHour());
        assertFunction("hour(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getHourOfDay());
        assertFunction("day_of_week(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.dayOfWeek().get());
        assertFunction("dow(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.dayOfWeek().get());
        assertFunction("day(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getDayOfMonth());
        assertFunction("day_of_month(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getDayOfMonth());
        assertFunction("day_of_year(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.dayOfYear().get());
        assertFunction("doy(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.dayOfYear().get());
        assertFunction("week(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.weekOfWeekyear().get());
        assertFunction("week_of_year(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.weekOfWeekyear().get());
        assertFunction("month(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getMonthOfYear());
        assertFunction("quarter(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getYear());
        assertFunction("century(" + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getCenturyOfEra() + 1);
        assertFunction("timezone_minute(" + WEIRD_DATE_TIME_LITERAL + ")", 9);
        assertFunction("timezone_hour(" + WEIRD_DATE_TIME_LITERAL + ")", 7);
    }

    @Test
    public void testExtractFromTimestamp()
    {
        assertFunction("extract(second FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getSecondOfMinute());
        assertFunction("extract(minute FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getMinuteOfHour());
        assertFunction("extract(hour FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getHourOfDay());
        assertFunction("extract(day_of_week FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getDayOfWeek());
        assertFunction("extract(dow FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getDayOfWeek());
        assertFunction("extract(day FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getDayOfMonth());
        assertFunction("extract(day_of_month FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getDayOfMonth());
        assertFunction("extract(day_of_year FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getDayOfYear());
        assertFunction("extract(doy FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getDayOfYear());
        assertFunction("extract(week FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getWeekOfWeekyear());
        assertFunction("extract(month FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getMonthOfYear());
        assertFunction("extract(quarter FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getMonthOfYear() / 4 + 1);
        assertFunction("extract(year FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getYear());
        assertFunction("extract(century FROM " + DATE_TIME_LITERAL + ")", DATE_TIME.getCenturyOfEra() + 1);

        assertFunction("extract(second FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getSecondOfMinute());
        assertFunction("extract(minute FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getMinuteOfHour());
        assertFunction("extract(hour FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getHourOfDay());
        assertFunction("extract(day_of_week FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getDayOfWeek());
        assertFunction("extract(dow FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getDayOfWeek());
        assertFunction("extract(day FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getDayOfMonth());
        assertFunction("extract(day_of_month FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getDayOfMonth());
        assertFunction("extract(day_of_year FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getDayOfYear());
        assertFunction("extract(doy FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getDayOfYear());
        assertFunction("extract(week FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getWeekOfWeekyear());
        assertFunction("extract(month FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getMonthOfYear());
        assertFunction("extract(quarter FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getMonthOfYear() / 4 + 1);
        assertFunction("extract(year FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getYear());
        assertFunction("extract(century FROM " + WEIRD_DATE_TIME_LITERAL + ")", WEIRD_DATE_TIME.getCenturyOfEra() + 1);
        assertFunction("extract(timezone_minute FROM " + WEIRD_DATE_TIME_LITERAL + ")", 9);
        assertFunction("extract(timezone_hour FROM " + WEIRD_DATE_TIME_LITERAL + ")", 7);
    }

    @Test
    public void testExtractFromTime()
    {
        assertFunction("extract(second FROM TIME '3:4:5.321')", 5);
        assertFunction("extract(minute FROM TIME '3:4:5.321')", 4);
        assertFunction("extract(hour FROM TIME '3:4:5.321')", 3);

        assertFunction("extract(second FROM TIME '3:4:5.321 +07:09')", 5);
        assertFunction("extract(minute FROM TIME '3:4:5.321 +07:09')", 4);
        assertFunction("extract(hour FROM TIME '3:4:5.321 +07:09')", 3);
    }

    @Test
    public void testExtractFromDate()
    {
        assertFunction("extract(day_of_week FROM DATE '2001-3-22')", 4);
        assertFunction("extract(dow FROM DATE '2001-3-22')", 4);
        assertFunction("extract(day FROM DATE '2001-3-22')", 22);
        assertFunction("extract(day_of_month FROM DATE '2001-3-22')", 22);
        assertFunction("extract(day_of_year FROM DATE '2001-3-22')", 81);
        assertFunction("extract(doy FROM DATE '2001-3-22')", 81);
        assertFunction("extract(week FROM DATE '2001-3-22')", 12);
        assertFunction("extract(month FROM DATE '2001-3-22')", 3);
        assertFunction("extract(quarter FROM DATE '2001-3-22')", 1);
        assertFunction("extract(year FROM DATE '2001-3-22')", 2001);
        assertFunction("extract(century FROM DATE '2001-3-22')", 21);
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
    public void testDateAdd()
    {
        assertFunction("date_add('second', 3, " + DATE_TIME_LITERAL + ")", toTimestamp(DATE_TIME.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + DATE_TIME_LITERAL + ")", toTimestamp(DATE_TIME.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + DATE_TIME_LITERAL + ")", toTimestamp(DATE_TIME.plusHours(3)));
        assertFunction("date_add('day', 3, " + DATE_TIME_LITERAL + ")", toTimestamp(DATE_TIME.plusDays(3)));
        assertFunction("date_add('week', 3, " + DATE_TIME_LITERAL + ")", toTimestamp(DATE_TIME.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + DATE_TIME_LITERAL + ")", toTimestamp(DATE_TIME.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + DATE_TIME_LITERAL + ")", toTimestamp(DATE_TIME.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + DATE_TIME_LITERAL + ")", toTimestamp(DATE_TIME.plusYears(3)));
        assertFunction("date_add('century', 3, " + DATE_TIME_LITERAL + ")", toTimestamp(CENTURY_FIELD.add(DATE_TIME.getMillis(), 3)));

        assertFunction("date_add('second', 3, " + WEIRD_DATE_TIME_LITERAL + ")", toTimestampWithTimeZone(WEIRD_DATE_TIME.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + WEIRD_DATE_TIME_LITERAL + ")", toTimestampWithTimeZone(WEIRD_DATE_TIME.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + WEIRD_DATE_TIME_LITERAL + ")", toTimestampWithTimeZone(WEIRD_DATE_TIME.plusHours(3)));
        assertFunction("date_add('day', 3, " + WEIRD_DATE_TIME_LITERAL + ")", toTimestampWithTimeZone(WEIRD_DATE_TIME.plusDays(3)));
        assertFunction("date_add('week', 3, " + WEIRD_DATE_TIME_LITERAL + ")", toTimestampWithTimeZone(WEIRD_DATE_TIME.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + WEIRD_DATE_TIME_LITERAL + ")", toTimestampWithTimeZone(WEIRD_DATE_TIME.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + WEIRD_DATE_TIME_LITERAL + ")", toTimestampWithTimeZone(WEIRD_DATE_TIME.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + WEIRD_DATE_TIME_LITERAL + ")", toTimestampWithTimeZone(WEIRD_DATE_TIME.plusYears(3)));
        assertFunction("date_add('century', 3, " + WEIRD_DATE_TIME_LITERAL + ")",
                toTimestampWithTimeZone(new DateTime(CENTURY_FIELD.add(WEIRD_DATE_TIME.getMillis(), 3), WEIRD_ZONE)));
    }

    @Test
    public void testDateDiff()
    {
        DateTime baseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, DATE_TIME_ZONE);
        String baseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678'";

        assertFunction("date_diff('second', " + baseDateTimeLiteral + ", " + DATE_TIME_LITERAL + ")", secondsBetween(baseDateTime, DATE_TIME).getSeconds());
        assertFunction("date_diff('minute', " + baseDateTimeLiteral + ", " + DATE_TIME_LITERAL + ")", minutesBetween(baseDateTime, DATE_TIME).getMinutes());
        assertFunction("date_diff('hour', " + baseDateTimeLiteral + ", " + DATE_TIME_LITERAL + ")", hoursBetween(baseDateTime, DATE_TIME).getHours());
        assertFunction("date_diff('day', " + baseDateTimeLiteral + ", " + DATE_TIME_LITERAL + ")", daysBetween(baseDateTime, DATE_TIME).getDays());
        assertFunction("date_diff('week', " + baseDateTimeLiteral + ", " + DATE_TIME_LITERAL + ")", weeksBetween(baseDateTime, DATE_TIME).getWeeks());
        assertFunction("date_diff('month', " + baseDateTimeLiteral + ", " + DATE_TIME_LITERAL + ")", monthsBetween(baseDateTime, DATE_TIME).getMonths());
        assertFunction("date_diff('quarter', " + baseDateTimeLiteral + ", " + DATE_TIME_LITERAL + ")", monthsBetween(baseDateTime, DATE_TIME).getMonths() / 4 + 1);
        assertFunction("date_diff('year', " + baseDateTimeLiteral + ", " + DATE_TIME_LITERAL + ")", yearsBetween(baseDateTime, DATE_TIME).getYears());
        assertFunction("date_diff('century', " + baseDateTimeLiteral + ", " + DATE_TIME_LITERAL + ")",
                (long) CENTURY_FIELD.getDifference(baseDateTime.getMillis(), DATE_TIME.getMillis()));

        DateTime weirdBaseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, WEIRD_ZONE);
        String weirdBaseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678 +07:09'";

        assertFunction("date_diff('second', " + weirdBaseDateTimeLiteral + ", " + WEIRD_DATE_TIME_LITERAL + ")", secondsBetween(weirdBaseDateTime, WEIRD_DATE_TIME).getSeconds());
        assertFunction("date_diff('minute', " + weirdBaseDateTimeLiteral + ", " + WEIRD_DATE_TIME_LITERAL + ")", minutesBetween(weirdBaseDateTime, WEIRD_DATE_TIME).getMinutes());
        assertFunction("date_diff('hour', " + weirdBaseDateTimeLiteral + ", " + WEIRD_DATE_TIME_LITERAL + ")", hoursBetween(weirdBaseDateTime, WEIRD_DATE_TIME).getHours());
        assertFunction("date_diff('day', " + weirdBaseDateTimeLiteral + ", " + WEIRD_DATE_TIME_LITERAL + ")", daysBetween(weirdBaseDateTime, WEIRD_DATE_TIME).getDays());
        assertFunction("date_diff('week', " + weirdBaseDateTimeLiteral + ", " + WEIRD_DATE_TIME_LITERAL + ")", weeksBetween(weirdBaseDateTime, WEIRD_DATE_TIME).getWeeks());
        assertFunction("date_diff('month', " + weirdBaseDateTimeLiteral + ", " + WEIRD_DATE_TIME_LITERAL + ")", monthsBetween(weirdBaseDateTime, WEIRD_DATE_TIME).getMonths());
        assertFunction("date_diff('quarter', " + weirdBaseDateTimeLiteral + ", " + WEIRD_DATE_TIME_LITERAL + ")",
                monthsBetween(weirdBaseDateTime, WEIRD_DATE_TIME).getMonths() / 4 + 1);
        assertFunction("date_diff('year', " + weirdBaseDateTimeLiteral + ", " + WEIRD_DATE_TIME_LITERAL + ")", yearsBetween(weirdBaseDateTime, WEIRD_DATE_TIME).getYears());
        assertFunction("date_diff('century', " + weirdBaseDateTimeLiteral + ", " + WEIRD_DATE_TIME_LITERAL + ")",
                (long) CENTURY_FIELD.getDifference(weirdBaseDateTime.getMillis(), WEIRD_DATE_TIME.getMillis()));
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
        assertFunction("format_datetime(" + DATE_TIME_LITERAL + ", 'YYYY/MM/DD HH:mm')", "2001/01/22 03:04");
        assertFunction("format_datetime(" + DATE_TIME_LITERAL + ", 'YYYY/MM/DD HH:mm ZZZZ')", "2001/01/22 03:04 Asia/Kathmandu");
        assertFunction("format_datetime(" + WEIRD_DATE_TIME_LITERAL + ", 'YYYY/MM/DD HH:mm')", "2001/01/22 03:04");
        assertFunction("format_datetime(" + WEIRD_DATE_TIME_LITERAL + ", 'YYYY/MM/DD HH:mm ZZZZ')", "2001/01/22 03:04 +07:09");
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

    private static Timestamp toTimestamp(long milliseconds)
    {
        return new Timestamp(milliseconds);
    }

    private static Timestamp toTimestamp(DateTime dateTime)
    {
        return new Timestamp(dateTime.getMillis());
    }

    private static TimestampWithTimeZone toTimestampWithTimeZone(DateTime dateTime)
    {
        return new TimestampWithTimeZone(dateTime.getMillis(), dateTime.getZone().toTimeZone());
    }
}
