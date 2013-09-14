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

import com.facebook.presto.sql.analyzer.Session;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.chrono.ISOChronology;
import org.testng.annotations.Test;

import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunction;
import static com.facebook.presto.operator.scalar.FunctionAssertions.selectSingleValue;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestUnixTimeFunctions
{
    private static final DateTimeField CENTURY_FIELD = ISOChronology.getInstance(DateTimeZone.UTC).centuryOfEra();

    @Test
    public void testCurrentTime()
    {
        long millis = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis();
        Session session = new Session("user", "test", DEFAULT_CATALOG, DEFAULT_SCHEMA, null, null, millis);

        assertEquals((long) selectSingleValue("current_timestamp", session), fromMillis(millis));
        assertEquals((long) selectSingleValue("now()", session), fromMillis(millis));
    }

    @Test
    public void testFromUnixTime()
    {
        long seconds = getSeconds(new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC));
        assertFunction("from_unixtime(980132645)", seconds);
        assertFunction("from_unixtime(980132645.888)", seconds + 1);
    }

    @Test
    public void testToUnixTime()
    {
        long seconds = getSeconds(new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC));
        assertFunction("to_unixtime(" + seconds + ")", (double) seconds);
    }

    @Test
    public void testPartFunctions()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long seconds = getSeconds(dateTime);

        assertFunction("second(" + seconds + ")", dateTime.getSecondOfMinute());
        assertFunction("minute(" + seconds + ")", dateTime.getMinuteOfHour());
        assertFunction("hour(" + seconds + ")", dateTime.getHourOfDay());
        assertFunction("day_of_week(" + seconds + ")", dateTime.dayOfWeek().get());
        assertFunction("dow(" + seconds + ")", dateTime.dayOfWeek().get());
        assertFunction("day(" + seconds + ")", dateTime.getDayOfMonth());
        assertFunction("day_of_month(" + seconds + ")", dateTime.getDayOfMonth());
        assertFunction("day_of_year(" + seconds + ")", dateTime.dayOfYear().get());
        assertFunction("doy(" + seconds + ")", dateTime.dayOfYear().get());
        assertFunction("week(" + seconds + ")", dateTime.weekOfWeekyear().get());
        assertFunction("week_of_year(" + seconds + ")", dateTime.weekOfWeekyear().get());
        assertFunction("month(" + seconds + ")", dateTime.getMonthOfYear());
        assertFunction("quarter(" + seconds + ")", dateTime.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + seconds + ")", dateTime.getYear());
        assertFunction("century(" + seconds + ")", dateTime.getCenturyOfEra());
    }

    @Test
    public void testExtract()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long seconds = getSeconds(dateTime);

        assertFunction("extract(second FROM " + seconds + ")", dateTime.getSecondOfMinute());
        assertFunction("extract(minute FROM " + seconds + ")", dateTime.getMinuteOfHour());
        assertFunction("extract(hour FROM " + seconds + ")", dateTime.getHourOfDay());
        assertFunction("extract(day_of_week FROM " + seconds + ")", dateTime.getDayOfWeek());
        assertFunction("extract(dow FROM " + seconds + ")", dateTime.getDayOfWeek());
        assertFunction("extract(day FROM " + seconds + ")", dateTime.getDayOfMonth());
        assertFunction("extract(day_of_month FROM " + seconds + ")", dateTime.getDayOfMonth());
        assertFunction("extract(day_of_year FROM " + seconds + ")", dateTime.getDayOfYear());
        assertFunction("extract(doy FROM " + seconds + ")", dateTime.getDayOfYear());
        assertFunction("extract(week FROM " + seconds + ")", dateTime.getWeekOfWeekyear());
        assertFunction("extract(month FROM " + seconds + ")", dateTime.getMonthOfYear());
        assertFunction("extract(quarter FROM " + seconds + ")", dateTime.getMonthOfYear() / 4 + 1);
        assertFunction("extract(year FROM " + seconds + ")", dateTime.getYear());
        assertFunction("extract(century FROM " + seconds + ")", dateTime.getCenturyOfEra());
    }

    @Test
    public void testDateAdd()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long seconds = getSeconds(dateTime);

        assertFunction("date_add('second', 3, " + seconds + ")", getSeconds(dateTime.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + seconds + ")", getSeconds(dateTime.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + seconds + ")", getSeconds(dateTime.plusHours(3)));
        assertFunction("date_add('day', 3, " + seconds + ")", getSeconds(dateTime.plusDays(3)));
        assertFunction("date_add('week', 3, " + seconds + ")", getSeconds(dateTime.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + seconds + ")", getSeconds(dateTime.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + seconds + ")", getSeconds(dateTime.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + seconds + ")", getSeconds(dateTime.plusYears(3)));
        assertFunction("date_add('century', 3, " + seconds + ")", fromMillis(CENTURY_FIELD.add(dateTime.getMillis(), 3)));
    }

    @Test
    public void testDateDiff()
    {
        DateTime dateTime1 = new DateTime(1960, 1, 22, 3, 4, 5, 0, DateTimeZone.UTC);
        long seconds1 = getSeconds(dateTime1);
        DateTime dateTime2 = new DateTime(2011, 5, 1, 7, 2, 9, 0, DateTimeZone.UTC);
        long seconds2 = getSeconds(dateTime2);

        assertFunction("date_diff('second', " + seconds1 + ", " + seconds2 + ")", Seconds.secondsBetween(dateTime1, dateTime2).getSeconds());
        assertFunction("date_diff('minute', " + seconds1 + ", " + seconds2 + ")", Minutes.minutesBetween(dateTime1, dateTime2).getMinutes());
        assertFunction("date_diff('hour', " + seconds1 + ", " + seconds2 + ")", Hours.hoursBetween(dateTime1, dateTime2).getHours());
        assertFunction("date_diff('day', " + seconds1 + ", " + seconds2 + ")", Days.daysBetween(dateTime1, dateTime2).getDays());
        assertFunction("date_diff('week', " + seconds1 + ", " + seconds2 + ")", Weeks.weeksBetween(dateTime1, dateTime2).getWeeks());
        assertFunction("date_diff('month', " + seconds1 + ", " + seconds2 + ")", Months.monthsBetween(dateTime1, dateTime2).getMonths());
        assertFunction("date_diff('quarter', " + seconds1 + ", " + seconds2 + ")", Months.monthsBetween(dateTime1, dateTime2).getMonths() / 4 + 1);
        assertFunction("date_diff('year', " + seconds1 + ", " + seconds2 + ")", Years.yearsBetween(dateTime1, dateTime2).getYears());
        assertFunction("date_diff('century', " + seconds1 + ", " + seconds2 + ")", fromMillis(CENTURY_FIELD.getDifference(dateTime1.getMillis(), dateTime2.getMillis())));
    }

    @Test
    public void testParseDatetime()
    {
        DateTimeZone timeZone = DateTimeZone.forOffsetHours(5);

        assertFunction("parse_datetime('1960/01/22 03:04', 'YYYY/MM/DD HH:mm')", getSeconds(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.UTC)));
        assertFunction("parse_datetime('1960/01/22 03:04 Asia/Oral', 'YYYY/MM/DD HH:mm ZZZZZ')", getSeconds(new DateTime(1960, 1, 22, 3, 4, 0, 0, timeZone)));
        assertFunction("parse_datetime('1960/01/22 03:04 +0500', 'YYYY/MM/DD HH:mm Z')", getSeconds(new DateTime(1960, 1, 22, 3, 4, 0, 0, timeZone)));
    }

    @Test
    public void testFormatDatetime()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long seconds = getSeconds(dateTime);

        assertFunction("format_datetime(" + seconds + ", 'YYYY/MM/DD HH:mm')", "2001/01/22 03:04");
        assertFunction("format_datetime(" + seconds + ", 'YYYY/MM/DD HH:mm ZZZZ')", "2001/01/22 03:04 UTC");
    }

    @Test
    public void testDateFormat()
    {
        DateTimeZone defaultTimeZone = DateTimeZone.getDefault();
        DateTimeZone localTimeZone = DateTimeZone.forOffsetHours(-8);
        DateTimeZone.setDefault(localTimeZone);
        try {
            DateTime dateTime = new DateTime(2001, 1, 9, 13, 4, 5, 0, localTimeZone);
            long seconds = getSeconds(dateTime);

            assertFunction("date_format(" + seconds + ", '%a')", "Tue");
            assertFunction("date_format(" + seconds + ", '%b')", "Jan");
            assertFunction("date_format(" + seconds + ", '%c')", "1");
            assertFunction("date_format(" + seconds + ", '%d')", "09");
            assertFunction("date_format(" + seconds + ", '%e')", "9");
            assertFunction("date_format(" + seconds + ", '%f')", "000000");
            assertFunction("date_format(" + seconds + ", '%H')", "13");
            assertFunction("date_format(" + seconds + ", '%h')", "01");
            assertFunction("date_format(" + seconds + ", '%I')", "01");
            assertFunction("date_format(" + seconds + ", '%i')", "04");
            assertFunction("date_format(" + seconds + ", '%j')", "009");
            assertFunction("date_format(" + seconds + ", '%k')", "13");
            assertFunction("date_format(" + seconds + ", '%l')", "1");
            assertFunction("date_format(" + seconds + ", '%M')", "January");
            assertFunction("date_format(" + seconds + ", '%m')", "01");
            assertFunction("date_format(" + seconds + ", '%p')", "PM");
            assertFunction("date_format(" + seconds + ", '%r')", "01:04:05 PM");
            assertFunction("date_format(" + seconds + ", '%S')", "05");
            assertFunction("date_format(" + seconds + ", '%s')", "05");
            assertFunction("date_format(" + seconds + ", '%T')", "13:04:05");
            assertFunction("date_format(" + seconds + ", '%v')", "02");
            assertFunction("date_format(" + seconds + ", '%W')", "Tuesday");
            assertFunction("date_format(" + seconds + ", '%w')", "2");
            assertFunction("date_format(" + seconds + ", '%Y')", "2001");
            assertFunction("date_format(" + seconds + ", '%y')", "01");
            assertFunction("date_format(" + seconds + ", '%%')", "%");
            assertFunction("date_format(" + seconds + ", 'foo')", "foo");
            assertFunction("date_format(" + seconds + ", '%g')", "g");
            assertFunction("date_format(" + seconds + ", '%4')", "4");
        }
        finally {
            DateTimeZone.setDefault(defaultTimeZone);
        }
    }

    @Test
    public void testDateParse()
    {
        DateTimeZone defaultTimeZone = DateTimeZone.getDefault();
        DateTimeZone localTimeZone = DateTimeZone.forOffsetHours(-8);
        DateTimeZone.setDefault(localTimeZone);
        try {
            assertFunction("date_parse('2013', '%Y')", getSeconds(new DateTime(2013, 1, 1, 0, 0, 0, 0, localTimeZone)));
            assertFunction("date_parse('2013-05', '%Y-%m')", getSeconds(new DateTime(2013, 5, 1, 0, 0, 0, 0, localTimeZone)));
            assertFunction("date_parse('2013-05-17', '%Y-%m-%d')", getSeconds(new DateTime(2013, 5, 17, 0, 0, 0, 0, localTimeZone)));
            assertFunction("date_parse('2013-05-17 12:35:10', '%Y-%m-%d %h:%i:%s')", getSeconds(new DateTime(2013, 5, 17, 0, 35, 10, 0, localTimeZone)));
            assertFunction("date_parse('2013-05-17 12:35:10 PM', '%Y-%m-%d %h:%i:%s %p')", getSeconds(new DateTime(2013, 5, 17, 12, 35, 10, 0, localTimeZone)));
            assertFunction("date_parse('2013-05-17 12:35:10 AM', '%Y-%m-%d %h:%i:%s %p')", getSeconds(new DateTime(2013, 5, 17, 0, 35, 10, 0, localTimeZone)));

            assertFunction("date_parse('2013-05-17 00:35:10', '%Y-%m-%d %H:%i:%s')", getSeconds(new DateTime(2013, 5, 17, 0, 35, 10, 0, localTimeZone)));
            assertFunction("date_parse('2013-05-17 23:35:10', '%Y-%m-%d %H:%i:%s')", getSeconds(new DateTime(2013, 5, 17, 23, 35, 10, 0, localTimeZone)));
            assertFunction("date_parse('abc 2013-05-17 fff 23:35:10 xyz', 'abc %Y-%m-%d fff %H:%i:%s xyz')", getSeconds(new DateTime(2013, 5, 17, 23, 35, 10, 0, localTimeZone)));

            assertFunction("date_parse('2013 14', '%Y %y')", getSeconds(new DateTime(2014, 1, 1, 0, 0, 0, 0, localTimeZone)));
        }
        finally {
            DateTimeZone.setDefault(defaultTimeZone);
        }
    }

    private static long getSeconds(DateTime dateTime)
    {
        return MILLISECONDS.toSeconds(dateTime.getMillis());
    }

    private static long fromMillis(long millis)
    {
        return MILLISECONDS.toSeconds(millis);
    }
}
