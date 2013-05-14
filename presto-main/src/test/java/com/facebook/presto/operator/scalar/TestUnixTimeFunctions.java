/*
 * Copyright 2004-present Facebook. All Rights Reserved.
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
    public void testPartFunctions()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long seconds = getSeconds(dateTime);

        assertFunction("second(" + seconds + ")", dateTime.getSecondOfMinute());
        assertFunction("minute(" + seconds + ")", dateTime.getMinuteOfHour());
        assertFunction("hour(" + seconds + ")", dateTime.getHourOfDay());
        assertFunction("dayOfWeek(" + seconds + ")", dateTime.dayOfWeek().get());
        assertFunction("dow(" + seconds + ")", dateTime.dayOfWeek().get());
        assertFunction("day(" + seconds + ")", dateTime.getDayOfMonth());
        assertFunction("day_of_month(" + seconds + ")", dateTime.getDayOfMonth());
        assertFunction("dayOfMonth(" + seconds + ")", dateTime.getDayOfMonth());
        assertFunction("dayOfYear(" + seconds + ")", dateTime.dayOfYear().get());
        assertFunction("doy(" + seconds + ")", dateTime.dayOfYear().get());
        assertFunction("week(" + seconds + ")", dateTime.weekOfWeekyear().get());
        assertFunction("week_of_year(" + seconds + ")", dateTime.weekOfWeekyear().get());
        assertFunction("weekOfYear(" + seconds + ")", dateTime.weekOfWeekyear().get());
        assertFunction("month(" + seconds + ")", dateTime.getMonthOfYear());
        assertFunction("quarter(" + seconds + ")", dateTime.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + seconds + ")", dateTime.getYear());
    }

    @Test
    public void testExtract()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long seconds = getSeconds(dateTime);

        assertFunction("extract(day FROM " + seconds + ")", dateTime.getDayOfMonth());
        assertFunction("extract(doy FROM " + seconds + ")", dateTime.getDayOfYear());
        assertFunction("extract(hour FROM " + seconds + ")", dateTime.getHourOfDay());
        assertFunction("extract(minute FROM " + seconds + ")", dateTime.getMinuteOfHour());
        assertFunction("extract(month FROM " + seconds + ")", dateTime.getMonthOfYear());
        assertFunction("extract(quarter FROM " + seconds + ")", dateTime.getMonthOfYear() / 4 + 1);
        assertFunction("extract(second FROM " + seconds + ")", dateTime.getSecondOfMinute());
        assertFunction("extract(week FROM " + seconds + ")", dateTime.getWeekOfWeekyear());
        assertFunction("extract(year FROM " + seconds + ")", dateTime.getYear());
        assertFunction("extract(century FROM " + seconds + ")", dateTime.getCenturyOfEra());
    }

    @Test
    public void testDateAdd()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long seconds = getSeconds(dateTime);

        assertFunction("dateAdd('day', 3, " + seconds + ")", getSeconds(dateTime.plusDays(3)));
        assertFunction("dateAdd('doy', 3, " + seconds + ")", getSeconds(dateTime.plusDays(3)));
        assertFunction("dateAdd('hour', 3, " + seconds + ")", getSeconds(dateTime.plusHours(3)));
        assertFunction("dateAdd('minute', 3, " + seconds + ")", getSeconds(dateTime.plusMinutes(3)));
        assertFunction("dateAdd('month', 3, " + seconds + ")", getSeconds(dateTime.plusMonths(3)));
        assertFunction("dateAdd('quarter', 3, " + seconds + ")", getSeconds(dateTime.plusMonths(3 * 3)));
        assertFunction("dateAdd('second', 3, " + seconds + ")", getSeconds(dateTime.plusSeconds(3)));
        assertFunction("dateAdd('week', 3, " + seconds + ")", getSeconds(dateTime.plusWeeks(3)));
        assertFunction("dateAdd('year', 3, " + seconds + ")", getSeconds(dateTime.plusYears(3)));
        assertFunction("dateAdd('century', 3, " + seconds + ")", fromMillis(CENTURY_FIELD.add(dateTime.getMillis(), 3)));
    }

    @Test
    public void testDateDiff()
    {
        DateTime dateTime1 = new DateTime(1960, 1, 22, 3, 4, 5, 0, DateTimeZone.UTC);
        long seconds1 = getSeconds(dateTime1);
        DateTime dateTime2 = new DateTime(2011, 5, 1, 7, 2, 9, 0, DateTimeZone.UTC);
        long seconds2 = getSeconds(dateTime2);

        assertFunction("dateDiff('day', " + seconds1 + ", " + seconds2 + ")", Days.daysBetween(dateTime1, dateTime2).getDays());
        assertFunction("dateDiff('doy', " + seconds1 + ", " + seconds2 + ")", Days.daysBetween(dateTime1, dateTime2).getDays());
        assertFunction("dateDiff('hour', " + seconds1 + ", " + seconds2 + ")", Hours.hoursBetween(dateTime1, dateTime2).getHours());
        assertFunction("dateDiff('minute', " + seconds1 + ", " + seconds2 + ")", Minutes.minutesBetween(dateTime1, dateTime2).getMinutes());
        assertFunction("dateDiff('month', " + seconds1 + ", " + seconds2 + ")", Months.monthsBetween(dateTime1, dateTime2).getMonths());
        assertFunction("dateDiff('quarter', " + seconds1 + ", " + seconds2 + ")", Months.monthsBetween(dateTime1, dateTime2).getMonths() / 4 + 1);
        assertFunction("dateDiff('second', " + seconds1 + ", " + seconds2 + ")", Seconds.secondsBetween(dateTime1, dateTime2).getSeconds());
        assertFunction("dateDiff('week', " + seconds1 + ", " + seconds2 + ")", Weeks.weeksBetween(dateTime1, dateTime2).getWeeks());
        assertFunction("dateDiff('year', " + seconds1 + ", " + seconds2 + ")", Years.yearsBetween(dateTime1, dateTime2).getYears());
        assertFunction("dateDiff('century', " + seconds1 + ", " + seconds2 + ")", fromMillis(CENTURY_FIELD.getDifference(dateTime1.getMillis(), dateTime2.getMillis())));
    }

    @Test
    public void testParseDatetime()
    {
        DateTimeZone timeZone = DateTimeZone.forOffsetHours(5);

        assertFunction("parsedatetime('1960/01/22 03:04', 'YYYY/MM/DD HH:mm')", getSeconds(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.UTC)));
        assertFunction("parsedatetime('1960/01/22 03:04 Asia/Oral', 'YYYY/MM/DD HH:mm ZZZZZ')", getSeconds(new DateTime(1960, 1, 22, 3, 4, 0, 0, timeZone)));
        assertFunction("parsedatetime('1960/01/22 03:04 +0500', 'YYYY/MM/DD HH:mm Z')", getSeconds(new DateTime(1960, 1, 22, 3, 4, 0, 0, timeZone)));
    }

    @Test
    public void testFormatDatetime()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long seconds = getSeconds(dateTime);

        assertFunction("formatDatetime(" + seconds + ", 'YYYY/MM/DD HH:mm')", "2001/01/22 03:04");
        assertFunction("formatDatetime(" + seconds + ", 'YYYY/MM/DD HH:mm ZZZZ')", "2001/01/22 03:04 UTC");
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
