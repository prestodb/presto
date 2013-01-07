/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.scalar;

import org.joda.time.DateTime;
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
import static org.testng.Assert.assertTrue;

public class TestUnixTimeFunctions
{
    @Test
    public void testNow()
    {
        long min = System.currentTimeMillis();
        long now = (long) selectSingleValue("now()");
        long currentTimestamp = (long) selectSingleValue("current_timestamp");
        long max = System.currentTimeMillis();
        assertTrue(min <= now && now <= max);
        assertTrue(min <= currentTimestamp && currentTimestamp <= max);
    }

    @Test
    public void testPartFunctions()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long millis = dateTime.getMillis();

        assertFunction("second(" + millis + ")", dateTime.getSecondOfMinute());
        assertFunction("minute(" + millis + ")", dateTime.getMinuteOfHour());
        assertFunction("hour(" + millis + ")", dateTime.getHourOfDay());
        assertFunction("dayOfWeek(" + millis + ")", dateTime.dayOfWeek().get());
        assertFunction("dow(" + millis + ")", dateTime.dayOfWeek().get());
        assertFunction("day(" + millis + ")", dateTime.getDayOfMonth());
        assertFunction("day_of_month(" + millis + ")", dateTime.getDayOfMonth());
        assertFunction("dayOfMonth(" + millis + ")", dateTime.getDayOfMonth());
        assertFunction("dayOfYear(" + millis + ")", dateTime.dayOfYear().get());
        assertFunction("doy(" + millis + ")", dateTime.dayOfYear().get());
        assertFunction("week(" + millis + ")", dateTime.weekOfWeekyear().get());
        assertFunction("week_of_year(" + millis + ")", dateTime.weekOfWeekyear().get());
        assertFunction("weekOfYear(" + millis + ")", dateTime.weekOfWeekyear().get());
        assertFunction("month(" + millis + ")", dateTime.getMonthOfYear());
        assertFunction("quarter(" + millis + ")", dateTime.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + millis + ")", dateTime.getYear());
    }

    @Test
    public void testExtract()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long millis = dateTime.getMillis();

        assertFunction("extract(day FROM " + millis + ")", dateTime.getDayOfMonth());
        assertFunction("extract(doy FROM " + millis + ")", dateTime.getDayOfYear());
        assertFunction("extract(hour FROM " + millis + ")", dateTime.getHourOfDay());
        assertFunction("extract(minute FROM " + millis + ")", dateTime.getMinuteOfHour());
        assertFunction("extract(month FROM " + millis + ")", dateTime.getMonthOfYear());
        assertFunction("extract(quarter FROM " + millis + ")", dateTime.getMonthOfYear() / 4 + 1);
        assertFunction("extract(second FROM " + millis + ")", dateTime.getSecondOfMinute());
        assertFunction("extract(week FROM " + millis + ")", dateTime.getWeekOfWeekyear());
        assertFunction("extract(year FROM " + millis + ")", dateTime.getYear());
        assertFunction("extract(century FROM " + millis + ")", dateTime.getCenturyOfEra());
    }

    @Test
    public void testDateAdd()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long millis = dateTime.getMillis();

        assertFunction("dateAdd('day', 3, " + millis + ")", dateTime.plusDays(3).getMillis());
        assertFunction("dateAdd('doy', 3, " + millis + ")", dateTime.plusDays(3).getMillis());
        assertFunction("dateAdd('hour', 3, " + millis + ")", dateTime.plusHours(3).getMillis());
        assertFunction("dateAdd('minute', 3, " + millis + ")", dateTime.plusMinutes(3).getMillis());
        assertFunction("dateAdd('month', 3, " + millis + ")", dateTime.plusMonths(3).getMillis());
        assertFunction("dateAdd('quarter', 3, " + millis + ")", dateTime.plusMonths(3 * 3).getMillis());
        assertFunction("dateAdd('second', 3, " + millis + ")", dateTime.plusSeconds(3).getMillis());
        assertFunction("dateAdd('week', 3, " + millis + ")", dateTime.plusWeeks(3).getMillis());
        assertFunction("dateAdd('year', 3, " + millis + ")", dateTime.plusYears(3).getMillis());
        assertFunction("dateAdd('century', 3, " + millis + ")", ISOChronology.getInstance(DateTimeZone.UTC).centuryOfEra().add(millis, 3));
    }

    @Test
    public void testDateDiff()
    {
        DateTime dateTime1 = new DateTime(1960, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long millis1 = dateTime1.getMillis();
        DateTime dateTime2 = new DateTime(2011, 5, 1, 7, 2, 9, 456, DateTimeZone.UTC);
        long millis2 = dateTime2.getMillis();

        assertFunction("dateDiff('day', " + millis1 + ", " + millis2 + ")", Days.daysBetween(dateTime1, dateTime2).getDays());
        assertFunction("dateDiff('doy', " + millis1 + ", " + millis2 + ")", Days.daysBetween(dateTime1, dateTime2).getDays());
        assertFunction("dateDiff('hour', " + millis1 + ", " + millis2 + ")", Hours.hoursBetween(dateTime1, dateTime2).getHours());
        assertFunction("dateDiff('minute', " + millis1 + ", " + millis2 + ")", Minutes.minutesBetween(dateTime1, dateTime2).getMinutes());
        assertFunction("dateDiff('month', " + millis1 + ", " + millis2 + ")", Months.monthsBetween(dateTime1, dateTime2).getMonths());
        assertFunction("dateDiff('quarter', " + millis1 + ", " + millis2 + ")", Months.monthsBetween(dateTime1, dateTime2).getMonths() / 4 + 1);
        assertFunction("dateDiff('second', " + millis1 + ", " + millis2 + ")", Seconds.secondsBetween(dateTime1, dateTime2).getSeconds());
        assertFunction("dateDiff('week', " + millis1 + ", " + millis2 + ")", Weeks.weeksBetween(dateTime1, dateTime2).getWeeks());
        assertFunction("dateDiff('year', " + millis1 + ", " + millis2 + ")", Years.yearsBetween(dateTime1, dateTime2).getYears());
        assertFunction("dateDiff('century', " + millis1 + ", " + millis2 + ")", ISOChronology.getInstance(DateTimeZone.UTC).centuryOfEra().getDifference(millis2, millis1));
    }

    @Test
    public void testTimestampLiteral()
    {
        DateTimeZone timeZone = DateTimeZone.forOffsetHours(5);

        assertFunction("timestamp '1960-01-22 03:04:05.321'", new DateTime(1960, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis());
        assertFunction("timestamp '1960-01-22 03:04:05'", new DateTime(1960, 1, 22, 3, 4, 5, 0, DateTimeZone.UTC).getMillis());
        assertFunction("timestamp '1960-01-22 03:04'", new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.UTC).getMillis());
        assertFunction("timestamp '1960-01-22'", new DateTime(1960, 1, 22, 0, 0, 0, 0, DateTimeZone.UTC).getMillis());

        assertFunction("timestamp '1960-01-22 03:04:05.321Z'", new DateTime(1960, 1, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis());
        assertFunction("timestamp '1960-01-22 03:04:05Z'", new DateTime(1960, 1, 22, 3, 4, 5, 0, DateTimeZone.UTC).getMillis());
        assertFunction("timestamp '1960-01-22 03:04Z'", new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.UTC).getMillis());

        assertFunction("timestamp '1960-01-22 03:04:05.321+05:00'", new DateTime(1960, 1, 22, 3, 4, 5, 321, timeZone).getMillis());
        assertFunction("timestamp '1960-01-22 03:04:05+05:00'", new DateTime(1960, 1, 22, 3, 4, 5, 0, timeZone).getMillis());
        assertFunction("timestamp '1960-01-22 03:04+05:00'", new DateTime(1960, 1, 22, 3, 4, 0, 0, timeZone).getMillis());

        assertFunction("timestamp '1960-01-22 03:04:05.321+05'", new DateTime(1960, 1, 22, 3, 4, 5, 321, timeZone).getMillis());
        assertFunction("timestamp '1960-01-22 03:04:05+05'", new DateTime(1960, 1, 22, 3, 4, 5, 0, timeZone).getMillis());
        assertFunction("timestamp '1960-01-22 03:04+05'", new DateTime(1960, 1, 22, 3, 4, 0, 0, timeZone).getMillis());

        assertFunction("timestamp '1960-01-22 03:04:05.321 Asia/Oral'", new DateTime(1960, 1, 22, 3, 4, 5, 321, timeZone).getMillis());
        assertFunction("timestamp '1960-01-22 03:04:05 Asia/Oral'", new DateTime(1960, 1, 22, 3, 4, 5, 0, timeZone).getMillis());
        assertFunction("timestamp '1960-01-22 03:04 Asia/Oral'", new DateTime(1960, 1, 22, 3, 4, 0, 0, timeZone).getMillis());
    }
}
