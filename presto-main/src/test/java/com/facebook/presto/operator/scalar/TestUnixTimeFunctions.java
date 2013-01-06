/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.scalar;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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

        assertFunction("extract(century FROM " + millis + ")", dateTime.getCenturyOfEra());
        assertFunction("extract(day FROM " + millis + ")", dateTime.getDayOfMonth());
        assertFunction("extract(doy FROM " + millis + ")", dateTime.getDayOfYear());
        assertFunction("extract(hour FROM " + millis + ")", dateTime.getHourOfDay());
        assertFunction("extract(minute FROM " + millis + ")", dateTime.getMinuteOfHour());
        assertFunction("extract(month FROM " + millis + ")", dateTime.getMonthOfYear());
        assertFunction("extract(quarter FROM " + millis + ")", dateTime.getMonthOfYear() / 4 + 1);
        assertFunction("extract(second FROM " + millis + ")", dateTime.getSecondOfMinute());
        assertFunction("extract(week FROM " + millis + ")", dateTime.getWeekOfWeekyear());
        assertFunction("extract(year FROM " + millis + ")", dateTime.getYear());
    }
}
