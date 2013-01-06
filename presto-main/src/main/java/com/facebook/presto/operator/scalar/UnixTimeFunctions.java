/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.scalar;

import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

public class UnixTimeFunctions
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(DateTimeZone.UTC);
    private static final DateTimeField SECOND_OF_MINUTE = UTC_CHRONOLOGY.secondOfMinute();
    private static final DateTimeField MINUTE_OF_HOUR = UTC_CHRONOLOGY.minuteOfHour();
    private static final DateTimeField HOUR_OF_DAY = UTC_CHRONOLOGY.hourOfDay();
    private static final DateTimeField DAY_OF_WEEK = UTC_CHRONOLOGY.dayOfWeek();
    private static final DateTimeField DAY_OF_MONTH = UTC_CHRONOLOGY.dayOfMonth();
    private static final DateTimeField DAY_OF_YEAR = UTC_CHRONOLOGY.dayOfYear();
    private static final DateTimeField WEEK_OF_YEAR = UTC_CHRONOLOGY.weekOfWeekyear();
    private static final DateTimeField MONTH_OF_YEAR = UTC_CHRONOLOGY.monthOfYear();
    private static final DateTimeField YEAR = UTC_CHRONOLOGY.year();
    private static final DateTimeField CENTURY = UTC_CHRONOLOGY.centuryOfEra();

    @ScalarFunction(value = "current_timestamp", alias = "now")
    public static long currentTimestamp()
    {
        // todo this must come from the Session so it is consistent for every call on every node
        return System.currentTimeMillis();
    }

    @ScalarFunction
    public static long second(long unixTime)
    {
        return SECOND_OF_MINUTE.get(unixTime);
    }

    @ScalarFunction
    public static long minute(long unixTime)
    {
        return MINUTE_OF_HOUR.get(unixTime);
    }

    @ScalarFunction
    public static long hour(long unixTime)
    {
        return HOUR_OF_DAY.get(unixTime);
    }

    @ScalarFunction(alias = {"day_of_week", "dow"})
    public static long dayOfWeek(long unixTime)
    {
        return DAY_OF_WEEK.get(unixTime);
    }

    @ScalarFunction(alias = {"day_of_month", "dayOfMonth"})
    public static long day(long unixTime)
    {
        return DAY_OF_MONTH.get(unixTime);
    }

    @ScalarFunction(alias = {"day_of_year", "doy"})
    public static long dayOfYear(long unixTime)
    {
        return DAY_OF_YEAR.get(unixTime);
    }

    @ScalarFunction(alias = {"week_of_year", "weekOfYear"})
    public static long week(long unixTime)
    {
        return WEEK_OF_YEAR.get(unixTime);
    }

    @ScalarFunction
    public static long month(long unixTime)
    {
        return MONTH_OF_YEAR.get(unixTime);
    }

    @ScalarFunction
    public static long quarter(long unixTime)
    {
        return (MONTH_OF_YEAR.get(unixTime) / 4) + 1;
    }

    @ScalarFunction
    public static long year(long unixTime)
    {
        return YEAR.get(unixTime);
    }

    @ScalarFunction
    public static long century(long unixTime)
    {
        return CENTURY.get(unixTime);
    }
}
