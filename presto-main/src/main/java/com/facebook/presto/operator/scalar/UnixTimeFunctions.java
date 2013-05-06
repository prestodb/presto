/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.sql.analyzer.Session;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

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

    @ScalarFunction("now")
    public static long currentTimestamp(Session session)
    {
        return fromMillis(session.getStartTime());
    }

    @ScalarFunction(alias = "date_add")
    public static long dateAdd(Slice unit, long value, long unixTime)
    {
        return fromMillis(internalDateAdd(unit, value, toMillis(unixTime)));
    }

    private static long internalDateAdd(Slice unit, long value, long unixTime)
    {
        String unitString = unit.toString(Charsets.US_ASCII).toLowerCase();
        int intValue = Ints.checkedCast(value);
        switch (unitString) {
            case "second":
                return SECOND_OF_MINUTE.add(unixTime, intValue);
            case "minute":
                return MINUTE_OF_HOUR.add(unixTime, intValue);
            case "hour":
                return HOUR_OF_DAY.add(unixTime, intValue);
            case "day":
                return DAY_OF_MONTH.add(unixTime, intValue);
            case "dow":
                return DAY_OF_WEEK.add(unixTime, intValue);
            case "doy":
                return DAY_OF_YEAR.add(unixTime, intValue);
            case "week":
                return WEEK_OF_YEAR.add(unixTime, intValue);
            case "month":
                return MONTH_OF_YEAR.add(unixTime, intValue);
            case "year":
                return YEAR.add(unixTime, intValue);
            case "quarter":
                return MONTH_OF_YEAR.add(unixTime, intValue * 3);
            case "century":
                return CENTURY.add(unixTime, intValue);
            default:
                throw new IllegalArgumentException("Unsupported unit " + unitString);
        }
    }

    @ScalarFunction(alias = "date_diff")
    public static long dateDiff(Slice unit, long unixTime1, long unixTime2)
    {
        String unitString = unit.toString(Charsets.US_ASCII).toLowerCase();
        unixTime1 = toMillis(unixTime1);
        unixTime2 = toMillis(unixTime2);

        switch (unitString) {
            case "second":
                return SECOND_OF_MINUTE.getDifference(unixTime2, unixTime1);
            case "minute":
                return MINUTE_OF_HOUR.getDifference(unixTime2, unixTime1);
            case "hour":
                return HOUR_OF_DAY.getDifference(unixTime2, unixTime1);
            case "day":
                return DAY_OF_MONTH.getDifference(unixTime2, unixTime1);
            case "dow":
                return DAY_OF_WEEK.getDifference(unixTime2, unixTime1);
            case "doy":
                return DAY_OF_YEAR.getDifference(unixTime2, unixTime1);
            case "week":
                return WEEK_OF_YEAR.getDifference(unixTime2, unixTime1);
            case "month":
                return MONTH_OF_YEAR.getDifference(unixTime2, unixTime1);
            case "year":
                return YEAR.getDifference(unixTime2, unixTime1);
            case "quarter":
                return MONTH_OF_YEAR.getDifference(unixTime2, unixTime1) / 4 + 1;
            case "century":
                return CENTURY.getDifference(unixTime2, unixTime1);
            default:
                throw new IllegalArgumentException("Unsupported unit " + unitString);
        }
    }

    @ScalarFunction(alias = "parse_datetime")
    public static long parseDatetime(Slice datetime, Slice formatString)
    {
        String pattern = formatString.toString(Charsets.UTF_8);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern).withZoneUTC();

        String datetimeString = datetime.toString(Charsets.UTF_8);
        return fromMillis(formatter.parseMillis(datetimeString));
    }

    @ScalarFunction(alias = "format_datetime")
    public static Slice formatDatetime(long unixTime, Slice formatString)
    {
        String pattern = formatString.toString(Charsets.UTF_8);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern).withZoneUTC();

        String datetimeString = formatter.print(toMillis(unixTime));
        return Slices.wrappedBuffer(datetimeString.getBytes(Charsets.UTF_8));
    }

    @ScalarFunction
    public static long second(long unixTime)
    {
        return SECOND_OF_MINUTE.get(toMillis(unixTime));
    }

    @ScalarFunction
    public static long minute(long unixTime)
    {
        return MINUTE_OF_HOUR.get(toMillis(unixTime));
    }

    @ScalarFunction
    public static long hour(long unixTime)
    {
        return HOUR_OF_DAY.get(toMillis(unixTime));
    }

    @ScalarFunction(alias = {"day_of_week", "dow"})
    public static long dayOfWeek(long unixTime)
    {
        return DAY_OF_WEEK.get(toMillis(unixTime));
    }

    @ScalarFunction(alias = {"day_of_month", "dayOfMonth"})
    public static long day(long unixTime)
    {
        return DAY_OF_MONTH.get(toMillis(unixTime));
    }

    @ScalarFunction(alias = {"day_of_year", "doy"})
    public static long dayOfYear(long unixTime)
    {
        return DAY_OF_YEAR.get(toMillis(unixTime));
    }

    @ScalarFunction(alias = {"week_of_year", "weekOfYear"})
    public static long week(long unixTime)
    {
        return WEEK_OF_YEAR.get(toMillis(unixTime));
    }

    @ScalarFunction
    public static long month(long unixTime)
    {
        return MONTH_OF_YEAR.get(toMillis(unixTime));
    }

    @ScalarFunction
    public static long quarter(long unixTime)
    {
        return (MONTH_OF_YEAR.get(toMillis(unixTime)) / 4) + 1;
    }

    @ScalarFunction
    public static long year(long unixTime)
    {
        return YEAR.get(toMillis(unixTime));
    }

    @ScalarFunction
    public static long century(long unixTime)
    {
        return CENTURY.get(toMillis(unixTime));
    }

    private static long toMillis(long seconds)
    {
        return SECONDS.toMillis(seconds);
    }

    private static long fromMillis(long millis)
    {
        return MILLISECONDS.toSeconds(millis);
    }
}
