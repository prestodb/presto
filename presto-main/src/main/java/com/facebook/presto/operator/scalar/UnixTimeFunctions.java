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

import com.facebook.presto.operator.Description;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.ThreadLocalCache;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class UnixTimeFunctions
{
    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE = new ThreadLocalCache<Slice, DateTimeFormatter>(100)
    {
        @Override
        protected DateTimeFormatter load(Slice format)
        {
            return createDateTimeFormatter(format);
        }
    };

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

    private UnixTimeFunctions() {}

    @Description("s/system/current/")
    @ScalarFunction("now")
    public static long currentTimestamp(Session session)
    {
        return fromMillis(session.getStartTime());
    }

    @ScalarFunction("from_unixtime")
    public static long fromUnixTime(double unixTime)
    {
        return Math.round(unixTime);
    }

    @ScalarFunction("to_unixtime")
    public static double toUnixTime(long unixTime)
    {
        return unixTime;
    }

    @Description("add the specified amount of time to the given time")
    @ScalarFunction
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
            case "week":
                return WEEK_OF_YEAR.add(unixTime, intValue);
            case "month":
                return MONTH_OF_YEAR.add(unixTime, intValue);
            case "quarter":
                return MONTH_OF_YEAR.add(unixTime, intValue * 3);
            case "year":
                return YEAR.add(unixTime, intValue);
            case "century":
                return CENTURY.add(unixTime, intValue);
            default:
                throw new IllegalArgumentException("Unsupported unit " + unitString);
        }
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction
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
            case "week":
                return WEEK_OF_YEAR.getDifference(unixTime2, unixTime1);
            case "month":
                return MONTH_OF_YEAR.getDifference(unixTime2, unixTime1);
            case "quarter":
                return MONTH_OF_YEAR.getDifference(unixTime2, unixTime1) / 4 + 1;
            case "year":
                return YEAR.getDifference(unixTime2, unixTime1);
            case "century":
                return CENTURY.getDifference(unixTime2, unixTime1);
            default:
                throw new IllegalArgumentException("Unsupported unit " + unitString);
        }
    }

    @Description("parses the specified date/time by the given format")
    @ScalarFunction
    public static long parseDatetime(Slice datetime, Slice formatString)
    {
        String pattern = formatString.toString(Charsets.UTF_8);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern).withZoneUTC();

        String datetimeString = datetime.toString(Charsets.UTF_8);
        return fromMillis(formatter.parseMillis(datetimeString));
    }

    @Description("formats the given time by the given format")
    @ScalarFunction
    public static Slice formatDatetime(long unixTime, Slice formatString)
    {
        String pattern = formatString.toString(Charsets.UTF_8);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern).withZoneUTC();

        String datetimeString = formatter.print(toMillis(unixTime));
        return Slices.wrappedBuffer(datetimeString.getBytes(Charsets.UTF_8));
    }

    @ScalarFunction
    public static Slice dateFormat(long unixTime, Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString);
        return Slices.copiedBuffer(formatter.print(toMillis(unixTime)), Charsets.UTF_8);
    }

    @ScalarFunction
    public static long dateParse(Slice dateTime, Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString);
        return fromMillis(formatter.parseMillis(dateTime.toString(Charsets.UTF_8)));
    }

    @Description("second of the minute of the given time")
    @ScalarFunction
    public static long second(long unixTime)
    {
        return SECOND_OF_MINUTE.get(toMillis(unixTime));
    }

    @Description("minute of the hour of the given time")
    @ScalarFunction
    public static long minute(long unixTime)
    {
        return MINUTE_OF_HOUR.get(toMillis(unixTime));
    }

    @Description("hour of the day of the given time")
    @ScalarFunction
    public static long hour(long unixTime)
    {
        return HOUR_OF_DAY.get(toMillis(unixTime));
    }

    @Description("day of the week of the given time")
    @ScalarFunction(alias = "dow")
    public static long dayOfWeek(long unixTime)
    {
        return DAY_OF_WEEK.get(toMillis(unixTime));
    }

    @Description("day of the month of the given time")
    @ScalarFunction(alias = "day_of_month")
    public static long day(long unixTime)
    {
        return DAY_OF_MONTH.get(toMillis(unixTime));
    }

    @Description("day of the year of the given time")
    @ScalarFunction(alias = "doy")
    public static long dayOfYear(long unixTime)
    {
        return DAY_OF_YEAR.get(toMillis(unixTime));
    }

    @Description("week of the year of the given time")
    @ScalarFunction(alias = "week_of_year")
    public static long week(long unixTime)
    {
        return WEEK_OF_YEAR.get(toMillis(unixTime));
    }

    @Description("month of the year of the given time")
    @ScalarFunction
    public static long month(long unixTime)
    {
        return MONTH_OF_YEAR.get(toMillis(unixTime));
    }

    @Description("quarter of the year of the given time")
    @ScalarFunction
    public static long quarter(long unixTime)
    {
        return (MONTH_OF_YEAR.get(toMillis(unixTime)) / 4) + 1;
    }

    @Description("year of the given time")
    @ScalarFunction
    public static long year(long unixTime)
    {
        return YEAR.get(toMillis(unixTime));
    }

    @Description("century of the given time")
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

    @SuppressWarnings("fallthrough")
    public static DateTimeFormatter createDateTimeFormatter(Slice format)
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

        String formatString = format.toString(Charsets.UTF_8);
        boolean escaped = false;
        for (int i = 0; i < format.length(); i++) {
            char character = formatString.charAt(i);

            if (escaped) {
                switch (character) {
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                        builder.appendDayOfWeekShortText();
                        break;
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                        builder.appendMonthOfYearShortText();
                        break;
                    case 'c': // %c Month, numeric (0..12)
                        builder.appendMonthOfYear(1);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        builder.appendDayOfMonth(2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        builder.appendDayOfMonth(1);
                        break;
                    case 'f': // %f Microseconds (000000..999999)
                        builder.appendMillisOfSecond(6);
                        break;
                    case 'H': // %H Hour (00..23)
                        builder.appendHourOfDay(2);
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        builder.appendClockhourOfHalfday(2);
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        builder.appendMinuteOfHour(2);
                        break;
                    case 'j': // %j Day of year (001..366)
                        builder.appendDayOfYear(3);
                        break;
                    case 'k': // %k Hour (0..23)
                        builder.appendClockhourOfDay(1);
                        break;
                    case 'l': // %l Hour (1..12)
                        builder.appendClockhourOfHalfday(1);
                        break;
                    case 'M': // %M Month name (January..December)
                        builder.appendMonthOfYearText();
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        builder.appendMonthOfYear(2);
                        break;
                    case 'p': // %p AM or PM
                        builder.appendHalfdayOfDayText();
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        builder.appendClockhourOfHalfday(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2)
                                .appendLiteral(' ')
                                .appendHalfdayOfDayText();
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        builder.appendSecondOfMinute(2);
                        break;
                    case 'T': // %T Time, 24-hour (hh:mm:ss)
                        builder.appendHourOfDay(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2);
                        break;
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        builder.appendWeekOfWeekyear(2);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        builder.appendDayOfWeekText();
                        break;
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                        builder.appendDayOfWeek(1);
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        builder.appendYear(4, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendYearOfCentury(2, 2);
                        break;
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                    case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                    case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                        throw new UnsupportedOperationException(String.format("%%%s not supported in date format string", character));
                    case '%': // %% A literal “%” character
                        builder.appendLiteral('%');
                        break;
                    default: // %<x> The literal character represented by <x>
                        builder.appendLiteral(character);
                        break;
                }
                escaped = false;
            }
            else if (character == '%') {
                escaped = true;
            }
            else {
                builder.appendLiteral(character);
            }
        }

        return builder.toFormatter();
    }
}
