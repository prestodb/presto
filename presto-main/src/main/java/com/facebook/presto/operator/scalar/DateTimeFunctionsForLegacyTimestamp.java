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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import static com.facebook.presto.operator.scalar.DateTimeFunctions.DATETIME_FORMATTER_CACHE;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.SECOND_OF_MINUTE;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.doDateFormat;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.doFormatDatetime;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.getTimestampField;
import static com.facebook.presto.operator.scalar.QuarterOfYearDateTimeField.QUARTER_OF_YEAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;

public final class DateTimeFunctionsForLegacyTimestamp
{
    private DateTimeFunctionsForLegacyTimestamp() {}

    @Description("current timestamp without time zone")
    @ScalarFunction("localtimestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long localTimestamp(ConnectorSession session)
    {
        return session.getStartTime();
    }

    @ScalarFunction("from_unixtime")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime)
    {
        return Math.round(unixTime * 1000);
    }

    @ScalarFunction("to_unixtime")
    @SqlType(StandardTypes.DOUBLE)
    public static double toUnixTime(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return timestamp / 1000.0;
    }

    @ScalarFunction("to_iso8601")
    @SqlType("varchar(35)")
    // YYYY-MM-DDTHH:MM:SS.mmm+HH:MM is a standard notation, and it requires 29 characters.
    // However extended notation with format ±(Y)+-MM-DDTHH:MM:SS.mmm+HH:MM is also acceptable and as
    // the maximum year represented by 64bits timestamp is ~584944387 it may require up to 35 characters.
    public static Slice toISO8601FromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.dateTime()
                .withChronology(getChronology(session.getTimeZoneKey()));
        return utf8Slice(formatter.print(timestamp));
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long truncateTimestamp(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).roundFloor(timestamp);
    }

    @Description("add the specified amount of time to the given timestamp")
    @LiteralParameters("x")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long addFieldValueTimestamp(
            ConnectorSession session,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).add(timestamp, toIntExact(value));
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTimestamp(
            ConnectorSession session,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp1,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp2)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).getDifferenceAsLong(timestamp2, timestamp1);
    }

    @Description("formats the given time by the given format")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatDatetime(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        return doFormatDatetime(getChronology(session.getTimeZoneKey()), session.getLocale(), timestamp, formatString);
    }

    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateFormat(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        return doDateFormat(getChronology(session.getTimeZoneKey()), session.getLocale(), timestamp, formatString);
    }

    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.TIMESTAMP)
    public static long dateParse(ConnectorSession session, @SqlType("varchar(x)") Slice dateTime, @SqlType("varchar(y)") Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(getChronology(session.getTimeZoneKey()))
                .withLocale(session.getLocale());

        try {
            return formatter.parseMillis(dateTime.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("second of the minute of the given timestamp")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTimestamp(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(timestamp);
    }

    @Description("minute of the hour of the given timestamp")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).minuteOfHour().get(timestamp);
    }

    @Description("hour of the day of the given timestamp")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).hourOfDay().get(timestamp);
    }

    @Description("day of the week of the given timestamp")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeekFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfWeek().get(timestamp);
    }

    @Description("day of the month of the given timestamp")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfMonth().get(timestamp);
    }

    @Description("day of the year of the given timestamp")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfYear().get(timestamp);
    }

    @Description("week of the year of the given timestamp")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @SqlType(StandardTypes.BIGINT)
    public static long weekFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).weekOfWeekyear().get(timestamp);
    }

    @Description("year of the ISO week of the given timestamp")
    @ScalarFunction(value = "year_of_week", alias = "yow")
    @SqlType(StandardTypes.BIGINT)
    public static long yearOfWeekFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).weekyear().get(timestamp);
    }

    @Description("month of the year of the given timestamp")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().get(timestamp);
    }

    @Description("quarter of the year of the given timestamp")
    @ScalarFunction("quarter")
    @SqlType(StandardTypes.BIGINT)
    public static long quarterFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return QUARTER_OF_YEAR.getField(getChronology(session.getTimeZoneKey())).get(timestamp);
    }

    @Description("year of the given timestamp")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).year().get(timestamp);
    }
}
