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

import com.facebook.airlift.concurrent.ThreadLocalCache;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimeZoneNotSupportedException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.TimestampOperators;
import io.airlift.slice.Slice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.common.type.DateTimeEncoding.updateMillisUtc;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.operator.scalar.QuarterOfYearDateTimeField.QUARTER_OF_YEAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeZoneIndex.extractZoneOffsetMinutes;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.packDateTimeWithZone;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackChronology;
import static com.facebook.presto.util.Failures.checkCondition;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class DateTimeFunctions
{
    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE =
            new ThreadLocalCache<>(100, DateTimeFunctions::createDateTimeFormatter);

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();
    private static final DateTimeField SECOND_OF_MINUTE = UTC_CHRONOLOGY.secondOfMinute();
    private static final DateTimeField MILLISECOND_OF_SECOND = UTC_CHRONOLOGY.millisOfSecond();
    private static final DateTimeField MINUTE_OF_HOUR = UTC_CHRONOLOGY.minuteOfHour();
    private static final DateTimeField HOUR_OF_DAY = UTC_CHRONOLOGY.hourOfDay();
    private static final DateTimeField DAY_OF_WEEK = UTC_CHRONOLOGY.dayOfWeek();
    private static final DateTimeField DAY_OF_MONTH = UTC_CHRONOLOGY.dayOfMonth();
    private static final DateTimeField DAY_OF_YEAR = UTC_CHRONOLOGY.dayOfYear();
    private static final DateTimeField WEEK_OF_YEAR = UTC_CHRONOLOGY.weekOfWeekyear();
    private static final DateTimeField YEAR_OF_WEEK = UTC_CHRONOLOGY.weekyear();
    private static final DateTimeField MONTH_OF_YEAR = UTC_CHRONOLOGY.monthOfYear();
    private static final DateTimeField QUARTER = QUARTER_OF_YEAR.getField(UTC_CHRONOLOGY);
    private static final DateTimeField YEAR = UTC_CHRONOLOGY.year();
    private static final Pattern PATTERN = Pattern.compile("^\\s*(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)\\s*$");
    private static final int MILLISECONDS_IN_SECOND = 1000;
    private static final int MILLISECONDS_IN_MINUTE = 60 * MILLISECONDS_IN_SECOND;
    private static final int MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;
    private static final int MILLISECONDS_IN_DAY = 24 * MILLISECONDS_IN_HOUR;
    private static final int PIVOT_YEAR = 2020; // yy = 70 will correspond to 1970 but 69 to 2069

    private DateTimeFunctions() {}

    @Description("current date")
    @ScalarFunction
    @SqlType(StandardTypes.DATE)
    public static long currentDate(SqlFunctionProperties properties)
    {
        ISOChronology chronology = getChronology(properties.getTimeZoneKey());

        // It is ok for this method to use the Object interfaces because it is constant folded during
        // plan optimization
        LocalDate currentDate = new DateTime(properties.getSessionStartTime(), chronology).toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), currentDate).getDays();
    }

    @Description("current time with time zone")
    @ScalarFunction
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long currentTime(SqlFunctionProperties properties)
    {
        // We do all calculation in UTC, as session.getStartTime() is in UTC
        // and we need to have UTC millis for packDateTimeWithZone
        long millis = UTC_CHRONOLOGY.millisOfDay().get(properties.getSessionStartTime());

        // However, those UTC millis are pointing to the correct UTC timestamp
        // Our TIME WITH TIME ZONE representation does use UTC 1970-01-01 representation
        // So we have to hack here in order to get valid representation
        // of TIME WITH TIME ZONE
        millis -= valueToSessionTimeZoneOffsetDiff(properties.getSessionStartTime(), getDateTimeZone(properties.getTimeZoneKey()));

        try {
            return packDateTimeWithZone(millis, properties.getTimeZoneKey());
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    @Description("current time without time zone")
    @ScalarFunction("localtime")
    @SqlType(StandardTypes.TIME)
    public static long localTime(SqlFunctionProperties properties)
    {
        if (properties.isLegacyTimestamp()) {
            long millis = UTC_CHRONOLOGY.millisOfDay().get(properties.getSessionStartTime());
            return millis - valueToSessionTimeZoneOffsetDiff(properties.getSessionStartTime(), getDateTimeZone(properties.getTimeZoneKey()));
        }
        ISOChronology localChronology = getChronology(properties.getTimeZoneKey());
        return localChronology.millisOfDay().get(properties.getSessionStartTime());
    }

    @Description("current time zone")
    @ScalarFunction("current_timezone")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentTimeZone(SqlFunctionProperties properties)
    {
        return utf8Slice(properties.getTimeZoneKey().getId());
    }

    @Description("current timestamp with time zone")
    @ScalarFunction(value = "current_timestamp", alias = "now")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long currentTimestamp(SqlFunctionProperties properties)
    {
        try {
            return packDateTimeWithZone(properties.getSessionStartTime(), properties.getTimeZoneKey());
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    @Description("current timestamp without time zone")
    @ScalarFunction("localtimestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long localTimestamp(SqlFunctionProperties properties)
    {
        if (properties.isLegacyTimestamp()) {
            return properties.getSessionStartTime();
        }
        ISOChronology localChronology = getChronology(properties.getTimeZoneKey());
        return localChronology.getZone().convertUTCToLocal(properties.getSessionStartTime());
    }

    @ScalarFunction("from_unixtime")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime)
    {
        // This implementation fixes previous issue of precision loss when it comes for some edge cases.
        // For example, 1.7041507095805E9 would correctly yield "2024-01-01 15:11:49.580"
        // Machine-representable double for the 1.7041507095805E9 is 1704150709.58049988746643066406.
        // 1704150709 goes to seconds and 0.580499887466 should be rounded to 580 milliseconds.
        // Previous implementation would wrongly result in 581 milliseconds.
        // Reference: https://github.com/prestodb/presto/issues/21891#issue-2126580070
        return Math.round(Math.floor(unixTime) * 1000 + Math.round((unixTime - Math.floor(unixTime)) * 1000));
    }

    @ScalarFunction("from_unixtime")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime, @SqlType(StandardTypes.BIGINT) long hoursOffset, @SqlType(StandardTypes.BIGINT) long minutesOffset)
    {
        TimeZoneKey timeZoneKey;
        try {
            timeZoneKey = getTimeZoneKeyForOffset(toIntExact(hoursOffset * 60 + minutesOffset));
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }

        try {
            return packDateTimeWithZone(Math.round(unixTime * 1000), timeZoneKey);
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    @ScalarFunction("from_unixtime")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime, @SqlType("varchar(x)") Slice zoneId)
    {
        try {
            return packDateTimeWithZone(Math.round(unixTime * 1000), zoneId.toStringUtf8());
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    @ScalarFunction("to_unixtime")
    @SqlType(StandardTypes.DOUBLE)
    public static double toUnixTime(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return timestamp / 1000.0;
    }

    @ScalarFunction("to_unixtime")
    @SqlType(StandardTypes.DOUBLE)
    public static double toUnixTimeFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackMillisUtc(timestampWithTimeZone) / 1000.0;
    }

    @ScalarFunction("to_iso8601")
    @SqlType("varchar(35)")
    // YYYY-MM-DDTHH:MM:SS.mmm+HH:MM is a standard notation, and it requires 29 characters.
    // However extended notation with format ±(Y)+-MM-DDTHH:MM:SS.mmm+HH:MM is also acceptable and as
    // the maximum year represented by 64bits timestamp is ~584944387 it may require up to 35 characters.
    public static Slice toISO8601FromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            DateTimeFormatter formatter = ISODateTimeFormat.dateTime()
                    .withChronology(getChronology(properties.getTimeZoneKey()));
            return utf8Slice(formatter.print(timestamp));
        }
        else {
            DateTimeFormatter formatter = ISODateTimeFormat.dateHourMinuteSecondMillis()
                    .withChronology(UTC_CHRONOLOGY);
            return utf8Slice(formatter.print(timestamp));
        }
    }

    @ScalarFunction("to_iso8601")
    @SqlType("varchar(35)")
    // YYYY-MM-DDTHH:MM:SS.mmm+HH:MM is a standard notation, and it requires 29 characters.
    // However extended notation with format ±(Y)+-MM-DDTHH:MM:SS.mmm+HH:MM is also acceptable and as
    // the maximum year represented by 64bits timestamp is ~584944387 it may require up to 35 characters.
    public static Slice toISO8601FromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        long millisUtc = unpackMillisUtc(timestampWithTimeZone);
        DateTimeFormatter formatter = ISODateTimeFormat.dateTime()
                .withChronology(getChronology(unpackZoneKey(timestampWithTimeZone)));
        return utf8Slice(formatter.print(millisUtc));
    }

    @ScalarFunction("to_iso8601")
    @SqlType("varchar(16)")
    // Standard format is YYYY-MM-DD, which gives up to 10 characters.
    // However extended notation with format ±(Y)+-MM-DD is also acceptable and as the maximum year
    // represented by 64bits timestamp is ~584944387 it may require up to 16 characters to represent a date.
    public static Slice toISO8601FromDate(SqlFunctionProperties properties, @SqlType(StandardTypes.DATE) long date)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.date()
                .withChronology(UTC_CHRONOLOGY);
        return utf8Slice(formatter.print(DAYS.toMillis(date)));
    }

    @ScalarFunction("from_iso8601_timestamp")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long fromISO8601Timestamp(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice iso8601DateTime)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser()
                .withChronology(getChronology(properties.getTimeZoneKey()))
                .withOffsetParsed();
        try {
            return packDateTimeWithZone(parseDateTimeHelper(formatter, iso8601DateTime.toStringUtf8()));
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    @ScalarFunction("from_iso8601_date")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long fromISO8601Date(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice iso8601DateTime)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.dateElementParser()
                .withChronology(UTC_CHRONOLOGY);
        DateTime dateTime = parseDateTimeHelper(formatter, iso8601DateTime.toStringUtf8());
        return MILLISECONDS.toDays(dateTime.getMillis());
    }

    @ScalarFunction(value = "at_timezone", visibility = HIDDEN)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeAtTimeZone(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone, @SqlType("varchar(x)") Slice zoneId)
    {
        return timeAtTimeZone(properties, timeWithTimeZone, getTimeZoneKey(zoneId.toStringUtf8()));
    }

    @ScalarFunction(value = "at_timezone", visibility = HIDDEN)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeAtTimeZone(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long zoneOffset)
    {
        checkCondition((zoneOffset % 60_000L) == 0L, INVALID_FUNCTION_ARGUMENT, "Invalid time zone offset interval: interval contains seconds");
        long zoneOffsetMinutes = zoneOffset / 60_000L;
        return timeAtTimeZone(properties, timeWithTimeZone, getTimeZoneKeyForOffset(zoneOffsetMinutes));
    }

    @ScalarFunction(value = "at_timezone", visibility = HIDDEN)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampAtTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone, @SqlType("varchar(x)") Slice zoneId)
    {
        try {
            return packDateTimeWithZone(unpackMillisUtc(timestampWithTimeZone), zoneId.toStringUtf8());
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    @ScalarFunction(value = "at_timezone", visibility = HIDDEN)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampAtTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long zoneOffset)
    {
        checkCondition((zoneOffset % 60_000L) == 0L, INVALID_FUNCTION_ARGUMENT, "Invalid time zone offset interval: interval contains seconds");
        long zoneOffsetMinutes = zoneOffset / 60_000L;
        try {
            return packDateTimeWithZone(unpackMillisUtc(timestampWithTimeZone), getTimeZoneKeyForOffset(zoneOffsetMinutes));
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long truncateDate(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date)
    {
        long millis = getDateField(UTC_CHRONOLOGY, unit).roundFloor(DAYS.toMillis(date));
        return MILLISECONDS.toDays(millis);
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME)
    public static long truncateTime(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIME) long time)
    {
        if (properties.isLegacyTimestamp()) {
            return getTimeField(getChronology(properties.getTimeZoneKey()), unit).roundFloor(time);
        }
        else {
            return getTimeField(UTC_CHRONOLOGY, unit).roundFloor(time);
        }
    }

    @Description("truncate to the specified precision")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long truncateTimeWithTimeZone(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone)
    {
        long millis = getTimeField(unpackChronology(timeWithTimeZone), unit).roundFloor(unpackMillisUtc(timeWithTimeZone));
        return updateMillisUtc(millis, timeWithTimeZone);
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long truncateTimestamp(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getTimestampField(getChronology(properties.getTimeZoneKey()), unit).roundFloor(timestamp);
        }
        else {
            return getTimestampField(UTC_CHRONOLOGY, unit).roundFloor(timestamp);
        }
    }

    @Description("truncate to the specified precision")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long truncateTimestampWithTimezone(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        long millis = getTimestampField(unpackChronology(timestampWithTimeZone), unit).roundFloor(unpackMillisUtc(timestampWithTimeZone));
        return updateMillisUtc(millis, timestampWithTimeZone);
    }

    @Description("add the specified amount of date to the given date")
    @LiteralParameters("x")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.DATE)
    public static long addFieldValueDate(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DATE) long date)
    {
        long millis = getDateField(UTC_CHRONOLOGY, unit).add(DAYS.toMillis(date), toIntExact(value));
        return MILLISECONDS.toDays(millis);
    }

    @Description("add the specified amount of time to the given time")
    @LiteralParameters("x")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.TIME)
    public static long addFieldValueTime(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.TIME) long time)
    {
        if (properties.isLegacyTimestamp()) {
            ISOChronology chronology = getChronology(properties.getTimeZoneKey());
            return modulo24Hour(chronology, getTimeField(chronology, unit).add(time, toIntExact(value)));
        }

        return modulo24Hour(getTimeField(UTC_CHRONOLOGY, unit).add(time, toIntExact(value)));
    }

    @Description("add the specified amount of time to the given time")
    @LiteralParameters("x")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long addFieldValueTimeWithTimeZone(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone)
    {
        ISOChronology chronology = unpackChronology(timeWithTimeZone);
        long millis = modulo24Hour(chronology, getTimeField(chronology, unit).add(unpackMillisUtc(timeWithTimeZone), toIntExact(value)));
        return updateMillisUtc(millis, timeWithTimeZone);
    }

    @Description("add the specified amount of time to the given timestamp")
    @LiteralParameters("x")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long addFieldValueTimestamp(
            SqlFunctionProperties properties,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getTimestampField(getChronology(properties.getTimeZoneKey()), unit).add(timestamp, toIntExact(value));
        }

        return getTimestampField(UTC_CHRONOLOGY, unit).add(timestamp, toIntExact(value));
    }

    @Description("add the specified amount of time to the given timestamp")
    @LiteralParameters("x")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long addFieldValueTimestampWithTimeZone(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        long millis = getTimestampField(unpackChronology(timestampWithTimeZone), unit).add(unpackMillisUtc(timestampWithTimeZone), toIntExact(value));
        return updateMillisUtc(millis, timestampWithTimeZone);
    }

    @Description("difference of the given dates in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffDate(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date1, @SqlType(StandardTypes.DATE) long date2)
    {
        return getDateField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(DAYS.toMillis(date2), DAYS.toMillis(date1));
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTime(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIME) long time1, @SqlType(StandardTypes.TIME) long time2)
    {
        if (properties.isLegacyTimestamp()) {
            // Session zone could have policy change on/around 1970-01-01, so we cannot use UTC
            ISOChronology chronology = getChronology(properties.getTimeZoneKey());
            return getTimeField(chronology, unit).getDifferenceAsLong(time2, time1);
        }

        return getTimeField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(time2, time1);
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTimeWithTimeZone(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone1,
            @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone2)
    {
        return getTimeField(unpackChronology(timeWithTimeZone1), unit).getDifferenceAsLong(unpackMillisUtc(timeWithTimeZone2), unpackMillisUtc(timeWithTimeZone1));
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTimestamp(
            SqlFunctionProperties properties,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp1,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp2)
    {
        if (properties.isLegacyTimestamp()) {
            return getTimestampField(getChronology(properties.getTimeZoneKey()), unit).getDifferenceAsLong(timestamp2, timestamp1);
        }

        return getTimestampField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(timestamp2, timestamp1);
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTimestampWithTimeZone(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone1,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone2)
    {
        return getTimestampField(unpackChronology(timestampWithTimeZone1), unit).getDifferenceAsLong(unpackMillisUtc(timestampWithTimeZone2), unpackMillisUtc(timestampWithTimeZone1));
    }

    private static DateTimeField getDateField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
                return chronology.year();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid DATE field");
    }

    private static DateTimeField getTimeField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "millisecond":
                return chronology.millisOfSecond();
            case "second":
                return chronology.secondOfMinute();
            case "minute":
                return chronology.minuteOfHour();
            case "hour":
                return chronology.hourOfDay();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Time field");
    }

    private static DateTimeField getTimestampField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "millisecond":
                return chronology.millisOfSecond();
            case "second":
                return chronology.secondOfMinute();
            case "minute":
                return chronology.minuteOfHour();
            case "hour":
                return chronology.hourOfDay();
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
                return chronology.year();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Timestamp field");
    }

    @Description("parses the specified date/time by the given format")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long parseDatetime(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice datetime, @SqlType("varchar(y)") Slice formatString)
    {
        try {
            return packDateTimeWithZone(parseDateTimeHelper(
                    DateTimeFormat.forPattern(formatString.toStringUtf8())
                            .withChronology(getChronology(properties.getTimeZoneKey()))
                            .withOffsetParsed()
                            .withLocale(properties.getSessionLocale()),
                    datetime.toStringUtf8()));
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    private static DateTime parseDateTimeHelper(DateTimeFormatter formatter, String datetimeString)
    {
        try {
            return formatter.parseDateTime(datetimeString);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("formats the given time by the given format")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatDatetime(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        if (properties.isLegacyTimestamp()) {
            return formatDatetime(getChronology(properties.getTimeZoneKey()), properties.getSessionLocale(), timestamp, formatString);
        }
        else {
            if (datetimeFormatSpecifiesZone(formatString)) {
                // Timezone is unknown for TIMESTAMP w/o TZ so it cannot be printed out.
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "format_datetime for TIMESTAMP type, cannot use 'Z' nor 'z' in format, as this type does not contain TZ information");
            }
            return formatDatetime(UTC_CHRONOLOGY, properties.getSessionLocale(), timestamp, formatString);
        }
    }

    /**
     * Checks whether {@link DateTimeFormat} pattern contains time zone-related field.
     */
    private static boolean datetimeFormatSpecifiesZone(Slice formatString)
    {
        boolean quoted = false;
        for (char c : formatString.toStringUtf8().toCharArray()) {
            if (quoted) {
                if (c == '\'') {
                    quoted = false;
                }
                continue;
            }

            switch (c) {
                case 'z':
                case 'Z':
                    return true;
                case '\'':
                    // '' (two apostrophes) in a pattern denote single apostrophe and here we interpret this as "start quote" + "end quote".
                    // This has no impact on method's result value.
                    quoted = true;
                    break;
            }
        }
        return false;
    }

    @Description("formats the given time by the given format")
    @ScalarFunction("format_datetime")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatDatetimeWithTimeZone(
            SqlFunctionProperties properties,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone,
            @SqlType("varchar(x)") Slice formatString)
    {
        return formatDatetime(unpackChronology(timestampWithTimeZone), properties.getSessionLocale(), unpackMillisUtc(timestampWithTimeZone), formatString);
    }

    private static Slice formatDatetime(ISOChronology chronology, Locale locale, long timestamp, Slice formatString)
    {
        try {
            return utf8Slice(DateTimeFormat.forPattern(formatString.toStringUtf8())
                    .withChronology(chronology)
                    .withLocale(locale)
                    .print(timestamp));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateFormat(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        if (properties.isLegacyTimestamp()) {
            return dateFormat(getChronology(properties.getTimeZoneKey()), properties.getSessionLocale(), timestamp, formatString);
        }
        else {
            return dateFormat(UTC_CHRONOLOGY, properties.getSessionLocale(), timestamp, formatString);
        }
    }

    @ScalarFunction("date_format")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateFormatWithTimeZone(
            SqlFunctionProperties properties,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone,
            @SqlType("varchar(x)") Slice formatString)
    {
        return dateFormat(unpackChronology(timestampWithTimeZone), properties.getSessionLocale(), unpackMillisUtc(timestampWithTimeZone), formatString);
    }

    private static Slice dateFormat(ISOChronology chronology, Locale locale, long timestamp, Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(chronology)
                .withLocale(locale);

        return utf8Slice(formatter.print(timestamp));
    }

    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.TIMESTAMP)
    public static long dateParse(SqlFunctionProperties properties, @SqlType("varchar(x)") Slice dateTime, @SqlType("varchar(y)") Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(properties.isLegacyTimestamp() ? getChronology(properties.getTimeZoneKey()) : UTC_CHRONOLOGY)
                .withLocale(properties.getSessionLocale());

        try {
            return formatter.parseMillis(dateTime.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("millisecond of the second of the given timestamp")
    @ScalarFunction("millisecond")
    @SqlType(StandardTypes.BIGINT)
    public static long millisecondFromTimestamp(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        // No need to check isLegacyTimestamp:
        // * Under legacy semantics, the session zone matters. But a zone always has offset of whole minutes.
        // * Under new semantics, timestamp is agnostic to the session zone.
        return MILLISECOND_OF_SECOND.get(timestamp);
    }

    @Description("millisecond of the second of the given timestamp")
    @ScalarFunction("millisecond")
    @SqlType(StandardTypes.BIGINT)
    public static long millisecondFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        // No need to check the associated zone here. A zone always has offset of whole minutes.
        return MILLISECOND_OF_SECOND.get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("millisecond of the second of the given time")
    @ScalarFunction("millisecond")
    @SqlType(StandardTypes.BIGINT)
    public static long millisecondFromTime(@SqlType(StandardTypes.TIME) long time)
    {
        // No need to check isLegacyTimestamp:
        // * Under legacy semantics, the session zone matters. But a zone always has offset of whole minutes.
        // * Under new semantics, time is agnostic to the session zone.
        return MILLISECOND_OF_SECOND.get(time);
    }

    @Description("millisecond of the second of the given time")
    @ScalarFunction("millisecond")
    @SqlType(StandardTypes.BIGINT)
    public static long millisecondFromTimeWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time)
    {
        // No need to check the associated zone here. A zone always has offset of whole minutes.
        return MILLISECOND_OF_SECOND.get(unpackMillisUtc(time));
    }

    @Description("millisecond of the second of the given interval")
    @ScalarFunction("millisecond")
    @SqlType(StandardTypes.BIGINT)
    public static long millisecondFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return milliseconds % MILLISECONDS_IN_SECOND;
    }

    @Description("second of the minute of the given timestamp")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).secondOfMinute().get(timestamp);
        }
        else {
            return SECOND_OF_MINUTE.get(timestamp);
        }
    }

    @Description("second of the minute of the given timestamp")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).secondOfMinute().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("second of the minute of the given time")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTime(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME) long time)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).secondOfMinute().get(time);
        }
        else {
            return SECOND_OF_MINUTE.get(time);
        }
    }

    @Description("second of the minute of the given time")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTimeWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone)
    {
        return unpackChronology(timeWithTimeZone).secondOfMinute().get(unpackMillisUtc(timeWithTimeZone));
    }

    @Description("second of the minute of the given interval")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_MINUTE) / MILLISECONDS_IN_SECOND;
    }

    @Description("minute of the hour of the given timestamp")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).minuteOfHour().get(timestamp);
        }
        else {
            return MINUTE_OF_HOUR.get(timestamp);
        }
    }

    @Description("minute of the hour of the given timestamp")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).minuteOfHour().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("minute of the hour of the given time")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromTime(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME) long time)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).minuteOfHour().get(time);
        }
        else {
            return MINUTE_OF_HOUR.get(time);
        }
    }

    @Description("minute of the hour of the given time")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromTimeWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone)
    {
        return unpackChronology(timeWithTimeZone).minuteOfHour().get(unpackMillisUtc(timeWithTimeZone));
    }

    @Description("minute of the hour of the given interval")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_HOUR) / MILLISECONDS_IN_MINUTE;
    }

    @Description("hour of the day of the given timestamp")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).hourOfDay().get(timestamp);
        }
        else {
            return HOUR_OF_DAY.get(timestamp);
        }
    }

    @Description("hour of the day of the given timestamp")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).hourOfDay().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("hour of the day of the given time")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromTime(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME) long time)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).hourOfDay().get(time);
        }
        else {
            return HOUR_OF_DAY.get(time);
        }
    }

    @Description("hour of the day of the given time")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromTimeWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone)
    {
        return unpackChronology(timeWithTimeZone).hourOfDay().get(unpackMillisUtc(timeWithTimeZone));
    }

    @Description("hour of the day of the given interval")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_DAY) / MILLISECONDS_IN_HOUR;
    }

    @Description("day of the week of the given timestamp")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeekFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).dayOfWeek().get(timestamp);
        }
        else {
            return DAY_OF_WEEK.get(timestamp);
        }
    }

    @Description("day of the week of the given timestamp")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeekFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).dayOfWeek().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("day of the week of the given date")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DAY_OF_WEEK.get(DAYS.toMillis(date));
    }

    @Description("day of the month of the given timestamp")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).dayOfMonth().get(timestamp);
        }
        else {
            return DAY_OF_MONTH.get(timestamp);
        }
    }

    @Description("day of the month of the given timestamp")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).dayOfMonth().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("day of the month of the given date")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DAY_OF_MONTH.get(DAYS.toMillis(date));
    }

    @Description("day of the month of the given interval")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromInterval(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long milliseconds)
    {
        return milliseconds / MILLISECONDS_IN_DAY;
    }

    @Description("last day of the month of the given timestamp")
    @ScalarFunction("last_day_of_month")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonthFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        ISOChronology isoChronology = unpackChronology(timestampWithTimeZone);
        long millis = unpackMillisUtc(timestampWithTimeZone);
        // Calculate point in time corresponding to midnight (00:00) of first day of next month in the given zone.
        millis = isoChronology.monthOfYear().roundCeiling(millis + 1);
        // Convert to UTC and take the previous day
        millis = isoChronology.getZone().convertUTCToLocal(millis) - MILLISECONDS_IN_DAY;
        return MILLISECONDS.toDays(millis);
    }

    @Description("last day of the month of the given timestamp")
    @ScalarFunction("last_day_of_month")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonthFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            long date = TimestampOperators.castToDate(properties, timestamp);
            return lastDayOfMonthFromDate(date);
        }
        long millis = UTC_CHRONOLOGY.monthOfYear().roundCeiling(timestamp + 1) - MILLISECONDS_IN_DAY;
        return MILLISECONDS.toDays(millis);
    }

    @Description("last day of the month of the given date")
    @ScalarFunction("last_day_of_month")
    @SqlType(StandardTypes.DATE)
    public static long lastDayOfMonthFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        long millis = UTC_CHRONOLOGY.monthOfYear().roundCeiling(DAYS.toMillis(date) + 1) - MILLISECONDS_IN_DAY;
        return MILLISECONDS.toDays(millis);
    }

    @Description("day of the year of the given timestamp")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).dayOfYear().get(timestamp);
        }
        else {
            return DAY_OF_YEAR.get(timestamp);
        }
    }

    @Description("day of the year of the given timestamp")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).dayOfYear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("day of the year of the given date")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DAY_OF_YEAR.get(DAYS.toMillis(date));
    }

    @Description("week of the year of the given timestamp")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @SqlType(StandardTypes.BIGINT)
    public static long weekFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).weekOfWeekyear().get(timestamp);
        }
        else {
            return WEEK_OF_YEAR.get(timestamp);
        }
    }

    @Description("week of the year of the given timestamp")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @SqlType(StandardTypes.BIGINT)
    public static long weekFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).weekOfWeekyear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("week of the year of the given date")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @SqlType(StandardTypes.BIGINT)
    public static long weekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return WEEK_OF_YEAR.get(DAYS.toMillis(date));
    }

    @Description("year of the ISO week of the given timestamp")
    @ScalarFunction(value = "year_of_week", alias = "yow")
    @SqlType(StandardTypes.BIGINT)
    public static long yearOfWeekFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).weekyear().get(timestamp);
        }
        else {
            return YEAR_OF_WEEK.get(timestamp);
        }
    }

    @Description("year of the ISO week of the given timestamp")
    @ScalarFunction(value = "year_of_week", alias = "yow")
    @SqlType(StandardTypes.BIGINT)
    public static long yearOfWeekFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).weekyear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("year of the ISO week of the given date")
    @ScalarFunction(value = "year_of_week", alias = "yow")
    @SqlType(StandardTypes.BIGINT)
    public static long yearOfWeekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return YEAR_OF_WEEK.get(DAYS.toMillis(date));
    }

    @Description("month of the year of the given timestamp")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).monthOfYear().get(timestamp);
        }
        else {
            return MONTH_OF_YEAR.get(timestamp);
        }
    }

    @Description("month of the year of the given timestamp")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).monthOfYear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("month of the year of the given date")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return MONTH_OF_YEAR.get(DAYS.toMillis(date));
    }

    @Description("month of the year of the given interval")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromInterval(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long months)
    {
        return months % 12;
    }

    @Description("quarter of the year of the given timestamp")
    @ScalarFunction("quarter")
    @SqlType(StandardTypes.BIGINT)
    public static long quarterFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return QUARTER_OF_YEAR.getField(getChronology(properties.getTimeZoneKey())).get(timestamp);
        }
        else {
            return QUARTER_OF_YEAR.getField(UTC_CHRONOLOGY).get(timestamp);
        }
    }

    @Description("quarter of the year of the given timestamp")
    @ScalarFunction("quarter")
    @SqlType(StandardTypes.BIGINT)
    public static long quarterFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return QUARTER_OF_YEAR.getField(unpackChronology(timestampWithTimeZone)).get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("quarter of the year of the given date")
    @ScalarFunction("quarter")
    @SqlType(StandardTypes.BIGINT)
    public static long quarterFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return QUARTER.get(DAYS.toMillis(date));
    }

    @Description("year of the given timestamp")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        if (properties.isLegacyTimestamp()) {
            return getChronology(properties.getTimeZoneKey()).year().get(timestamp);
        }
        else {
            return YEAR.get(timestamp);
        }
    }

    @Description("year of the given timestamp")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).year().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("year of the given date")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return YEAR.get(DAYS.toMillis(date));
    }

    @Description("year of the given interval")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromInterval(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long months)
    {
        return months / 12;
    }

    @Description("time zone minute of the given timestamp")
    @ScalarFunction("timezone_minute")
    @SqlType(StandardTypes.BIGINT)
    public static long timeZoneMinuteFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return extractZoneOffsetMinutes(timestampWithTimeZone) % 60;
    }

    @Description("time zone hour of the given timestamp")
    @ScalarFunction("timezone_hour")
    @SqlType(StandardTypes.BIGINT)
    public static long timeZoneHourFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        return extractZoneOffsetMinutes(timestampWithTimeZone) / 60;
    }

    @SuppressWarnings("fallthrough")
    public static DateTimeFormatter createDateTimeFormatter(Slice format)
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

        String formatString = format.toStringUtf8();
        boolean escaped = false;
        for (int i = 0; i < formatString.length(); i++) {
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
                        builder.appendFractionOfSecond(6, 9);
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
                        builder.appendHourOfDay(1);
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
                    case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
                        builder.appendWeekyear(4, 4);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        builder.appendDayOfWeekText();
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        builder.appendYear(4, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendTwoDigitYear(PIVOT_YEAR);
                        break;
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                    case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, String.format("%%%s not supported in date format string", character));
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

        try {
            return builder.toFormatter();
        }
        catch (UnsupportedOperationException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    @Description("convert duration string to an interval")
    @ScalarFunction("parse_duration")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long parseDuration(@SqlType("varchar(x)") Slice duration)
    {
        String durationStr = duration.toStringUtf8();

        if (durationStr.isEmpty()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "duration is empty");
        }

        try {
            Matcher matcher = PATTERN.matcher(durationStr);

            if (!matcher.matches()) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                        "duration is not a valid data duration string: " + durationStr);
            }

            BigDecimal value = new BigDecimal(matcher.group(1));
            TimeUnit timeUnit = Duration.valueOfTimeUnit(matcher.group(2));

            return value.multiply(millisPerTimeUnit(timeUnit))
                    .add(BigDecimal.valueOf(0.5)).longValue();
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    private static BigDecimal millisPerTimeUnit(TimeUnit timeUnit)
    {
        switch (timeUnit) {
            case NANOSECONDS:
                return new BigDecimal("0.000001");
            case MICROSECONDS:
                return new BigDecimal("0.001");
            case MILLISECONDS:
                return BigDecimal.ONE;
            case SECONDS:
                return BigDecimal.valueOf(1000);
            case MINUTES:
                return BigDecimal.valueOf(60_000);
            case HOURS:
                return BigDecimal.valueOf(3_600_000);
            case DAYS:
                return BigDecimal.valueOf(86_400_000);
            default:
                throw new AssertionError("Unknown TimeUnit: " + timeUnit);
        }
    }

    private static long timeAtTimeZone(SqlFunctionProperties properties, long timeWithTimeZone, TimeZoneKey timeZoneKey)
    {
        DateTimeZone sourceTimeZone = getDateTimeZone(unpackZoneKey(timeWithTimeZone));
        DateTimeZone targetTimeZone = getDateTimeZone(timeZoneKey);
        long millis = unpackMillisUtc(timeWithTimeZone);

        // STEP 1. Calculate source UTC millis in session start
        millis += valueToSessionTimeZoneOffsetDiff(properties.getSessionStartTime(), sourceTimeZone);

        // STEP 2. Calculate target UTC millis in 1970
        millis -= valueToSessionTimeZoneOffsetDiff(properties.getSessionStartTime(), targetTimeZone);

        // STEP 3. Make sure that value + offset is in 0 - 23:59:59.999
        long localMillis = millis + targetTimeZone.getOffset(0);
        // Loops up to 2 times in total
        while (localMillis > TimeUnit.DAYS.toMillis(1)) {
            millis -= TimeUnit.DAYS.toMillis(1);
            localMillis -= TimeUnit.DAYS.toMillis(1);
        }
        while (localMillis < 0) {
            millis += TimeUnit.DAYS.toMillis(1);
            localMillis += TimeUnit.DAYS.toMillis(1);
        }

        try {
            return packDateTimeWithZone(millis, timeZoneKey);
        }
        catch (NotSupportedException | TimeZoneNotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e.getMessage(), e);
        }
    }

    // HACK WARNING!
    // This method does calculate difference between timezone offset on current date (session start)
    // and 1970-01-01 (same timezone). This is used to be able to avoid using fixed offset TZ for
    // places where TZ offset is explicitly accessed (namely AT TIME ZONE).
    // DateTimeFormatter does format specified instance in specified time zone calculating offset for
    // that time zone based on provided instance. As Presto TIME type is represented as millis since
    // 00:00.000 of some day UTC, we always use timezone offset that was valid on 1970-01-01.
    // Best effort without changing representation of TIME WITH TIME ZONE is to use offset of the timezone
    // based on session start time.
    // By adding this difference to instance that we would like to convert to other TZ, we can
    // get exact value of utcMillis for current session start time.
    // Silent assumption is made, that no changes in TZ offsets were done on 1970-01-01.
    private static long valueToSessionTimeZoneOffsetDiff(long millisUtcSessionStart, DateTimeZone timeZone)
    {
        return timeZone.getOffset(0) - timeZone.getOffset(millisUtcSessionStart);
    }

    @ScalarFunction("to_milliseconds")
    @SqlType(StandardTypes.BIGINT)
    public static long toMilliseconds(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long value)
    {
        return value;
    }
}
