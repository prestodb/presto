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
import io.airlift.concurrent.ThreadLocalCache;
import io.airlift.slice.Slice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.Days;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;

import static com.facebook.presto.operator.scalar.QuarterOfYearDateTimeField.QUARTER_OF_YEAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.spi.type.DateTimeEncoding.updateMillisUtc;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeZoneIndex.extractZoneOffsetMinutes;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.DateTimeZoneIndex.packDateTimeWithZone;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackChronology;
import static com.facebook.presto.util.Failures.checkCondition;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

public final class DateTimeFunctions
{
    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE =
            new ThreadLocalCache<>(100, DateTimeFunctions::createDateTimeFormatter);

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);
    private static final DateTimeField SECOND_OF_MINUTE = UTC_CHRONOLOGY.secondOfMinute();
    private static final DateTimeField DAY_OF_WEEK = UTC_CHRONOLOGY.dayOfWeek();
    private static final DateTimeField DAY_OF_MONTH = UTC_CHRONOLOGY.dayOfMonth();
    private static final DateTimeField DAY_OF_YEAR = UTC_CHRONOLOGY.dayOfYear();
    private static final DateTimeField WEEK_OF_YEAR = UTC_CHRONOLOGY.weekOfWeekyear();
    private static final DateTimeField YEAR_OF_WEEK = UTC_CHRONOLOGY.weekyear();
    private static final DateTimeField MONTH_OF_YEAR = UTC_CHRONOLOGY.monthOfYear();
    private static final DateTimeField QUARTER = QUARTER_OF_YEAR.getField(UTC_CHRONOLOGY);
    private static final DateTimeField YEAR = UTC_CHRONOLOGY.year();
    private static final int MILLISECONDS_IN_SECOND = 1000;
    private static final int MILLISECONDS_IN_MINUTE = 60 * MILLISECONDS_IN_SECOND;
    private static final int MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;
    private static final int MILLISECONDS_IN_DAY = 24 * MILLISECONDS_IN_HOUR;
    private static final int PIVOT_YEAR = 2020; // yy = 70 will correspond to 1970 but 69 to 2069

    private DateTimeFunctions() {}

    @Description("current date")
    @ScalarFunction
    @SqlType(StandardTypes.DATE)
    public static long currentDate(ConnectorSession session)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());

        // It is ok for this method to use the Object interfaces because it is constant folded during
        // plan optimization
        DateTime currentDateTime = new DateTime(session.getStartTime(), chronology).withTimeAtStartOfDay();
        DateTime baseDateTime = new DateTime(1970, 1, 1, 0, 0, chronology).withTimeAtStartOfDay();
        return Days.daysBetween(baseDateTime, currentDateTime).getDays();
    }

    @Description("current time with time zone")
    @ScalarFunction
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long currentTime(ConnectorSession session)
    {
        // We do all calculation in UTC, as session.getStartTime() is in UTC
        // and we need to have UTC millis for packDateTimeWithZone
        long millis = UTC_CHRONOLOGY.millisOfDay().get(session.getStartTime());

        if (!session.isLegacyTimestamp()) {
            // However, those UTC millis are pointing to the correct UTC timestamp
            // Our TIME WITH TIME ZONE representation does use UTC 1970-01-01 representation
            // So we have to hack here in order to get valid representation
            // of TIME WITH TIME ZONE
            millis -= valueToSessionTimeZoneOffsetDiff(millis, session.getStartTime(), getDateTimeZone(session.getTimeZoneKey()));
        }
        return packDateTimeWithZone(millis, session.getTimeZoneKey());
    }

    @Description("current time without time zone")
    @ScalarFunction("localtime")
    @SqlType(StandardTypes.TIME)
    public static long localTime(ConnectorSession session)
    {
        ISOChronology localChronology = getChronology(session.getTimeZoneKey());
        return localChronology.millisOfDay().get(session.getStartTime());
    }

    @Description("current time zone")
    @ScalarFunction("current_timezone")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentTimeZone(ConnectorSession session)
    {
        return utf8Slice(session.getTimeZoneKey().getId());
    }

    @Description("current timestamp with time zone")
    @ScalarFunction(value = "current_timestamp", alias = "now")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long currentTimestamp(ConnectorSession session)
    {
        return packDateTimeWithZone(session.getStartTime(), session.getTimeZoneKey());
    }

    @Description("current timestamp without time zone")
    @ScalarFunction("localtimestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long localTimestamp(ConnectorSession session)
    {
        ISOChronology localChronology = getChronology(session.getTimeZoneKey());
        return localChronology.getZone().convertUTCToLocal(session.getStartTime());
    }

    @ScalarFunction("from_unixtime")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime)
    {
        return Math.round(unixTime * 1000);
    }

    @ScalarFunction("from_unixtime")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime, @SqlType(StandardTypes.BIGINT) long hoursOffset, @SqlType(StandardTypes.BIGINT) long minutesOffset)
    {
        return packDateTimeWithZone(Math.round(unixTime * 1000), (int) (hoursOffset * 60 + minutesOffset));
    }

    @ScalarFunction("from_unixtime")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long fromUnixTime(@SqlType(StandardTypes.DOUBLE) double unixTime, @SqlType("varchar(x)") Slice zoneId)
    {
        return packDateTimeWithZone(Math.round(unixTime * 1000), zoneId.toStringUtf8());
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
    public static Slice toISO8601FromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.dateTime()
                .withChronology(getChronology(session.getTimeZoneKey()));
        return utf8Slice(formatter.print(timestamp));
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
    public static Slice toISO8601FromDate(ConnectorSession session, @SqlType(StandardTypes.DATE) long date)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.date()
                .withChronology(UTC_CHRONOLOGY);
        return utf8Slice(formatter.print(DAYS.toMillis(date)));
    }

    @ScalarFunction("from_iso8601_timestamp")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long fromISO8601Timestamp(ConnectorSession session, @SqlType("varchar(x)") Slice iso8601DateTime)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser()
                .withChronology(getChronology(session.getTimeZoneKey()))
                .withOffsetParsed();
        return packDateTimeWithZone(parseDateTimeHelper(formatter, iso8601DateTime.toStringUtf8()));
    }

    @ScalarFunction("from_iso8601_date")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long fromISO8601Date(ConnectorSession session, @SqlType("varchar(x)") Slice iso8601DateTime)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.dateElementParser()
                .withChronology(UTC_CHRONOLOGY);
        DateTime dateTime = parseDateTimeHelper(formatter, iso8601DateTime.toStringUtf8());
        return MILLISECONDS.toDays(dateTime.getMillis());
    }

    @ScalarFunction(value = "at_timezone", hidden = true)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeAtTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone, @SqlType("varchar(x)") Slice zoneId)
    {
        return packDateTimeWithZone(unpackMillisUtc(timeWithTimeZone), zoneId.toStringUtf8());
    }

    @ScalarFunction(value = "at_timezone", hidden = true)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeAtTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long zoneOffset)
    {
        checkCondition((zoneOffset % 60_000) == 0, INVALID_FUNCTION_ARGUMENT, "Invalid time zone offset interval: interval contains seconds");
        int zoneOffsetMinutes = (int) (zoneOffset / 60_000);
        return packDateTimeWithZone(unpackMillisUtc(timeWithTimeZone), getTimeZoneKeyForOffset(zoneOffsetMinutes));
    }

    @ScalarFunction(value = "at_timezone", hidden = true)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampAtTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone, @SqlType("varchar(x)") Slice zoneId)
    {
        return packDateTimeWithZone(unpackMillisUtc(timestampWithTimeZone), zoneId.toStringUtf8());
    }

    @ScalarFunction(value = "at_timezone", hidden = true)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampAtTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long zoneOffset)
    {
        checkCondition((zoneOffset % 60_000) == 0, INVALID_FUNCTION_ARGUMENT, "Invalid time zone offset interval: interval contains seconds");
        int zoneOffsetMinutes = (int) (zoneOffset / 60_000);
        return packDateTimeWithZone(unpackMillisUtc(timestampWithTimeZone), getTimeZoneKeyForOffset(zoneOffsetMinutes));
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long truncateDate(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date)
    {
        long millis = getDateField(UTC_CHRONOLOGY, unit).roundFloor(DAYS.toMillis(date));
        return MILLISECONDS.toDays(millis);
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("date_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME)
    public static long truncateTime(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIME) long time)
    {
        return getTimeField(getChronology(session.getTimeZoneKey()), unit).roundFloor(time);
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
    public static long truncateTimestamp(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).roundFloor(timestamp);
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
    public static long addFieldValueDate(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DATE) long date)
    {
        long millis = getDateField(UTC_CHRONOLOGY, unit).add(DAYS.toMillis(date), toIntExact(value));
        return MILLISECONDS.toDays(millis);
    }

    @Description("add the specified amount of time to the given time")
    @LiteralParameters("x")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.TIME)
    public static long addFieldValueTime(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.TIME) long time)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        return modulo24Hour(chronology, getTimeField(chronology, unit).add(time, toIntExact(value)));
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
            ConnectorSession session,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).add(timestamp, toIntExact(value));
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
    public static long diffDate(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date1, @SqlType(StandardTypes.DATE) long date2)
    {
        return getDateField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(DAYS.toMillis(date2), DAYS.toMillis(date1));
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTime(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIME) long time1, @SqlType(StandardTypes.TIME) long time2)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        return getTimeField(chronology, unit).getDifferenceAsLong(time2, time1);
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
            ConnectorSession session,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp1,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp2)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).getDifferenceAsLong(timestamp2, timestamp1);
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
    public static long parseDatetime(ConnectorSession session, @SqlType("varchar(x)") Slice datetime, @SqlType("varchar(y)") Slice formatString)
    {
        try {
            return packDateTimeWithZone(parseDateTimeHelper(DateTimeFormat.forPattern(formatString.toStringUtf8())
                                                .withChronology(getChronology(session.getTimeZoneKey()))
                                                .withOffsetParsed()
                                                .withLocale(session.getLocale()),
                                                datetime.toStringUtf8()));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
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
    public static Slice formatDatetime(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        return formatDatetime(getChronology(session.getTimeZoneKey()), session.getLocale(), timestamp, formatString);
    }

    @Description("formats the given time by the given format")
    @ScalarFunction("format_datetime")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatDatetimeWithTimeZone(
            ConnectorSession session,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone,
            @SqlType("varchar(x)") Slice formatString)
    {
        return formatDatetime(unpackChronology(timestampWithTimeZone), session.getLocale(), unpackMillisUtc(timestampWithTimeZone), formatString);
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
    public static Slice dateFormat(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType("varchar(x)") Slice formatString)
    {
        return dateFormat(getChronology(session.getTimeZoneKey()), session.getLocale(), timestamp, formatString);
    }

    @ScalarFunction("date_format")
    @LiteralParameters("x")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateFormatWithTimeZone(
            ConnectorSession session,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone,
            @SqlType("varchar(x)") Slice formatString)
    {
        return dateFormat(unpackChronology(timestampWithTimeZone), session.getLocale(), unpackMillisUtc(timestampWithTimeZone), formatString);
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

    @Description("second of the minute of the given timestamp")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("second of the minute of the given time")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTime(@SqlType(StandardTypes.TIME) long time)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(time);
    }

    @Description("second of the minute of the given time")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTimeWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(unpackMillisUtc(time));
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
    public static long minuteFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).minuteOfHour().get(timestamp);
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
    public static long minuteFromTime(ConnectorSession session, @SqlType(StandardTypes.TIME) long time)
    {
        return getChronology(session.getTimeZoneKey()).minuteOfHour().get(time);
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
    public static long hourFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).hourOfDay().get(timestamp);
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
    public static long hourFromTime(ConnectorSession session, @SqlType(StandardTypes.TIME) long time)
    {
        return getChronology(session.getTimeZoneKey()).hourOfDay().get(time);
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
    public static long dayOfWeekFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfWeek().get(timestamp);
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
    public static long dayFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfMonth().get(timestamp);
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

    @Description("day of the year of the given timestamp")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfYear().get(timestamp);
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
    public static long weekFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).weekOfWeekyear().get(timestamp);
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
    public static long yearOfWeekFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).weekyear().get(timestamp);
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
    public static long monthFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().get(timestamp);
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
    public static long quarterFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return QUARTER_OF_YEAR.getField(getChronology(session.getTimeZoneKey())).get(timestamp);
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
    public static long yearFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).year().get(timestamp);
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
}
