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
package com.facebook.presto.maxcompute.util;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimePrinter;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateTimeEncoding.updateMillisUtc;
import static com.facebook.presto.operator.scalar.UrlFunctions.urlExtractParameter;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackChronology;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class MaxComputeBuiltinFunctions
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    private MaxComputeBuiltinFunctions()
    {
    }

    private static final LoadingCache<Slice, DateTimeFormatter> FORMATTER_CACHE = CacheBuilder.newBuilder()
            .maximumSize(100)
            .build(new CacheLoader<Slice, DateTimeFormatter>() {
                @Override public DateTimeFormatter load(Slice format) throws Exception
                {
                    String formatString = format.toStringUtf8();
                    DateTimePrinter timestampWithoutTimeZonePrinter = DateTimeFormat.forPattern(formatString).getPrinter();
                    return new DateTimeFormatterBuilder()
                            .append(timestampWithoutTimeZonePrinter)
                            .toFormatter();
                }
            });

    @Description("Formats a timestamp")
    @ScalarFunction("to_char")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice to_char(@SqlType(StandardTypes.TIME) long timestamp, @SqlType(StandardTypes.VARCHAR)
            Slice formatString)
    {
        String format = formatString.toStringUtf8();
        format = transformDatetimeUnitFormat(format);
        formatString = utf8Slice(format);
        DateTimeFormatter formatter = null;
        try {
            formatter = FORMATTER_CACHE.get(formatString);
        }
        catch (ExecutionException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
        return utf8Slice(formatter.print(timestamp));
    }

    @Description("Formats a timestamp")
    @ScalarFunction("to_char")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice timeStampToChar(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.VARCHAR)
            Slice formatString)
    {
        String format = formatString.toStringUtf8();
        format = transformDatetimeUnitFormat(format);
        formatString = utf8Slice(format);
        DateTimeFormatter formatter = null;
        try {
            if (properties.isLegacyTimestamp()) {
                formatter = FORMATTER_CACHE.get(formatString).withChronology(getChronology(properties.getTimeZoneKey()));
            }
            else {
                formatter = FORMATTER_CACHE.get(formatString).withChronology(UTC_CHRONOLOGY);
            }
        }
        catch (ExecutionException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
        return utf8Slice(formatter.print(timestamp));
    }

    private static String transformDatetimeUnitFormat(String unitStr)
    {
        return unitStr.replace("mm", "MM")
                .replace("mi", "mm");
    }

    @Description("to_char function")
    @ScalarFunction("to_char")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice to_char(@SqlType(StandardTypes.BIGINT) long value)
    {
        return utf8Slice(String.valueOf(value));
    }

    @Description("to_char function")
    @ScalarFunction("to_char")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice to_char(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return utf8Slice(String.valueOf(value));
    }

    @Description("to_char function")
    @ScalarFunction("to_char")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice to_char(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return utf8Slice(String.valueOf(value));
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("instr")
    @SqlType(StandardTypes.BIGINT)
    public static long stringPosition(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(
            StandardTypes.VARCHAR) Slice substring)
    {
        return StringFunctions.stringPosition(string, substring);
    }

    @SqlNullable
    @ScalarFunction("parse_url")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice parseUrlPart(@SqlType(StandardTypes.VARCHAR) Slice url, @SqlType(
            StandardTypes.VARCHAR) Slice part)
    {
        String partStr = part.toStringUtf8();
        URI uri = parseUrl(url);
        String result;
        if (uri == null) {
            result = null;
        }
        else if (partStr.equalsIgnoreCase("host")) {
            result = uri.getHost();
        }
        else if (partStr.equalsIgnoreCase("path")) {
            result = uri.getPath();
        }
        else if (partStr.equalsIgnoreCase("query")) {
            result = uri.getQuery();
        }
        else if (partStr.equalsIgnoreCase("ref")) {
            result = uri.getFragment();
        }
        else if (partStr.equalsIgnoreCase("protocol")) {
            result = uri.getScheme();
        }
        else if (partStr.equalsIgnoreCase("authority")) {
            result = uri.getAuthority();
        }
        else if (partStr.equalsIgnoreCase("userinfo")) {
            result = uri.getUserInfo();
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupport parse url part: " + partStr);
        }
        return (uri == null) ? null : utf8Slice(nullToEmpty(result));
    }

    @SqlNullable
    @ScalarFunction("parse_url")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice parseQueryParameter(@SqlType(StandardTypes.VARCHAR) Slice url, @SqlType(
            StandardTypes.VARCHAR) Slice part, @SqlType(StandardTypes.VARCHAR) Slice key)
    {
        String partStr = part.toStringUtf8();
        if (partStr.equalsIgnoreCase("query")) {
            return urlExtractParameter(url, key);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupport parse url part: " + partStr);
        }
    }

    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long dateDiff(@SqlType(StandardTypes.TIMESTAMP) long time1, @SqlType(
            StandardTypes.TIMESTAMP) long time2, @SqlType(StandardTypes.VARCHAR) Slice part)
    {
        ChronoUnit chronoUnit = getChronoUnit(part);
        LocalDateTime localDateTime1 = truncate(LocalDateTime.ofInstant(Instant.ofEpochMilli(time1), ZoneId.of("UTC")), chronoUnit);
        LocalDateTime localDateTime2 = truncate(LocalDateTime.ofInstant(Instant.ofEpochMilli(time2), ZoneId.of("UTC")), chronoUnit);
        return chronoUnit.between(localDateTime2, localDateTime1);
    }

    @Description("add the specified amount of date to the given date")
    @LiteralParameters("x")
    @ScalarFunction("dateadd")
    @SqlType(StandardTypes.DATE)
    public static long addFieldValueDate(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.BIGINT) long value, @SqlType("varchar(x)") Slice unit)
    {
        long millis = getDateTimeField(UTC_CHRONOLOGY, unit).add(DAYS.toMillis(date), toIntExact(value));
        return MILLISECONDS.toDays(millis);
    }

    @Description("add the specified amount of time to the given time")
    @LiteralParameters("x")
    @ScalarFunction("dateadd")
    @SqlType(StandardTypes.TIME)
    public static long addFieldValueTime(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME) long time, @SqlType(StandardTypes.BIGINT) long value, @SqlType("varchar(x)") Slice unit)
    {
        if (properties.isLegacyTimestamp()) {
            ISOChronology chronology = getChronology(properties.getTimeZoneKey());
            return modulo24Hour(chronology, getDateTimeField(chronology, unit).add(time, toIntExact(value)));
        }

        return modulo24Hour(getDateTimeField(UTC_CHRONOLOGY, unit).add(time, toIntExact(value)));
    }

    @Description("add the specified amount of time to the given time")
    @LiteralParameters("x")
    @ScalarFunction("dateadd")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long addFieldValueTimeWithTimeZone(
            @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone, @SqlType(StandardTypes.BIGINT) long value, @SqlType("varchar(x)") Slice unit)
    {
        ISOChronology chronology = unpackChronology(timeWithTimeZone);
        long millis = modulo24Hour(chronology, getDateTimeField(chronology, unit).add(unpackMillisUtc(timeWithTimeZone), toIntExact(value)));
        return updateMillisUtc(millis, timeWithTimeZone);
    }

    @Description("add the specified amount of time to the given timestamp")
    @LiteralParameters("x")
    @ScalarFunction("dateadd")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long addFieldValueTimestamp(
            SqlFunctionProperties properties,
            @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.BIGINT) long value, @SqlType("varchar(x)") Slice unit)
    {
        if (properties.isLegacyTimestamp()) {
            return getDateTimeField(getChronology(properties.getTimeZoneKey()), unit).add(timestamp, toIntExact(value));
        }

        return getDateTimeField(UTC_CHRONOLOGY, unit).add(timestamp, toIntExact(value));
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("datetrunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long truncateDate(@SqlType(StandardTypes.DATE) long date, @SqlType("varchar(x)") Slice unit)
    {
        long millis = getDateTimeField(UTC_CHRONOLOGY, unit).roundFloor(DAYS.toMillis(date));
        return MILLISECONDS.toDays(millis);
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("datetrunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME)
    public static long truncateTime(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME) long time, @SqlType("varchar(x)") Slice unit)
    {
        if (properties.isLegacyTimestamp()) {
            return getDateTimeField(getChronology(properties.getTimeZoneKey()), unit).roundFloor(time);
        }
        else {
            return getDateTimeField(UTC_CHRONOLOGY, unit).roundFloor(time);
        }
    }

    @Description("truncate to the specified precision")
    @ScalarFunction("datetrunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long truncateTimeWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long timeWithTimeZone, @SqlType("varchar(x)") Slice unit)
    {
        long millis = getDateTimeField(unpackChronology(timeWithTimeZone), unit).roundFloor(unpackMillisUtc(timeWithTimeZone));
        return updateMillisUtc(millis, timeWithTimeZone);
    }

    @Description("truncate to the specified precision in the session timezone")
    @ScalarFunction("datetrunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long truncateTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType("varchar(x)") Slice unit)
    {
        if (properties.isLegacyTimestamp()) {
            return getDateTimeField(getChronology(properties.getTimeZoneKey()), unit).roundFloor(timestamp);
        }
        else {
            return getDateTimeField(UTC_CHRONOLOGY, unit).roundFloor(timestamp);
        }
    }

    @Description("truncate to the specified precision")
    @ScalarFunction("datetrunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long truncateTimestampWithTimezone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone, @SqlType("varchar(x)") Slice unit)
    {
        long millis = getDateTimeField(unpackChronology(timestampWithTimeZone), unit).roundFloor(unpackMillisUtc(timestampWithTimeZone));
        return updateMillisUtc(millis, timestampWithTimeZone);
    }

    @ScalarFunction("datepart")
    @SqlType(StandardTypes.BIGINT)
    public static long dateTimePart(@SqlType(StandardTypes.TIMESTAMP) long time, @SqlType(StandardTypes.VARCHAR) Slice part)
    {
        ChronoField chronoField = getChronoField(part);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.of("UTC"));
        return part(localDateTime, chronoField);
    }

    @ScalarFunction("datepart")
    @SqlType(StandardTypes.BIGINT)
    public static long datePart(@SqlType(StandardTypes.DATE) long time, @SqlType(StandardTypes.VARCHAR) Slice part)
    {
        ChronoField chronoField = getChronoField(part);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(DAYS.toMillis(time)), ZoneId.of("UTC"));
        return part(localDateTime, chronoField);
    }

    @ScalarFunction("getdate")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long getDateTime(SqlFunctionProperties properties)
    {
        return properties.getSessionStartTime();
    }

    public static ChronoUnit getChronoUnit(Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "ff3":
                return ChronoUnit.MILLIS;
            case "ss":
                return ChronoUnit.SECONDS;
            case "mi":
                return ChronoUnit.MINUTES;
            case "hh":
                return ChronoUnit.HOURS;
            case "dd":
                return ChronoUnit.DAYS;
            case "mm":
                return ChronoUnit.MONTHS;
            case "yyyy":
                return ChronoUnit.YEARS;
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid datetime part");
    }

    public static ChronoField getChronoField(Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "ff3":
                return ChronoField.MILLI_OF_SECOND;
            case "ss":
                return ChronoField.SECOND_OF_MINUTE;
            case "mi":
                return ChronoField.MINUTE_OF_HOUR;
            case "hh":
                return ChronoField.HOUR_OF_DAY;
            case "dd":
                return ChronoField.DAY_OF_MONTH;
            case "mm":
                return ChronoField.MONTH_OF_YEAR;
            case "yyyy":
                return ChronoField.YEAR;
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid datetime part");
    }

    public static DateTimeField getDateTimeField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "ff3":
                return chronology.millisOfSecond();
            case "ss":
                return chronology.secondOfMinute();
            case "mi":
                return chronology.minuteOfHour();
            case "hh":
                return chronology.hourOfDay();
            case "dd":
                return chronology.dayOfMonth();
            case "mm":
                return chronology.monthOfYear();
            case "yyyy":
                return chronology.year();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Timestamp field");
    }

    public static LocalDateTime truncate(LocalDateTime localDateTime, ChronoUnit unit)
    {
        switch (unit) {
            case YEARS:
                return localDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
            case MONTHS:
                return localDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
        }
        return localDateTime.truncatedTo(unit);
    }

    public static long part(LocalDateTime localDateTime, ChronoField field)
    {
        return localDateTime.getLong(field);
    }

    @Nullable
    private static URI parseUrl(Slice url)
    {
        try {
            return new URI(url.toStringUtf8());
        }
        catch (URISyntaxException e) {
            return null;
        }
    }
}
