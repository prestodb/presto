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
package com.facebook.presto.teradata.functions;

import com.facebook.airlift.concurrent.ThreadLocalCache;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.zone.ZoneRulesException;
import java.util.Locale;

import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKeys;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.teradata.functions.dateformat.DateFormatParser.Mode.FORMATTER;
import static com.facebook.presto.teradata.functions.dateformat.DateFormatParser.Mode.PARSER;
import static com.facebook.presto.teradata.functions.dateformat.DateFormatParser.createDateTimeFormatter;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class TeradataDateFunctions
{
    // Separate DateTimeFormatter instance caches (for formatting and parsing) in order to keep support the following use cases:
    // 1. Do not require leading zero for parsing two position date fields (MM, DD, HH, HH24, MI, SS)
    //    e.g. allow "to_timestamp('1988/4/8 2:3:4','yyyy/mm/dd hh24:mi:ss')"
    // 2. Always add leading zero for formatting single valued two position date fields (MM, DD, HH, HH24, MI, SS)
    //    e.g. evaluate "to_char(TIMESTAMP '1988-4-8 2:3:4','yyyy/mm/dd hh24:mi:ss')" to "1988/04/08 02:03:04"

    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_PARSER_CACHE =
            new ThreadLocalCache<>(100, format -> createDateTimeFormatter(format.toStringUtf8(), PARSER));

    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE =
            new ThreadLocalCache<>(100, format -> createDateTimeFormatter(format.toStringUtf8(), FORMATTER));

    private static final Logger LOG = Logger.get(TeradataDateFunctions.class);

    private static final ZoneId[] ZONE_IDS = new ZoneId[MAX_TIME_ZONE_KEY + 1];

    static {
        for (TimeZoneKey timeZoneKey : getTimeZoneKeys()) {
            try {
                ZONE_IDS[timeZoneKey.getKey()] = ZoneId.of(timeZoneKey.getId());
            }
            catch (ZoneRulesException ex) {
                // Ignore this exception so that older JRE versions that might not support newer time-zone identifiers do not fail
                LOG.error(ex, "Failed to obtain an instance of ZoneId for %s, ignoring this exception", timeZoneKey.getId());
            }
        }
    }

    private TeradataDateFunctions()
    {
    }

    @Description("Formats a timestamp")
    @ScalarFunction("to_char")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toChar(
            SqlFunctionProperties properties,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone,
            @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(IsoChronology.INSTANCE)
                .withZone(ZONE_IDS[unpackZoneKey(timestampWithTimeZone).getKey()])
                .withLocale(properties.getSessionLocale());

        return utf8Slice(formatter.format(Instant.ofEpochMilli((unpackMillisUtc(timestampWithTimeZone)))));
    }

    @Description("Converts a string to a DATE data type")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.DATE)
    public static long toDate(SqlFunctionProperties properties, @SqlType(StandardTypes.VARCHAR) Slice dateTime, @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        try {
            long millis = parseMillis(UTC_KEY, properties.getSessionLocale(), dateTime, formatString);
            return MILLISECONDS.toDays(millis);
        }
        catch (Throwable t) {
            throwIfInstanceOf(t, Error.class);
            throwIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    @Description("Converts a string to a TIMESTAMP data type")
    @ScalarFunction("to_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long toTimestamp(
            SqlFunctionProperties properties,
            @SqlType(StandardTypes.VARCHAR) Slice dateTime,
            @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return parseMillis(properties, dateTime, formatString);
    }

    private static long parseMillis(SqlFunctionProperties properties, Slice dateTime, Slice formatString)
    {
        TimeZoneKey timeZoneKey = UTC_KEY;
        if (properties.isLegacyTimestamp()) {
            timeZoneKey = properties.getTimeZoneKey();
        }
        return parseMillis(timeZoneKey, properties.getSessionLocale(), dateTime, formatString);
    }

    private static long parseMillis(TimeZoneKey timeZoneKey, Locale locale, Slice dateTime, Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_PARSER_CACHE.get(formatString)
                .withChronology(IsoChronology.INSTANCE)
                .withZone(ZONE_IDS[timeZoneKey.getKey()])
                .withLocale(locale);

        try {
            return ZonedDateTime.parse(dateTime.toString(UTF_8), formatter).toInstant().toEpochMilli();
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
