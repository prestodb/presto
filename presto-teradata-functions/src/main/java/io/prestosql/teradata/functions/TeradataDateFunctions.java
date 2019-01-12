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
package io.prestosql.teradata.functions;

import io.airlift.concurrent.ThreadLocalCache;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimeZoneKey;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKeys;
import static io.prestosql.teradata.functions.dateformat.DateFormatParser.createDateTimeFormatter;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class TeradataDateFunctions
{
    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE =
            new ThreadLocalCache<>(100, format -> createDateTimeFormatter(format.toStringUtf8()));

    private static final ISOChronology[] CHRONOLOGIES = new ISOChronology[MAX_TIME_ZONE_KEY + 1];

    static {
        for (TimeZoneKey timeZoneKey : getTimeZoneKeys()) {
            DateTimeZone dateTimeZone = DateTimeZone.forID(timeZoneKey.getId());
            CHRONOLOGIES[timeZoneKey.getKey()] = ISOChronology.getInstance(dateTimeZone);
        }
    }

    private TeradataDateFunctions()
    {
    }

    @Description("Formats a timestamp")
    @ScalarFunction("to_char")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toChar(
            ConnectorSession session,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone,
            @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(CHRONOLOGIES[unpackZoneKey(timestampWithTimeZone).getKey()])
                .withLocale(session.getLocale());

        return utf8Slice(formatter.print(unpackMillisUtc(timestampWithTimeZone)));
    }

    @Description("Converts a string to a DATE data type")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.DATE)
    public static long toDate(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice dateTime, @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        try {
            long millis = parseMillis(UTC_KEY, session.getLocale(), dateTime, formatString);
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
            ConnectorSession session,
            @SqlType(StandardTypes.VARCHAR) Slice dateTime,
            @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return parseMillis(session, dateTime, formatString);
    }

    private static long parseMillis(ConnectorSession session, Slice dateTime, Slice formatString)
    {
        TimeZoneKey timeZoneKey = UTC_KEY;
        if (session.isLegacyTimestamp()) {
            timeZoneKey = session.getTimeZoneKey();
        }
        return parseMillis(timeZoneKey, session.getLocale(), dateTime, formatString);
    }

    private static long parseMillis(TimeZoneKey timeZoneKey, Locale locale, Slice dateTime, Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(CHRONOLOGIES[timeZoneKey.getKey()])
                .withLocale(locale);

        try {
            return formatter.parseMillis(dateTime.toString(UTF_8));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
