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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.teradata.functions.dateformat.DateFormatParser;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.util.ThreadLocalCache;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormatter;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.type.TimestampOperators.castToDate;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackChronology;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TeradataDateFunctions
{
    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE = new ThreadLocalCache<Slice, DateTimeFormatter>(100)
    {
        @Override
        protected DateTimeFormatter load(Slice format)
        {
            String formatString = format.toStringUtf8();
            return DateFormatParser.createDateTimeFormatter(formatString);
        }
    };

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
                .withChronology(unpackChronology(timestampWithTimeZone))
                .withLocale(session.getLocale());

        return utf8Slice(formatter.print(unpackMillisUtc(timestampWithTimeZone)));
    }

    @Description("Converts a string to a DATE data type")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.DATE)
    public static long toDate(
            ConnectorSession session,
            @SqlType(StandardTypes.VARCHAR) Slice dateTime,
            @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return castToDate(session, parseMillis(session, dateTime, formatString));
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
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString)
                .withChronology(getChronology(session.getTimeZoneKey()))
                .withLocale(session.getLocale());

        try {
            return formatter.parseMillis(dateTime.toString(UTF_8));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
