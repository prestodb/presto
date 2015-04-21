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
package com.teradata.presto.functions;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.util.ThreadLocalCache;
import com.teradata.presto.functions.dateformat.TeradataDateFormatterBuilder;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormatter;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class TeradataDateFunctions
{
    private static final TeradataDateFormatterBuilder formatterBuilder = new TeradataDateFormatterBuilder();
    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE = new ThreadLocalCache<Slice, DateTimeFormatter>(100)
    {
        @Override
        protected DateTimeFormatter load(Slice format)
        {
            String formatString = format.toString(UTF_8);
            return formatterBuilder.createDateTimeFormatter(formatString);
        }
    };

    private TeradataDateFunctions()
    {
    }

    @Description("Converts string_expr to a DATE data type. Teradata extension to the ANSI SQL-2003 standard")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.DATE)
    public static long toDate(ConnectorSession session,
                              @SqlType(StandardTypes.VARCHAR) Slice dateTime,
                              @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return MILLISECONDS.toDays(parseMilis(session, dateTime, formatString));
    }

    @Description("Converts string_expr to a TIMESTAMP data type. Teradata extension to the ANSI SQL-2003 standard")
    @ScalarFunction("to_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long toTimestamp(ConnectorSession session,
                                   @SqlType(StandardTypes.VARCHAR) Slice dateTime,
                                   @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        return parseMilis(session, dateTime, formatString);
    }

    private static long parseMilis(ConnectorSession session, Slice dateTime, Slice formatString)
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
