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
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.fasterxml.jackson.core.JsonGenerator;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.scalar.JsonOperators.JSON_FACTORY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZoneForLegacyTimestamp;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithoutTimeZoneForLegacyTimestamp;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.JsonUtil.createJsonGenerator;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;

public final class TimestampOperatorsForLegacyTimestamp
{
    private TimestampOperatorsForLegacyTimestamp()
    {
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromDate(ConnectorSession session, @SqlType(StandardTypes.DATE) long value)
    {
        long utcMillis = TimeUnit.DAYS.toMillis(value);

        // date is encoded as milliseconds at midnight in UTC
        // convert to midnight if the session timezone
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        return utcMillis - chronology.getZone().getOffset(utcMillis);
    }

    @ScalarFunction("date")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DATE)
    public static long castToDate(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        // round down the current timestamp to days
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        long date = chronology.dayOfYear().roundFloor(value);
        // date is currently midnight in timezone of the session
        // convert to UTC
        long millis = date + chronology.getZone().getOffset(date);
        return TimeUnit.MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromTime(@SqlType(StandardTypes.TIME) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME)
    public static long castToTime(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        return modulo24Hour(getChronology(session.getTimeZoneKey()), value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromTimeWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        return unpackMillisUtc(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long castToTimeWithTimeZone(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        int timeMillis = modulo24Hour(getChronology(session.getTimeZoneKey()), value);
        return packDateTimeWithZone(timeMillis, session.getTimeZoneKey());
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return unpackMillisUtc(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long castToTimestampWithTimeZone(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        return packDateTimeWithZone(value, session.getTimeZoneKey());
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        return utf8Slice(printTimestampWithoutTimeZoneForLegacyTimestamp(session.getTimeZoneKey(), value));
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromSlice(ConnectorSession session, @SqlType("varchar(x)") Slice value)
    {
        try {
            return parseTimestampWithoutTimeZoneForLegacyTimestamp(session.getTimeZoneKey(), trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castToJson(ConnectorSession session, @SqlType(TIMESTAMP) long value)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(25);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeString(printTimestampWithoutTimeZoneForLegacyTimestamp(session.getTimeZoneKey(), value));
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", value, JSON));
        }
    }
}
