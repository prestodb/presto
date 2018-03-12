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
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.spi.type.StandardTypes.JSON;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZoneForFixedTimestamp;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithoutTimeZoneForFixedTimestamp;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.JsonUtil.createJsonGenerator;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;

public final class TimestampOperatorsForFixedTimestamp
{
    private TimestampOperatorsForFixedTimestamp()
    {
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromDate(@SqlType(StandardTypes.DATE) long value)
    {
        return TimeUnit.DAYS.toMillis(value);
    }

    @ScalarFunction("date")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DATE)
    public static long castToDate(@SqlType(StandardTypes.TIMESTAMP) long value)
    {
        return TimeUnit.MILLISECONDS.toDays(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromTime(ConnectorSession session, @SqlType(StandardTypes.TIME) long value)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        return chronology.getZone().convertUTCToLocal(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME)
    public static long castToTime(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        return modulo24Hour(chronology, chronology.getZone().convertLocalToUTC(value, false));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromTimeWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        ISOChronology chronology = getChronology(unpackZoneKey(value));
        return chronology.getZone().convertUTCToLocal(unpackMillisUtc(value));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long castToTimeWithTimeZone(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        int timeMillis = modulo24Hour(chronology, chronology.getZone().convertLocalToUTC(value, false));
        return packDateTimeWithZone(timeMillis, session.getTimeZoneKey());
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        ISOChronology chronology = getChronology(unpackZoneKey(value));
        return chronology.getZone().convertUTCToLocal(unpackMillisUtc(value));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long castToTimestampWithTimeZone(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        return packDateTimeWithZone(chronology.getZone().convertLocalToUTC(value, false), session.getTimeZoneKey());
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(@SqlType(StandardTypes.TIMESTAMP) long value)
    {
        return utf8Slice(printTimestampWithoutTimeZoneForFixedTimestamp(value));
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromSlice(@SqlType("varchar(x)") Slice value)
    {
        try {
            return parseTimestampWithoutTimeZoneForFixedTimestamp(trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(JSON)
    public static Slice castToJson(@SqlType(TIMESTAMP) long value)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(25);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeString(printTimestampWithoutTimeZoneForFixedTimestamp(value));
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", value, JSON));
        }
    }
}
