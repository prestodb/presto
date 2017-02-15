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
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import org.joda.time.chrono.ISOChronology;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;

import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.util.DateTimeUtils.printTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static io.airlift.slice.Slices.utf8Slice;

public final class TimeWithTimeZoneOperators
{
    private static final long REFERENCE_TIMESTAMP_UTC = System.currentTimeMillis();

    private TimeWithTimeZoneOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) == unpackMillisUtc(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) != unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) < unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) <= unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) > unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) >= unpackMillisUtc(right);
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long min, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long max)
    {
        return unpackMillisUtc(min) <= unpackMillisUtc(value) && unpackMillisUtc(value) <= unpackMillisUtc(max);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME)
    public static long castToTime(ConnectorSession session, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        // This is exactly the same operation as for TIME WITH TIME ZONE -> TIMESTAMP, as the representations
        // of those types are aligned in range that is covered by TIME WITH TIME ZONE.
        return castToTimestamp(session, value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castToTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        if (session.isLegacyTimestamp()) {
            return unpackMillisUtc(value);
        }
        else {
            // FIXME This is hack that we need to use as the timezone interpretation depends on date (not only on time)
            long currentMillisOfDay = ChronoField.MILLI_OF_DAY.getFrom(Instant.ofEpochMilli(REFERENCE_TIMESTAMP_UTC).atZone(ZoneOffset.UTC));
            long timeMillisUtcInCurrentDay = REFERENCE_TIMESTAMP_UTC - currentMillisOfDay + unpackMillisUtc(value);

            ISOChronology chronology = getChronology(unpackZoneKey(value));
            return unpackMillisUtc(value) + chronology.getZone().getOffset(timeMillisUtcInCurrentDay);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long castToTimestampWithTimeZone(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        return utf8Slice(printTimeWithTimeZone(value));
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        return AbstractLongType.hash(unpackMillisUtc(value));
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left,
            @IsNull boolean leftNull,
            @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right,
            @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return notEqual(left, right);
    }
}
