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
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.timestampWithTimeZoneToDate;
import static com.facebook.presto.util.DateTimeUtils.timestampWithTimeZoneToTime;
import static com.facebook.presto.util.DateTimeUtils.timestampWithTimeZoneToTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.timestampWithTimeZoneToTimestampWithoutTimeZone;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;

public final class TimestampWithTimeZoneOperators
{
    private TimestampWithTimeZoneOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) == unpackMillisUtc(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) != unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) < unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) <= unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) > unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) >= unpackMillisUtc(right);
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long min,
            @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long max)
    {
        return unpackMillisUtc(min) <= unpackMillisUtc(value) && unpackMillisUtc(value) <= unpackMillisUtc(max);
    }

    @ScalarFunction("date")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DATE)
    public static long castToDate(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return timestampWithTimeZoneToDate(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME)
    public static long castToTime(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return timestampWithTimeZoneToTime(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long castToTimeWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return timestampWithTimeZoneToTimeWithTimeZone(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castToTimestamp(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return timestampWithTimeZoneToTimestampWithoutTimeZone(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToSlice(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return utf8Slice(printTimestampWithTimeZone(value));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long castFromSlice(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        try {
            return parseTimestampWithTimeZone(session.getTimeZoneKey(), trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp with time zone: " + value.toStringUtf8(), e);
        }
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return unpackMillisUtc(value);
    }
}
