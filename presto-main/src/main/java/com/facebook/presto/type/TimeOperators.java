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

import com.facebook.presto.operator.scalar.annotations.ScalarOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithoutTimeZone;
import static com.facebook.presto.util.DateTimeUtils.printTimeWithoutTimeZone;
import static io.airlift.slice.Slices.utf8Slice;

public final class TimeOperators
{
    private TimeOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.TIME) long value, @SqlType(StandardTypes.TIME) long min, @SqlType(StandardTypes.TIME) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long castToTimeWithTimeZone(ConnectorSession session, @SqlType(StandardTypes.TIME) long value)
    {
        return packDateTimeWithZone(value, session.getTimeZoneKey());
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castToTimestamp(@SqlType(StandardTypes.TIME) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long castToTimestampWithTimeZone(ConnectorSession session, @SqlType(StandardTypes.TIME) long value)
    {
        return packDateTimeWithZone(value, session.getTimeZoneKey());
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToSlice(ConnectorSession session, @SqlType(StandardTypes.TIME) long value)
    {
        return utf8Slice(printTimeWithoutTimeZone(session.getTimeZoneKey(), value));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME)
    public static long castFromSlice(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        try {
            return parseTimeWithoutTimeZone(session.getTimeZoneKey(), value.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to time: " + value.toStringUtf8(), e);
        }
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.TIME) long value)
    {
        return value;
    }
}
