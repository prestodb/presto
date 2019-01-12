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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.AbstractLongType;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import java.util.concurrent.TimeUnit;

import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.BETWEEN;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;
import static io.prestosql.spi.function.OperatorType.XX_HASH_64;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.type.DateTimeOperators.modulo24Hour;
import static io.prestosql.util.DateTimeUtils.parseTimestampWithTimeZone;
import static io.prestosql.util.DateTimeUtils.printTimestampWithTimeZone;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;
import static io.prestosql.util.DateTimeZoneIndex.unpackChronology;

public final class TimestampWithTimeZoneOperators
{
    private TimestampWithTimeZoneOperators()
    {
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long subtract(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) - unpackMillisUtc(right);
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) == unpackMillisUtc(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
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
        // round down the current timestamp to days
        ISOChronology chronology = unpackChronology(value);
        long date = chronology.dayOfYear().roundFloor(unpackMillisUtc(value));
        // date is currently midnight in timezone of the original value
        // convert to UTC
        long millis = date + chronology.getZone().getOffset(date);
        return TimeUnit.MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME)
    public static long castToTime(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        if (session.isLegacyTimestamp()) {
            return modulo24Hour(unpackChronology(value), unpackMillisUtc(value));
        }
        else {
            return modulo24Hour(castToTimestamp(session, value));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long castToTimeWithTimeZone(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        if (session.isLegacyTimestamp()) {
            int millis = modulo24Hour(unpackChronology(value), unpackMillisUtc(value));
            return packDateTimeWithZone(millis, unpackZoneKey(value));
        }
        else {
            long millis = modulo24Hour(castToTimestamp(session, value));
            ISOChronology localChronology = unpackChronology(value);

            // This cast does treat TIME as wall time in given TZ. This means that in order to get
            // its UTC representation we need to shift the value by the offset of TZ.
            // We use value offset in this place to be sure that we will have same hour represented
            // in TIME WITH TIME ZONE. Calculating real TZ offset will happen when really required.
            // This is done due to inadequate TIME WITH TIME ZONE representation.
            return packDateTimeWithZone(millis - localChronology.getZone().getOffset(millis), unpackZoneKey(value));
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castToTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        if (session.isLegacyTimestamp()) {
            return unpackMillisUtc(value);
        }
        else {
            ISOChronology chronology = getChronology(unpackZoneKey(value));
            return chronology.getZone().convertUTCToLocal(unpackMillisUtc(value));
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return utf8Slice(printTimestampWithTimeZone(value));
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long castFromSlice(ConnectorSession session, @SqlType("varchar(x)") Slice value)
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
        return AbstractLongType.hash(unpackMillisUtc(value));
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value)
    {
        return XxHash64.hash(unpackMillisUtc(value));
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class TimestampWithTimeZoneDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right,
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

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = StandardTypes.TIMESTAMP_WITH_TIME_ZONE, nativeContainerType = long.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.TIMESTAMP_WITH_TIME_ZONE, nativeContainerType = long.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return notEqual(TIMESTAMP_WITH_TIME_ZONE.getLong(left, leftPosition), TIMESTAMP_WITH_TIME_ZONE.getLong(right, rightPosition));
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long value, @IsNull boolean isNull)
    {
        return isNull;
    }
}
