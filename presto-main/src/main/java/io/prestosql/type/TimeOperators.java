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
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.AbstractLongType;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

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
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.util.DateTimeUtils.parseTimeWithoutTimeZone;
import static io.prestosql.util.DateTimeUtils.printTimeWithoutTimeZone;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;

public final class TimeOperators
{
    private TimeOperators()
    {
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long subtract(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return left - right;
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.TIME) long right)
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
        if (session.isLegacyTimestamp()) {
            return packDateTimeWithZone(value, session.getTimeZoneKey());
        }
        else {
            ISOChronology localChronology = getChronology(session.getTimeZoneKey());

            // This cast does treat TIME as wall time in session TZ. This means that in order to get
            // its UTC representation we need to shift the value by the offset of TZ.
            // We use value offset in this place to be sure that we will have same hour represented
            // in TIME WITH TIME ZONE. Calculating real TZ offset will happen when really required.
            // This is done due to inadequate TIME WITH TIME ZONE representation.
            return packDateTimeWithZone(localChronology.getZone().convertLocalToUTC(value, false), session.getTimeZoneKey());
        }
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
        return castToTimeWithTimeZone(session, value);
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(ConnectorSession session, @SqlType(StandardTypes.TIME) long value)
    {
        if (session.isLegacyTimestamp()) {
            return utf8Slice(printTimeWithoutTimeZone(session.getTimeZoneKey(), value));
        }
        else {
            return utf8Slice(printTimeWithoutTimeZone(value));
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME)
    public static long castFromSlice(ConnectorSession session, @SqlType("varchar(x)") Slice value)
    {
        try {
            if (session.isLegacyTimestamp()) {
                return parseTimeWithoutTimeZone(session.getTimeZoneKey(), value.toStringUtf8());
            }
            else {
                return parseTimeWithoutTimeZone(value.toStringUtf8());
            }
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to time: " + value.toStringUtf8(), e);
        }
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.TIME) long value)
    {
        return AbstractLongType.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType(StandardTypes.TIME) long value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class TimeDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.TIME) long left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.TIME) long right,
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
                @BlockPosition @SqlType(value = StandardTypes.TIME, nativeContainerType = long.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.TIME, nativeContainerType = long.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return notEqual(TIME.getLong(left, leftPosition), TIME.getLong(right, rightPosition));
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.TIME) long value, @IsNull boolean isNull)
    {
        return isNull;
    }
}
