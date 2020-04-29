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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.AbstractLongType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import org.joda.time.chrono.ISOChronology;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.printTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static io.airlift.slice.Slices.utf8Slice;

public final class TimeWithTimeZoneOperators
{
    private static final long REFERENCE_TIMESTAMP_UTC = System.currentTimeMillis();

    private TimeWithTimeZoneOperators()
    {
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long subtract(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) - unpackMillisUtc(right);
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return unpackMillisUtc(left) == unpackMillisUtc(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
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
    public static long castToTime(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        // This is exactly the same operation as for TIME WITH TIME ZONE -> TIMESTAMP, as the representations
        // of those types are aligned in range that is covered by TIME WITH TIME ZONE.
        return castToTimestamp(properties, value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castToTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        if (properties.isLegacyTimestamp()) {
            return unpackMillisUtc(value);
        }
        else {
            // This is hack that we need to use as the timezone interpretation depends on date (not only on time)
            // TODO remove REFERENCE_TIMESTAMP_UTC when removing support for political time zones in TIME WIT TIME ZONE
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

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long castFromSlice(@SqlType("varchar(x)") Slice value)
    {
        return parseTimeWithTimeZone(value.toStringUtf8());
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        return AbstractLongType.hash(unpackMillisUtc(value));
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value)
    {
        return XxHash64.hash(unpackMillisUtc(value));
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class TimeWithTimeZoneDistinctFromOperator
    {
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

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = StandardTypes.TIME_WITH_TIME_ZONE, nativeContainerType = long.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.TIME_WITH_TIME_ZONE, nativeContainerType = long.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) && right.isNull(rightPosition)) {
                return false;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return notEqual(TIME_WITH_TIME_ZONE.getLong(left, leftPosition), TIME_WITH_TIME_ZONE.getLong(right, rightPosition));
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long value, @IsNull boolean isNull)
    {
        return isNull;
    }
}
