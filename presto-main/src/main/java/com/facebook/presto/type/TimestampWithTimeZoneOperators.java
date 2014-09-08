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

import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.chrono.ISOChronology;

import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackChronology;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TimestampWithTimeZoneOperators
{
    private TimestampWithTimeZoneOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(BooleanType.NAME)
    public static boolean equal(@SqlType(TimestampWithTimeZoneType.NAME) long left, @SqlType(TimestampWithTimeZoneType.NAME) long right)
    {
        return unpackMillisUtc(left) == unpackMillisUtc(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(BooleanType.NAME)
    public static boolean notEqual(@SqlType(TimestampWithTimeZoneType.NAME) long left, @SqlType(TimestampWithTimeZoneType.NAME) long right)
    {
        return unpackMillisUtc(left) != unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(BooleanType.NAME)
    public static boolean lessThan(@SqlType(TimestampWithTimeZoneType.NAME) long left, @SqlType(TimestampWithTimeZoneType.NAME) long right)
    {
        return unpackMillisUtc(left) < unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(BooleanType.NAME)
    public static boolean lessThanOrEqual(@SqlType(TimestampWithTimeZoneType.NAME) long left, @SqlType(TimestampWithTimeZoneType.NAME) long right)
    {
        return unpackMillisUtc(left) <= unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(BooleanType.NAME)
    public static boolean greaterThan(@SqlType(TimestampWithTimeZoneType.NAME) long left, @SqlType(TimestampWithTimeZoneType.NAME) long right)
    {
        return unpackMillisUtc(left) > unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(BooleanType.NAME)
    public static boolean greaterThanOrEqual(@SqlType(TimestampWithTimeZoneType.NAME) long left, @SqlType(TimestampWithTimeZoneType.NAME) long right)
    {
        return unpackMillisUtc(left) >= unpackMillisUtc(right);
    }

    @ScalarOperator(BETWEEN)
    @SqlType(BooleanType.NAME)
    public static boolean between(
            @SqlType(TimestampWithTimeZoneType.NAME) long value,
            @SqlType(TimestampWithTimeZoneType.NAME) long min,
            @SqlType(TimestampWithTimeZoneType.NAME) long max)
    {
        return unpackMillisUtc(min) <= unpackMillisUtc(value) && unpackMillisUtc(value) <= unpackMillisUtc(max);
    }

    @ScalarOperator(CAST)
    @SqlType(DateType.NAME)
    public static long castToDate(@SqlType(TimestampWithTimeZoneType.NAME) long value)
    {
        // round down the current timestamp to days
        ISOChronology chronology = unpackChronology(value);
        long date = chronology.dayOfYear().roundFloor(unpackMillisUtc(value));
        // date is currently midnight in timezone of the original value
        // convert to UTC
        return date + chronology.getZone().getOffset(date);
    }

    @ScalarOperator(CAST)
    @SqlType(TimeType.NAME)
    public static long castToTime(@SqlType(TimestampWithTimeZoneType.NAME) long value)
    {
        return modulo24Hour(unpackChronology(value), unpackMillisUtc(value));
    }

    @ScalarOperator(CAST)
    @SqlType(TimeWithTimeZoneType.NAME)
    public static long castToTimeWithTimeZone(@SqlType(TimestampWithTimeZoneType.NAME) long value)
    {
        int millis = modulo24Hour(unpackChronology(value), unpackMillisUtc(value));
        return packDateTimeWithZone(millis, unpackZoneKey(value));
    }

    @ScalarOperator(CAST)
    @SqlType(TimestampType.NAME)
    public static long castToTimestamp(@SqlType(TimestampWithTimeZoneType.NAME) long value)
    {
        return unpackMillisUtc(value);
    }

    @ScalarOperator(CAST)
    @SqlType(VarcharType.NAME)
    public static Slice castToSlice(@SqlType(TimestampWithTimeZoneType.NAME) long value)
    {
        return Slices.copiedBuffer(printTimestampWithTimeZone(value), UTF_8);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(BigintType.NAME)
    public static long hashCode(@SqlType(TimestampWithTimeZoneType.NAME) long value)
    {
        long millis = unpackMillisUtc(value);
        return (int) (millis ^ (millis >>> 32));
    }
}
