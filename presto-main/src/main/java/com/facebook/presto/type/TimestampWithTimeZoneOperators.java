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
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.chrono.ISOChronology;

import static com.facebook.presto.metadata.OperatorInfo.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.NOT_EQUAL;
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
    public static boolean equal(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(TimestampWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) == unpackMillisUtc(right);
    }

    @ScalarOperator(NOT_EQUAL)
    public static boolean notEqual(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(TimestampWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) != unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN)
    public static boolean lessThan(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(TimestampWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) < unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    public static boolean lessThanOrEqual(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(TimestampWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) <= unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN)
    public static boolean greaterThan(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(TimestampWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) > unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    public static boolean greaterThanOrEqual(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(TimestampWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) >= unpackMillisUtc(right);
    }

    @ScalarOperator(BETWEEN)
    public static boolean between(@SqlType(TimestampWithTimeZoneType.class) long value, @SqlType(TimestampWithTimeZoneType.class) long min, @SqlType(TimestampWithTimeZoneType.class) long max)
    {
        return unpackMillisUtc(min) <= unpackMillisUtc(value) && unpackMillisUtc(value) <= unpackMillisUtc(max);
    }

    @ScalarOperator(CAST)
    @SqlType(DateType.class)
    public static long castToDate(@SqlType(TimestampWithTimeZoneType.class) long value)
    {
        // round down the current timestamp to days
        ISOChronology chronology = unpackChronology(value);
        long date = chronology.dayOfYear().roundFloor(unpackMillisUtc(value));
        // date is currently midnight in timezone of the original value
        // convert to UTC
        return date + chronology.getZone().getOffset(date);
    }

    @ScalarOperator(CAST)
    @SqlType(TimeType.class)
    public static long castToTime(@SqlType(TimestampWithTimeZoneType.class) long value)
    {
        return modulo24Hour(unpackChronology(value), unpackMillisUtc(value));
    }

    @ScalarOperator(CAST)
    @SqlType(TimeWithTimeZoneType.class)
    public static long castToTimeWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long value)
    {
        int millis = modulo24Hour(unpackChronology(value), unpackMillisUtc(value));
        return packDateTimeWithZone(millis, unpackZoneKey(value));
    }

    @ScalarOperator(CAST)
    @SqlType(TimestampType.class)
    public static long castToTimestamp(@SqlType(TimestampWithTimeZoneType.class) long value)
    {
        return unpackMillisUtc(value);
    }

    @ScalarOperator(CAST)
    public static Slice castToSlice(@SqlType(TimestampWithTimeZoneType.class) long value)
    {
        return Slices.copiedBuffer(printTimestampWithTimeZone(value), UTF_8);
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(@SqlType(TimestampWithTimeZoneType.class) long value)
    {
        long millis = unpackMillisUtc(value);
        return (int) (millis ^ (millis >>> 32));
    }
}
