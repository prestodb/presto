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
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.metadata.OperatorInfo.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorInfo.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.util.DateTimeUtils.printTimeWithTimeZone;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TimeWithTimeZoneOperators
{
    private TimeWithTimeZoneOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(BooleanType.class)
    public static boolean equal(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(TimeWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) == unpackMillisUtc(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean notEqual(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(TimeWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) != unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(BooleanType.class)
    public static boolean lessThan(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(TimeWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) < unpackMillisUtc(right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean lessThanOrEqual(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(TimeWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) <= unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(BooleanType.class)
    public static boolean greaterThan(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(TimeWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) > unpackMillisUtc(right);
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean greaterThanOrEqual(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(TimeWithTimeZoneType.class) long right)
    {
        return unpackMillisUtc(left) >= unpackMillisUtc(right);
    }

    @ScalarOperator(BETWEEN)
    @SqlType(BooleanType.class)
    public static boolean between(@SqlType(TimeWithTimeZoneType.class) long value, @SqlType(TimeWithTimeZoneType.class) long min, @SqlType(TimeWithTimeZoneType.class) long max)
    {
        return unpackMillisUtc(min) <= unpackMillisUtc(value) && unpackMillisUtc(value) <= unpackMillisUtc(max);
    }

    @ScalarOperator(CAST)
    @SqlType(TimeType.class)
    public static long castToTime(Session session, @SqlType(TimeWithTimeZoneType.class) long value)
    {
        return unpackMillisUtc(value);
    }

    @ScalarOperator(CAST)
    @SqlType(TimestampType.class)
    public static long castToTimestamp(@SqlType(TimeWithTimeZoneType.class) long value)
    {
        return unpackMillisUtc(value);
    }

    @ScalarOperator(CAST)
    @SqlType(TimestampWithTimeZoneType.class)
    public static long castToTimestampWithTimeZone(@SqlType(TimeWithTimeZoneType.class) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(VarcharType.class)
    public static Slice castToSlice(@SqlType(TimeWithTimeZoneType.class) long value)
    {
        return Slices.copiedBuffer(printTimeWithTimeZone(value), UTF_8);
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(@SqlType(TimeWithTimeZoneType.class) long value)
    {
        long millis = unpackMillisUtc(value);
        return (int) (millis ^ (millis >>> 32));
    }
}
