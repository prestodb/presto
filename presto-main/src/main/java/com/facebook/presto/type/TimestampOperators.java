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
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithoutTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TimestampOperators
{
    private TimestampOperators()
    {
    }

    @ScalarOperator(EQUAL)
    public static boolean equal(@SqlType(TimestampType.class) long left, @SqlType(TimestampType.class) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    public static boolean notEqual(@SqlType(TimestampType.class) long left, @SqlType(TimestampType.class) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    public static boolean lessThan(@SqlType(TimestampType.class) long left, @SqlType(TimestampType.class) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    public static boolean lessThanOrEqual(@SqlType(TimestampType.class) long left, @SqlType(TimestampType.class) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    public static boolean greaterThan(@SqlType(TimestampType.class) long left, @SqlType(TimestampType.class) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    public static boolean greaterThanOrEqual(@SqlType(TimestampType.class) long left, @SqlType(TimestampType.class) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    public static boolean between(@SqlType(TimestampType.class) long value, @SqlType(TimestampType.class) long min, @SqlType(TimestampType.class) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(DateType.class)
    public static long castToDate(Session session, @SqlType(TimestampType.class) long value)
    {
        // round down the current timestamp to days
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        long date = chronology.dayOfYear().roundFloor(value);
        // date is currently midnight in timezone of the session
        // convert to UTC
        return date + chronology.getZone().getOffset(date);
    }

    @ScalarOperator(CAST)
    @SqlType(TimeType.class)
    public static long castToTime(Session session, @SqlType(TimestampType.class) long value)
    {
        return modulo24Hour(getChronology(session.getTimeZoneKey()), value);
    }

    @ScalarOperator(CAST)
    @SqlType(TimeWithTimeZoneType.class)
    public static long castToTimeWithTimeZone(Session session, @SqlType(TimestampType.class) long value)
    {
        int timeMillis = modulo24Hour(getChronology(session.getTimeZoneKey()), value);
        return packDateTimeWithZone(timeMillis, session.getTimeZoneKey());
    }

    @ScalarOperator(CAST)
    @SqlType(TimestampType.class)
    public static long castToTimestamp(@SqlType(TimestampType.class) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(TimestampWithTimeZoneType.class)
    public static long castToTimestampWithTimeZone(Session session, @SqlType(TimestampType.class) long value)
    {
        return packDateTimeWithZone(value, session.getTimeZoneKey());
    }

    @ScalarOperator(CAST)
    public static Slice castToSlice(Session session, @SqlType(TimestampType.class) long value)
    {
        return Slices.copiedBuffer(printTimestampWithoutTimeZone(session.getTimeZoneKey(), value), UTF_8);
    }

    @ScalarOperator(CAST)
    @SqlType(TimestampType.class)
    public static long castFromSlice(Session session, Slice value)
    {
        return parseTimestampWithoutTimeZone(session.getTimeZoneKey(), value.toStringUtf8());
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(@SqlType(TimestampType.class) long value)
    {
        return (int) (value ^ (value >>> 32));
    }
}
