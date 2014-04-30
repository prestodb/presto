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
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.VarcharType;
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
import static com.facebook.presto.util.DateTimeUtils.parseDate;
import static com.facebook.presto.util.DateTimeUtils.printDate;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DateOperators
{
    private DateOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(BooleanType.class)
    public static boolean equal(@SqlType(DateType.class) long left, @SqlType(DateType.class) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean notEqual(@SqlType(DateType.class) long left, @SqlType(DateType.class) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(BooleanType.class)
    public static boolean lessThan(@SqlType(DateType.class) long left, @SqlType(DateType.class) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean lessThanOrEqual(@SqlType(DateType.class) long left, @SqlType(DateType.class) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(BooleanType.class)
    public static boolean greaterThan(@SqlType(DateType.class) long left, @SqlType(DateType.class) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(BooleanType.class)
    public static boolean greaterThanOrEqual(@SqlType(DateType.class) long left, @SqlType(DateType.class) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(BooleanType.class)
    public static boolean between(@SqlType(DateType.class) long value, @SqlType(DateType.class) long min, @SqlType(DateType.class) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(TimestampType.class)
    public static long castToTimestamp(Session session, @SqlType(DateType.class) long value)
    {
        // date is encoded as milliseconds at midnight in UTC
        // convert to midnight if the session timezone
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        return value - chronology.getZone().getOffset(value);
    }

    @ScalarOperator(CAST)
    @SqlType(TimestampWithTimeZoneType.class)
    public static long castToTimestampWithTimeZone(Session session, @SqlType(DateType.class) long value)
    {
        // date is encoded as milliseconds at midnight in UTC
        // convert to midnight if the session timezone
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        long millis = value - chronology.getZone().getOffset(value);
        return packDateTimeWithZone(millis, session.getTimeZoneKey());
    }

    @ScalarOperator(CAST)
    @SqlType(VarcharType.class)
    public static Slice castToSlice(@SqlType(DateType.class) long value)
    {
        return Slices.copiedBuffer(printDate(value), UTF_8);
    }

    @ScalarOperator(CAST)
    @SqlType(DateType.class)
    public static long castFromSlice(@SqlType(VarcharType.class) Slice value)
    {
        return parseDate(value.toStringUtf8());
    }

    @ScalarOperator(HASH_CODE)
    public static int hashCode(@SqlType(DateType.class) long value)
    {
        return (int) (value ^ (value >>> 32));
    }
}
