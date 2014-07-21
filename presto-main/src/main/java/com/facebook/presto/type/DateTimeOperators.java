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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.IntervalDayTimeType;
import com.facebook.presto.spi.type.IntervalYearMonthType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.updateMillisUtc;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.DateTimeZoneIndex.unpackChronology;

public final class DateTimeOperators
{
    private static final DateTimeField MILLIS_OF_DAY = ISOChronology.getInstanceUTC().millisOfDay();
    private static final DateTimeField MONTH_OF_YEAR_UTC = ISOChronology.getInstanceUTC().monthOfYear();

    private DateTimeOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(DateType.class)
    public static long datePlusIntervalDayToSecond(@SqlType(DateType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        if (MILLIS_OF_DAY.get(right) != 0) {
            throw new IllegalArgumentException("Can not add hour, minutes or seconds to a Date");
        }
        return left + right;
    }

    @ScalarOperator(ADD)
    @SqlType(DateType.class)
    public static long intervalDayToSecondPlusDate(@SqlType(IntervalDayTimeType.class) long left, @SqlType(DateType.class) long right)
    {
        if (MILLIS_OF_DAY.get(left) != 0) {
            throw new IllegalArgumentException("Can not add hour, minutes or seconds to a Date");
        }
        return left + right;
    }

    @ScalarOperator(ADD)
    @SqlType(TimeType.class)
    public static long timePlusIntervalDayToSecond(ConnectorSession session, @SqlType(TimeType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        return modulo24Hour(getChronology(session.getTimeZoneKey()), left + right);
    }

    @ScalarOperator(ADD)
    @SqlType(TimeType.class)
    public static long intervalDayToSecondPlusTime(ConnectorSession session, @SqlType(IntervalDayTimeType.class) long left, @SqlType(TimeType.class) long right)
    {
        return modulo24Hour(getChronology(session.getTimeZoneKey()), left + right);
    }

    @ScalarOperator(ADD)
    @SqlType(TimeWithTimeZoneType.class)
    public static long timeWithTimeZonePlusIntervalDayToSecond(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        return updateMillisUtc((long) modulo24Hour(unpackChronology(left), unpackMillisUtc(left) + right), left);
    }

    @ScalarOperator(ADD)
    @SqlType(TimeWithTimeZoneType.class)
    public static long intervalDayToSecondPlusTimeWithTimeZone(@SqlType(IntervalDayTimeType.class) long left, @SqlType(TimeWithTimeZoneType.class) long right)
    {
        return updateMillisUtc((long) modulo24Hour(unpackChronology(right), left + unpackMillisUtc(right)), right);
    }

    @ScalarOperator(ADD)
    @SqlType(TimestampType.class)
    public static long timestampPlusIntervalDayToSecond(@SqlType(TimestampType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        return left + right;
    }

    @ScalarOperator(ADD)
    @SqlType(TimestampType.class)
    public static long intervalDayToSecondPlusTimestamp(@SqlType(IntervalDayTimeType.class) long left, @SqlType(TimestampType.class) long right)
    {
        return left + right;
    }

    @ScalarOperator(ADD)
    @SqlType(TimestampWithTimeZoneType.class)
    public static long timestampWithTimeZonePlusIntervalDayToSecond(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        return updateMillisUtc(unpackMillisUtc(left) + right, left);
    }

    @ScalarOperator(ADD)
    @SqlType(TimestampWithTimeZoneType.class)
    public static long intervalDayToSecondPlusTimestampWithTimeZone(@SqlType(IntervalDayTimeType.class) long left, @SqlType(TimestampWithTimeZoneType.class) long right)
    {
        return updateMillisUtc(left + unpackMillisUtc(right), right);
    }

    @ScalarOperator(ADD)
    @SqlType(DateType.class)
    public static long datePlusIntervalYearToMonth(@SqlType(DateType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        return MONTH_OF_YEAR_UTC.add(left, right);
    }

    @ScalarOperator(ADD)
    @SqlType(DateType.class)
    public static long intervalYearToMonthPlusDate(@SqlType(IntervalYearMonthType.class) long left, @SqlType(DateType.class) long right)
    {
        return MONTH_OF_YEAR_UTC.add(right, left);
    }

    @ScalarOperator(ADD)
    @SqlType(TimeType.class)
    public static long timePlusIntervalYearToMonth(@SqlType(TimeType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        return left;
    }

    @ScalarOperator(ADD)
    @SqlType(TimeType.class)
    public static long intervalYearToMonthPlusTime(@SqlType(IntervalYearMonthType.class) long left, @SqlType(TimeType.class) long right)
    {
        return right;
    }

    @ScalarOperator(ADD)
    @SqlType(TimeWithTimeZoneType.class)
    public static long timeWithTimeZonePlusIntervalYearToMonth(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        return left;
    }

    @ScalarOperator(ADD)
    @SqlType(TimeWithTimeZoneType.class)
    public static long intervalYearToMonthPlusTimeWithTimeZone(@SqlType(IntervalYearMonthType.class) long left, @SqlType(TimeWithTimeZoneType.class) long right)
    {
        return right;
    }

    @ScalarOperator(ADD)
    @SqlType(TimestampType.class)
    public static long timestampPlusIntervalYearToMonth(ConnectorSession session, @SqlType(TimestampType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().add(left, right);
    }

    @ScalarOperator(ADD)
    @SqlType(TimestampType.class)
    public static long intervalYearToMonthPlusTimestamp(ConnectorSession session, @SqlType(IntervalYearMonthType.class) long left, @SqlType(TimestampType.class) long right)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().add(right, left);
    }

    @ScalarOperator(ADD)
    @SqlType(TimestampWithTimeZoneType.class)
    public static long timestampWithTimeZonePlusIntervalYearToMonth(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        return updateMillisUtc(unpackChronology(left).monthOfYear().add(unpackMillisUtc(left), right), left);
    }

    @ScalarOperator(ADD)
    @SqlType(TimestampWithTimeZoneType.class)
    public static long intervalYearToMonthPlusTimestampWithTimeZone(@SqlType(IntervalYearMonthType.class) long left, @SqlType(TimestampWithTimeZoneType.class) long right)
    {
        return updateMillisUtc(unpackChronology(right).monthOfYear().add(unpackMillisUtc(right), left), right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(DateType.class)
    public static long dateMinusIntervalDayToSecond(@SqlType(DateType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        if (MILLIS_OF_DAY.get(right) != 0) {
            throw new IllegalArgumentException("Can not subtract hour, minutes or seconds from a Date");
        }
        return left - right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(TimeType.class)
    public static long timeMinusIntervalDayToSecond(ConnectorSession session, @SqlType(TimeType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        return modulo24Hour(getChronology(session.getTimeZoneKey()), left - right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(TimeWithTimeZoneType.class)
    public static long timeWithTimeZoneMinusIntervalDayToSecond(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        return updateMillisUtc((long) modulo24Hour(unpackChronology(left), unpackMillisUtc(left) - right), left);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(TimestampType.class)
    public static long timestampMinusIntervalDayToSecond(@SqlType(TimestampType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        return left - right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(TimestampWithTimeZoneType.class)
    public static long timestampWithTimeZoneMinusIntervalDayToSecond(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(IntervalDayTimeType.class) long right)
    {
        return updateMillisUtc(unpackMillisUtc(left) - right, left);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(DateType.class)
    public static long dateMinusIntervalYearToMonth(ConnectorSession session, @SqlType(DateType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        return MONTH_OF_YEAR_UTC.add(left, -right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(TimeType.class)
    public static long timeMinusIntervalYearToMonth(@SqlType(TimeType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        return left;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(TimeWithTimeZoneType.class)
    public static long timeWithTimeZoneMinusIntervalYearToMonth(@SqlType(TimeWithTimeZoneType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        return left;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(TimestampType.class)
    public static long timestampMinusIntervalYearToMonth(ConnectorSession session, @SqlType(TimestampType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().add(left, -right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(TimestampWithTimeZoneType.class)
    public static long timestampWithTimeZoneMinusIntervalYearToMonth(@SqlType(TimestampWithTimeZoneType.class) long left, @SqlType(IntervalYearMonthType.class) long right)
    {
        long dateTimeWithTimeZone = unpackChronology(left).monthOfYear().add(unpackMillisUtc(left), -right);
        return updateMillisUtc(dateTimeWithTimeZone, left);
    }

    public static int modulo24Hour(ISOChronology chronology, long millis)
    {
        return chronology.millisOfDay().get(millis) - chronology.getZone().getOffset(millis);
    }
}
