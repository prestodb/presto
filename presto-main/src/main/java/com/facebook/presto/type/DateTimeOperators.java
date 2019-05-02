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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
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
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        if (MILLIS_OF_DAY.get(right) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot add hour, minutes or seconds to a date");
        }
        return left + TimeUnit.MILLISECONDS.toDays(right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long intervalDayToSecondPlusDate(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left, @SqlType(StandardTypes.DATE) long right)
    {
        if (MILLIS_OF_DAY.get(left) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot add hour, minutes or seconds to a date");
        }
        return TimeUnit.MILLISECONDS.toDays(left) + right;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long timePlusIntervalDayToSecond(ConnectorSession session, @SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return modulo24Hour(getChronology(session.getTimeZoneKey()), left + right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long intervalDayToSecondPlusTime(ConnectorSession session, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return modulo24Hour(getChronology(session.getTimeZoneKey()), left + right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZonePlusIntervalDayToSecond(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return updateMillisUtc((long) modulo24Hour(unpackChronology(left), unpackMillisUtc(left) + right), left);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long intervalDayToSecondPlusTimeWithTimeZone(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return updateMillisUtc((long) modulo24Hour(unpackChronology(right), left + unpackMillisUtc(right)), right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampPlusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return left + right;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long intervalDayToSecondPlusTimestamp(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return left + right;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZonePlusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return updateMillisUtc(unpackMillisUtc(left) + right, left);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long intervalDayToSecondPlusTimestampWithTimeZone(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return updateMillisUtc(left + unpackMillisUtc(right), right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalYearToMonth(@SqlType(StandardTypes.DATE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        long millis = MONTH_OF_YEAR_UTC.add(TimeUnit.DAYS.toMillis(left), right);
        return TimeUnit.MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long intervalYearToMonthPlusDate(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.DATE) long right)
    {
        long millis = MONTH_OF_YEAR_UTC.add(TimeUnit.DAYS.toMillis(right), left);
        return TimeUnit.MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long timePlusIntervalYearToMonth(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME)
    public static long intervalYearToMonthPlusTime(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.TIME) long right)
    {
        return right;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZonePlusIntervalYearToMonth(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long intervalYearToMonthPlusTimeWithTimeZone(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long right)
    {
        return right;
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampPlusIntervalYearToMonth(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        if (session.isLegacyTimestamp()) {
            return getChronology(session.getTimeZoneKey()).monthOfYear().add(left, right);
        }
        else {
            return MONTH_OF_YEAR_UTC.add(left, right);
        }
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long intervalYearToMonthPlusTimestamp(ConnectorSession session, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        if (session.isLegacyTimestamp()) {
            return getChronology(session.getTimeZoneKey()).monthOfYear().add(right, left);
        }
        else {
            return MONTH_OF_YEAR_UTC.add(right, left);
        }
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZonePlusIntervalYearToMonth(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return updateMillisUtc(unpackChronology(left).monthOfYear().add(unpackMillisUtc(left), right), left);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long intervalYearToMonthPlusTimestampWithTimeZone(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long right)
    {
        return updateMillisUtc(unpackChronology(right).monthOfYear().add(unpackMillisUtc(right), left), right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        if (MILLIS_OF_DAY.get(right) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot subtract hour, minutes or seconds from a date");
        }
        return left - TimeUnit.MILLISECONDS.toDays(right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME)
    public static long timeMinusIntervalDayToSecond(ConnectorSession session, @SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return modulo24Hour(getChronology(session.getTimeZoneKey()), left - right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZoneMinusIntervalDayToSecond(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return updateMillisUtc((long) modulo24Hour(unpackChronology(left), unpackMillisUtc(left) - right), left);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampMinusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return left - right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZoneMinusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return updateMillisUtc(unpackMillisUtc(left) - right, left);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalYearToMonth(ConnectorSession session, @SqlType(StandardTypes.DATE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        long millis = MONTH_OF_YEAR_UTC.add(TimeUnit.DAYS.toMillis(left), -right);
        return TimeUnit.MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME)
    public static long timeMinusIntervalYearToMonth(@SqlType(StandardTypes.TIME) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long timeWithTimeZoneMinusIntervalYearToMonth(@SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return left;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampMinusIntervalYearToMonth(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        if (session.isLegacyTimestamp()) {
            return getChronology(session.getTimeZoneKey()).monthOfYear().add(left, -right);
        }
        else {
            return MONTH_OF_YEAR_UTC.add(left, -right);
        }
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long timestampWithTimeZoneMinusIntervalYearToMonth(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        long dateTimeWithTimeZone = unpackChronology(left).monthOfYear().add(unpackMillisUtc(left), -right);
        return updateMillisUtc(dateTimeWithTimeZone, left);
    }

    public static int modulo24Hour(ISOChronology chronology, long millis)
    {
        return chronology.millisOfDay().get(millis) - chronology.getZone().getOffset(millis);
    }

    public static long modulo24Hour(long millis)
    {
        return MILLIS_OF_DAY.get(millis);
    }
}
