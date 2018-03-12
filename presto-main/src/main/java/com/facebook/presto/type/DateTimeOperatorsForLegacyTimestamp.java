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
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;

public final class DateTimeOperatorsForLegacyTimestamp
{
    private DateTimeOperatorsForLegacyTimestamp() {}

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
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampPlusIntervalYearToMonth(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().add(left, right);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long intervalYearToMonthPlusTimestamp(ConnectorSession session, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().add(right, left);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampMinusIntervalDayToSecond(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long right)
    {
        return left - right;
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.TIMESTAMP)
    public static long timestampMinusIntervalYearToMonth(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long right)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().add(left, -right);
    }
}
