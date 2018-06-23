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
package com.facebook.presto.testing;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.LocalDateTime;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class DateTimeTestingUtils
{
    private DateTimeTestingUtils() {}

    public static SqlTimestamp sqlTimestampOf(
            int year,
            int monthOfYear,
            int dayOfMonth,
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond,
            DateTimeZone baseZone,
            TimeZoneKey timestampZone,
            Session session)
    {
        return sqlTimestampOf(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, baseZone, timestampZone, session.toConnectorSession());
    }

    public static SqlTimestamp sqlTimestampOf(
            int year,
            int monthOfYear,
            int dayOfMonth,
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond,
            DateTimeZone baseZone,
            TimeZoneKey timestampZone,
            ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return new SqlTimestamp(new DateTime(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, baseZone).getMillis(), timestampZone);
        }
        else {
            return new SqlTimestamp(new DateTime(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, DateTimeZone.UTC).getMillis());
        }
    }

    /**
     * Constructs standard (non-legacy) TIMESTAMP value corresponding to argument
     */
    public static SqlTimestamp sqlTimestampOf(LocalDateTime dateTime)
    {
        return new SqlTimestamp(DAYS.toMillis(dateTime.toLocalDate().toEpochDay()) + NANOSECONDS.toMillis(dateTime.toLocalTime().toNanoOfDay()));
    }

    public static SqlTimestamp sqlTimestampOf(DateTime dateTime, Session session)
    {
        return sqlTimestampOf(dateTime, session.toConnectorSession());
    }

    private static SqlTimestamp sqlTimestampOf(DateTime dateTime, ConnectorSession session)
    {
        return sqlTimestampOf(dateTime.getMillis(), session);
    }

    public static SqlTimestamp sqlTimestampOf(long millis, ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return new SqlTimestamp(millis, session.getTimeZoneKey());
        }
        else {
            return new SqlTimestamp(millis);
        }
    }

    public static SqlTime sqlTimeOf(
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond,
            DateTimeZone baseZone,
            TimeZoneKey timestampZone,
            Session session)
    {
        return sqlTimeOf(hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, baseZone, timestampZone, session.toConnectorSession());
    }

    public static SqlTime sqlTimeOf(
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond,
            DateTimeZone baseZone,
            TimeZoneKey timestampZone,
            ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return new SqlTime(new DateTime(1970, 1, 1, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, baseZone).getMillis(), timestampZone);
        }
        else {
            return new SqlTime(new DateTime(1970, 1, 1, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond, DateTimeZone.UTC).getMillis());
        }
    }
}
