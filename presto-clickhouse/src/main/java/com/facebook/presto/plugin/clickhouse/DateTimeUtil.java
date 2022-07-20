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
package com.facebook.presto.plugin.clickhouse;

import java.sql.Date;
import java.sql.Time;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class DateTimeUtil
{
    private DateTimeUtil()
    {
    }

    public static Date convertZonedDaysToDate(long zonedDays)
    {
        long epochMillis;

        try {
            ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDate.ofEpochDay(zonedDays), LocalTime.MIN, ZoneId.systemDefault());
            epochMillis = SECONDS.toMillis(zonedDateTime.toEpochSecond());
        }
        catch (DateTimeException ex) {
            epochMillis = zonedDays < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }

        return new Date(epochMillis);
    }

    public static long convertDateToZonedDays(Date date)
    {
        try {
            ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), ZoneId.systemDefault());
            return zonedDateTime.toLocalDate().toEpochDay();
        }
        catch (DateTimeException ex) {
            return date.getTime() < 0 ? LocalDate.MIN.toEpochDay() : LocalDate.MAX.toEpochDay();
        }
    }

    public static long getMillisOfDay(Time time)
    {
        try {
            LocalDateTime utcDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time.getTime()), ZoneOffset.UTC);
            return utcDateTime.get(ChronoField.MILLI_OF_DAY);
        }
        catch (DateTimeException ex) {
            return time.getTime() < 0 ? LocalTime.MIN.get(ChronoField.MILLI_OF_DAY) : LocalTime.MAX.get(ChronoField.MILLI_OF_DAY);
        }
    }
}
