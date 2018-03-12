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

package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.type.SqlTimestamp;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.WEEKS;
import static java.time.temporal.ChronoUnit.YEARS;

public class TestDateTimeFunctionsLegacy
        extends TestDateTimeFunctionsBase
{
    public TestDateTimeFunctionsLegacy()
    {
        super(true);
    }

    @Test
    public void testFormatDatetimeUsingLegacyTimezoneSemantics()
    {
        assertFunction("format_datetime(" + TIMESTAMP_WO_TZ_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')", VARCHAR, "2001/08/22 03:04 Asia/Kathmandu");
    }

    @Test
    public void testDateDiffTimestampWithoutTimeZone()
    {
        ZonedDateTime toDateTime = TIMESTAMP_WO_TZ.atZone(DATE_TIME_ZONE.toTimeZone().toZoneId());
        ZonedDateTime fromDateTime = ZonedDateTime.of(1960, 5, 3, 7, 2, 9, 678_000_000, DATE_TIME_ZONE.toTimeZone().toZoneId());
        String baseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678'";

        assertFunction("date_diff('millisecond', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, MILLIS.between(fromDateTime, toDateTime));
        assertFunction("date_diff('second', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, SECONDS.between(fromDateTime, toDateTime));
        assertFunction("date_diff('minute', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, MINUTES.between(fromDateTime, toDateTime));
        assertFunction("date_diff('hour', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, HOURS.between(fromDateTime, toDateTime));
        assertFunction("date_diff('day', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, DAYS.between(fromDateTime, toDateTime));
        assertFunction("date_diff('week', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, WEEKS.between(fromDateTime, toDateTime));
        assertFunction("date_diff('month', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, MONTHS.between(fromDateTime, toDateTime));
        assertFunction("date_diff('quarter', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, MONTHS.between(fromDateTime, toDateTime) / 3);
        assertFunction("date_diff('year', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, YEARS.between(fromDateTime, toDateTime));
    }

    @Test
    public void testFromUnixTime()
    {
        ZonedDateTime dateTime = ZonedDateTime.of(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE.toTimeZone().toZoneId());
        double seconds = dateTime.toEpochSecond();
        assertFunction("from_unixtime(" + seconds + ")", TIMESTAMP, toTimestamp(dateTime.toLocalDateTime()));

        dateTime = ZonedDateTime.of(2001, 1, 22, 3, 4, 5, 888_000_000, DATE_TIME_ZONE.toTimeZone().toZoneId());
        seconds = dateTime.toEpochSecond() + dateTime.getNano() / 1e9;
        assertFunction("from_unixtime(" + seconds + ")", TIMESTAMP, toTimestamp(dateTime.toLocalDateTime()));
    }

    @Test
    public void testToUnixTime()
    {
        assertFunction("to_unixtime(" + TIMESTAMP_WO_TZ_LITERAL + ")", DOUBLE, TIMESTAMP_WO_TZ.atZone(DATE_TIME_ZONE.toTimeZone().toZoneId()).toEpochSecond() + TIMESTAMP_WO_TZ.getNano() / 1e9);
        assertFunction("to_unixtime(" + WEIRD_TIMESTAMP_LITERAL + ")", DOUBLE, WEIRD_TIMESTAMP.getMillis() / 1000.0);
    }

    @Test
    public void testToISO8601TimestampWithoutTimeZone()
    {
        assertFunction("to_iso8601(" + TIMESTAMP_WO_TZ_LITERAL + ")", createVarcharType(35), TIMESTAMP_WITH_NUMERICAL_ZONE_ISO8601_STRING);
    }

    @Override
    protected SqlTimestamp toTimestamp(LocalDateTime localDateTime)
    {
        ZonedDateTime zonedDateTime = localDateTime.atZone(DATE_TIME_ZONE.toTimeZone().toZoneId());
        return new SqlTimestamp(zonedDateTime.toEpochSecond() * 1000 + zonedDateTime.getNano() / 1_000_000, session.getTimeZoneKey());
    }
}
