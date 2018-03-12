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
import java.time.ZoneOffset;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.WEEKS;
import static java.time.temporal.ChronoUnit.YEARS;

public class TestDateTimeFunctions
        extends TestDateTimeFunctionsBase
{
    public TestDateTimeFunctions()
    {
        super(false);
    }

    @Test
    public void testFormatDatetimeUsingLegacyTimezoneSemantics()
    {
        assertInvalidFunction("format_datetime(" + TIMESTAMP_WO_TZ_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')", "Pattern can only contain zone when timestamp value contains zone information");
    }

    @Test
    public void testDateDiffTimestampWithoutTimeZone()
    {
        LocalDateTime fromDateTime = LocalDateTime.of(1960, 5, 3, 7, 2, 9, 678_000_000);
        String baseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678'";

        assertFunction("date_diff('millisecond', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, MILLIS.between(fromDateTime, TIMESTAMP_WO_TZ));
        assertFunction("date_diff('second', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, SECONDS.between(fromDateTime, TIMESTAMP_WO_TZ));
        assertFunction("date_diff('minute', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, MINUTES.between(fromDateTime, TIMESTAMP_WO_TZ));
        assertFunction("date_diff('hour', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, HOURS.between(fromDateTime, TIMESTAMP_WO_TZ));
        assertFunction("date_diff('day', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, DAYS.between(fromDateTime, TIMESTAMP_WO_TZ));
        assertFunction("date_diff('week', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, WEEKS.between(fromDateTime, TIMESTAMP_WO_TZ));
        assertFunction("date_diff('month', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, MONTHS.between(fromDateTime, TIMESTAMP_WO_TZ));
        assertFunction("date_diff('quarter', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, MONTHS.between(fromDateTime, TIMESTAMP_WO_TZ) / 3);
        assertFunction("date_diff('year', " + baseDateTimeLiteral + ", " + TIMESTAMP_WO_TZ_LITERAL + ")", BIGINT, YEARS.between(fromDateTime, TIMESTAMP_WO_TZ));
    }

    @Test
    public void testFromUnixTime()
    {
        assertInvalidFunction(
                "from_unixtime(123456789.123)",
                "A value of type TIMESTAMP WITHOUT TIME ZONE does NOT represent an instant. It represents a specific date and time pair in an unspecified timezone. As a result, invoking from_unixtime on it does NOT make sense. You may want to call from_unixtime(.., current_timezone()) instead which interprets the unix time in the context of your session timezone.");
    }

    @Test
    public void testToUnixTime()
    {
        assertInvalidFunction(
                "to_unixtime(" + TIMESTAMP_WO_TZ_LITERAL + ")",
                "A value of type TIMESTAMP WITHOUT TIME ZONE does NOT represent an instant. It represents a specific date and time pair in an unspecified timezone. As a result, invoking from_unixtime on it does NOT make sense. You may want to call to_unixtime(at_timezone(.., current_timezone())) instead which interprets the unix time in the context of your session timezone.");
        assertFunction("to_unixtime(" + WEIRD_TIMESTAMP_LITERAL + ")", DOUBLE, WEIRD_TIMESTAMP.getMillis() / 1000.0);
    }

    @Test
    public void testToISO8601TimestampWithoutTimeZone()
    {
        assertFunction("to_iso8601(" + TIMESTAMP_WO_TZ_LITERAL + ")", createVarcharType(35), TIMESTAMP_WO_TZ_ISO8601_STRING);
    }

    @Override
    protected SqlTimestamp toTimestamp(LocalDateTime localDateTime)
    {
        return new SqlTimestamp(localDateTime.toEpochSecond(ZoneOffset.UTC) * 1000 + localDateTime.getNano() / 1_000_000);
    }
}
