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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlTime;
import com.facebook.presto.common.type.SqlTimeWithTimeZone;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.type.SqlIntervalDayTime;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.ReadableInstant;
import org.joda.time.Seconds;
import org.joda.time.chrono.ISOChronology;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SystemSessionProperties.isLegacyTimestamp;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.currentDate;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimeOf;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.joda.time.DateTimeUtils.getInstantChronology;
import static org.joda.time.DateTimeZone.UTC;
import static org.joda.time.Days.daysBetween;
import static org.joda.time.DurationFieldType.millis;
import static org.joda.time.Months.monthsBetween;
import static org.joda.time.Weeks.weeksBetween;
import static org.joda.time.Years.yearsBetween;
import static org.testng.Assert.assertEquals;

public abstract class TestDateTimeFunctionsBase
        extends AbstractTestFunctions
{
    protected static final TimeZoneKey TIME_ZONE_KEY = TestingSession.DEFAULT_TIME_ZONE_KEY;
    protected static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    protected static final DateTimeZone UTC_TIME_ZONE = getDateTimeZone(UTC_KEY);
    protected static final DateTimeZone DATE_TIME_ZONE_NUMERICAL = getDateTimeZone(getTimeZoneKey("-11:00"));
    protected static final TimeZoneKey KATHMANDU_ZONE_KEY = getTimeZoneKey("Asia/Kathmandu");
    protected static final DateTimeZone KATHMANDU_ZONE = getDateTimeZone(KATHMANDU_ZONE_KEY);
    protected static final ZoneOffset WEIRD_ZONE = ZoneOffset.ofHoursMinutes(7, 9);
    protected static final DateTimeZone WEIRD_DATE_TIME_ZONE = DateTimeZone.forID(WEIRD_ZONE.getId());

    protected static final DateTime DATE = new DateTime(2001, 8, 22, 0, 0, 0, 0, DateTimeZone.UTC);
    protected static final String DATE_LITERAL = "DATE '2001-08-22'";
    protected static final String DATE_ISO8601_STRING = "2001-08-22";

    protected static final LocalTime TIME = LocalTime.of(3, 4, 5, 321_000_000);
    protected static final String TIME_LITERAL = "TIME '03:04:05.321'";
    protected static final OffsetTime WEIRD_TIME = OffsetTime.of(3, 4, 5, 321_000_000, WEIRD_ZONE);
    protected static final String WEIRD_TIME_LITERAL = "TIME '03:04:05.321 +07:09'";

    protected static final DateTime NEW_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, UTC_TIME_ZONE); // This is TIMESTAMP w/o TZ
    protected static final DateTime LEGACY_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, DATE_TIME_ZONE);
    protected static final DateTime TIMESTAMP_WITH_NUMERICAL_ZONE = new DateTime(2001, 8, 22, 3, 4, 5, 321, DATE_TIME_ZONE_NUMERICAL);
    protected static final String TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321'";
    protected static final String TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321-11:00";
    protected static final String TIMESTAMP_ISO8601_STRING_NO_TIME_ZONE = "2001-08-22T03:04:05.321";
    protected static final DateTime WEIRD_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, WEIRD_DATE_TIME_ZONE);
    protected static final String WEIRD_TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321 +07:09'";
    protected static final String WEIRD_TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321+07:09";

    protected static final String INTERVAL_LITERAL = "INTERVAL '90061.234' SECOND";
    protected static final Duration DAY_TO_SECOND_INTERVAL = Duration.ofMillis(90061234);

    @SuppressWarnings("MemberName")
    private final DateTime TIMESTAMP;

    protected TestDateTimeFunctionsBase(boolean legacyTimestamp)
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", String.valueOf(legacyTimestamp))
                .setTimeZoneKey(TIME_ZONE_KEY)
                .setStartTime(new DateTime(2017, 4, 1, 12, 34, 56, 789, UTC_TIME_ZONE).getMillis())
                .build());
        TIMESTAMP = legacyTimestamp ? LEGACY_TIMESTAMP : NEW_TIMESTAMP;
    }

    @Test
    public void testCurrentDate()
    {
        // current date is the time at midnight in the session time zone
        assertFunction("CURRENT_DATE", DateType.DATE, new SqlDate(toIntExact(epochDaysInZone(TIME_ZONE_KEY, session.getStartTime()))));
    }

    @Test
    public void testCurrentDateTimezone()
    {
        TimeZoneKey kievTimeZoneKey = getTimeZoneKey("Europe/Kiev");
        TimeZoneKey bahiaBanderasTimeZoneKey = getTimeZoneKey("America/Bahia_Banderas"); // The zone has 'gap' on 1970-01-01
        TimeZoneKey montrealTimeZoneKey = getTimeZoneKey("America/Montreal");
        long timeIncrement = TimeUnit.MINUTES.toMillis(53);
        // We expect UTC millis later on so we have to use UTC chronology
        for (long instant = ISOChronology.getInstanceUTC().getDateTimeMillis(2000, 6, 15, 0, 0, 0, 0);
                instant < ISOChronology.getInstanceUTC().getDateTimeMillis(2016, 6, 15, 0, 0, 0, 0);
                instant += timeIncrement) {
            assertCurrentDateAtInstant(kievTimeZoneKey, instant);
            assertCurrentDateAtInstant(bahiaBanderasTimeZoneKey, instant);
            assertCurrentDateAtInstant(montrealTimeZoneKey, instant);
            assertCurrentDateAtInstant(TIME_ZONE_KEY, instant);
        }
    }

    private void assertCurrentDateAtInstant(TimeZoneKey timeZoneKey, long instant)
    {
        long expectedDays = epochDaysInZone(timeZoneKey, instant);
        long dateTimeCalculation = currentDate(
                new TestingConnectorSession(
                        "test",
                        new ConnectorIdentity("test", Optional.empty(), Optional.empty()),
                        Optional.empty(),
                        Optional.empty(),
                        timeZoneKey,
                        US,
                        instant,
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        isLegacyTimestamp(session),
                        Optional.empty(),
                        ImmutableSet.of(),
                        Optional.empty(),
                        session.getSessionFunctions()).getSqlFunctionProperties());
        assertEquals(dateTimeCalculation, expectedDays);
    }

    private static long epochDaysInZone(TimeZoneKey timeZoneKey, long instant)
    {
        return LocalDate.from(Instant.ofEpochMilli(instant).atZone(ZoneId.of(timeZoneKey.getId()))).toEpochDay();
    }

    @Test
    public void testLocalTime()
    {
        Session localSession = Session.builder(session)
                .setStartTime(new DateTime(2017, 3, 1, 14, 30, 0, 0, DATE_TIME_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("LOCALTIME", TimeType.TIME, "14:30:00.000");
        }
    }

    @Test
    public void testCurrentTime()
    {
        Session localSession = Session.builder(session)
                // we use Asia/Kathmandu here, as it has different zone offset on 2017-03-01 and on 1970-01-01
                .setTimeZoneKey(KATHMANDU_ZONE_KEY)
                .setStartTime(new DateTime(2017, 3, 1, 15, 45, 0, 0, KATHMANDU_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("CURRENT_TIME", TIME_WITH_TIME_ZONE, "15:45:00.000 Asia/Kathmandu");
        }
    }

    @Test
    public void testFromUnixTime()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE);
        double seconds = dateTime.getMillis() / 1000.0;
        assertFunction("from_unixtime(" + seconds + ")", TimestampType.TIMESTAMP, sqlTimestampOf(dateTime, session));

        dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 888, DATE_TIME_ZONE);
        seconds = dateTime.getMillis() / 1000.0;
        assertFunction("from_unixtime(" + seconds + ")", TimestampType.TIMESTAMP, sqlTimestampOf(dateTime, session));

        // The particular double, 1.7041507095805E9, was causing loss of precision in the function before the fix #21899
        SqlTimestamp expected = sqlTimestampOf(2024, 1, 1, 23, 11, 49, 580, UTC, session.getTimeZoneKey(), session.toConnectorSession());
        assertFunction("from_unixtime(1.7041507095805E9)", TimestampType.TIMESTAMP, expected);
    }

    @Test
    public void testFromUnixTimeWithOffset()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE);
        double seconds = dateTime.getMillis() / 1000.0;

        int timeZoneHoursOffset = 1;
        int timezoneMinutesOffset = 10;

        DateTime expected = new DateTime(dateTime, getDateTimeZone(getTimeZoneKeyForOffset((timeZoneHoursOffset * 60L) + timezoneMinutesOffset)));
        assertFunction("from_unixtime(" + seconds + ", " + timeZoneHoursOffset + ", " + timezoneMinutesOffset + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        // test invalid minute offsets
        assertInvalidFunction("from_unixtime(0, 1, 10000)", "Invalid offset minutes 10060");
        assertInvalidFunction("from_unixtime(0, 10000, 0)", "Invalid offset minutes 600000");
        assertInvalidFunction("from_unixtime(0, -100, 100)", "Invalid offset minutes -5900");
    }

    @Test
    public void testFromUnixTimeWithTimeZone()
    {
        String zoneId = "Asia/Shanghai";
        DateTime expected = new DateTime(1970, 1, 1, 10, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "Asia/Tokyo";
        expected = new DateTime(1970, 1, 1, 11, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "Europe/Moscow";
        expected = new DateTime(1970, 1, 1, 5, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "America/New_York";
        expected = new DateTime(1969, 12, 31, 21, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "America/Chicago";
        expected = new DateTime(1969, 12, 31, 20, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "America/Los_Angeles";
        expected = new DateTime(1969, 12, 31, 18, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));
    }

    @Test
    public void testFromUnixTimeWithTimeZoneOverflow()
    {
        String zoneId = "Asia/Shanghai";

        // Test the largest possible valid value.
        DateTime expected = new DateTime(73326, 9, 12, 4, 14, 45, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(2251799813685, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        // Test the smallest possible valid value.
        expected = new DateTime(-69387, 4, 22, 11, 50, 58, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(-2251799813685, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        // Test the values just outside the range of valid values.
        assertNumericOverflow(format("from_unixtime(2251799813686, '%s')", zoneId), "TimestampWithTimeZone overflow: 2251799813686000 ms");
        assertNumericOverflow(format("from_unixtime(-2251799813686, '%s')", zoneId), "TimestampWithTimeZone overflow: -2251799813686000 ms");
    }

    @Test
    public void testToUnixTime()
    {
        assertFunction("to_unixtime(" + TIMESTAMP_LITERAL + ")", DOUBLE, TIMESTAMP.getMillis() / 1000.0);
        assertFunction("to_unixtime(" + WEIRD_TIMESTAMP_LITERAL + ")", DOUBLE, WEIRD_TIMESTAMP.getMillis() / 1000.0);
    }

    @Test
    public void testDate()
    {
        assertFunction("date('" + DATE_ISO8601_STRING + "')", DateType.DATE, toDate(DATE));
        assertFunction("date(" + WEIRD_TIMESTAMP_LITERAL + ")", DateType.DATE, toDate(DATE));
        assertFunction("date(" + TIMESTAMP_LITERAL + ")", DateType.DATE, toDate(DATE));
    }

    @Test
    public void testFromISO8601()
    {
        assertFunction("from_iso8601_timestamp('" + TIMESTAMP_ISO8601_STRING + "')", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(TIMESTAMP_WITH_NUMERICAL_ZONE));
        assertFunction("from_iso8601_timestamp('" + WEIRD_TIMESTAMP_ISO8601_STRING + "')", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP));
        assertFunction("from_iso8601_date('" + DATE_ISO8601_STRING + "')", DateType.DATE, toDate(DATE));
    }

    @Test
    public void testFromISO8601Overflow()
    {
        String zoneId = "Z";

        // Test the largest possible valid value.
        DateTime expected = new DateTime(73326, 9, 11, 20, 14, 45, 247, DateTimeZone.forID(zoneId));
        assertFunction(format("from_iso8601_timestamp('73326-09-11T20:14:45.247%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        // Test the smallest possible valid value.
        expected = new DateTime(-69387, 12, 31, 23, 59, 59, 999, DateTimeZone.forID(zoneId));
        assertFunction(format("from_iso8601_timestamp('-69387-12-31T23:59:59.999%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        // Test the values just outside the range of valid values.
        assertNumericOverflow(format("from_iso8601_timestamp('73326-09-11T20:14:45.248%s')", zoneId), "TimestampWithTimeZone overflow: 2251799813685248 ms");
        assertNumericOverflow(format("from_iso8601_timestamp('-69388-01-01T00:00:00.000%s')", zoneId), "TimestampWithTimeZone overflow: -2251841040000000 ms");
    }

    @Test
    public void testToIso8601()
    {
        assertFunction("to_iso8601(" + WEIRD_TIMESTAMP_LITERAL + ")", createVarcharType(35), WEIRD_TIMESTAMP_ISO8601_STRING);
        assertFunction("to_iso8601(" + DATE_LITERAL + ")", createVarcharType(16), DATE_ISO8601_STRING);
    }

    @Test
    public void testTimeZone()
    {
        assertFunction("hour(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getHourOfDay());
        assertFunction("minute(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMinuteOfHour());
        assertFunction("hour(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction("minute(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction("current_timezone()", VARCHAR, TIME_ZONE_KEY.getId());
    }

    @Test
    public void testPartFunctions()
    {
        assertFunction("millisecond(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMillisOfSecond());
        assertFunction("second(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getSecondOfMinute());
        assertFunction("minute(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMinuteOfHour());
        assertFunction("hour(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getHourOfDay());
        assertFunction("day_of_week(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.dayOfWeek().get());
        assertFunction("dow(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.dayOfWeek().get());
        assertFunction("day(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_month(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_year(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.dayOfYear().get());
        assertFunction("doy(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.dayOfYear().get());
        assertFunction("week(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.weekOfWeekyear().get());
        assertFunction("week_of_year(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.weekOfWeekyear().get());
        assertFunction("month(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMonthOfYear());
        assertFunction("quarter(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getYear());
        assertFunction("timezone_minute(" + TIMESTAMP_LITERAL + ")", BIGINT, 0L);
        assertFunction("timezone_hour(" + TIMESTAMP_LITERAL + ")", BIGINT, -11L);

        assertFunction("timezone_hour(localtimestamp)", BIGINT, 14L);
        assertFunction("timezone_hour(current_timestamp)", BIGINT, 14L);

        assertFunction("millisecond(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMillisOfSecond());
        assertFunction("second(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getSecondOfMinute());
        assertFunction("minute(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction("hour(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction("day_of_week(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.dayOfWeek().get());
        assertFunction("dow(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.dayOfWeek().get());
        assertFunction("day(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_month(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("day_of_year(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.dayOfYear().get());
        assertFunction("doy(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.dayOfYear().get());
        assertFunction("week(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.weekOfWeekyear().get());
        assertFunction("week_of_year(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.weekOfWeekyear().get());
        assertFunction("month(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear());
        assertFunction("quarter(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("year(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getYear());
        assertFunction("timezone_minute(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, 9L);
        assertFunction("timezone_hour(" + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, 7L);

        assertFunction("millisecond(" + TIME_LITERAL + ")", BIGINT, TIME.getLong(MILLI_OF_SECOND));
        assertFunction("second(" + TIME_LITERAL + ")", BIGINT, (long) TIME.getSecond());
        assertFunction("minute(" + TIME_LITERAL + ")", BIGINT, (long) TIME.getMinute());
        assertFunction("hour(" + TIME_LITERAL + ")", BIGINT, (long) TIME.getHour());

        assertFunction("millisecond(" + WEIRD_TIME_LITERAL + ")", BIGINT, WEIRD_TIME.getLong(MILLI_OF_SECOND));
        assertFunction("second(" + WEIRD_TIME_LITERAL + ")", BIGINT, (long) WEIRD_TIME.getSecond());
        assertFunction("minute(" + WEIRD_TIME_LITERAL + ")", BIGINT, (long) WEIRD_TIME.getMinute());
        assertFunction("hour(" + WEIRD_TIME_LITERAL + ")", BIGINT, (long) WEIRD_TIME.getHour());

        assertFunction("millisecond(" + INTERVAL_LITERAL + ")", BIGINT, (long) DAY_TO_SECOND_INTERVAL.getNano() / 1_000_000);
        assertFunction("second(" + INTERVAL_LITERAL + ")", BIGINT, DAY_TO_SECOND_INTERVAL.getSeconds() % 60);
        assertFunction("minute(" + INTERVAL_LITERAL + ")", BIGINT, DAY_TO_SECOND_INTERVAL.getSeconds() / 60 % 60);
        assertFunction("hour(" + INTERVAL_LITERAL + ")", BIGINT, DAY_TO_SECOND_INTERVAL.getSeconds() / 3600 % 24);
    }

    @Test
    public void testPartFunctionsWithSecondsOffset()
    {
        // Prior to 1920 Asia/Kathmandu's offset was 5:41:16
        DateTime dateTime = new DateTime(1910, 1, 22, 3, 4, 5, 678, KATHMANDU_ZONE);
        String dateTimeLiteral = "TIMESTAMP '1910-01-22 03:04:05.678 Asia/Kathmandu'";

        assertFunction("millisecond(" + dateTimeLiteral + ")", BIGINT, (long) dateTime.getMillisOfSecond());
        assertFunction("second(" + dateTimeLiteral + ")", BIGINT, (long) dateTime.getSecondOfMinute());
        assertFunction("minute(" + dateTimeLiteral + ")", BIGINT, (long) dateTime.getMinuteOfHour());
        assertFunction("hour(" + dateTimeLiteral + ")", BIGINT, (long) dateTime.getHourOfDay());
    }

    @Test
    public void testYearOfWeek()
    {
        assertFunction("year_of_week(DATE '2001-08-22')", BIGINT, 2001L);
        assertFunction("yow(DATE '2001-08-22')", BIGINT, 2001L);
        assertFunction("year_of_week(DATE '2005-01-02')", BIGINT, 2004L);
        assertFunction("year_of_week(DATE '2008-12-28')", BIGINT, 2008L);
        assertFunction("year_of_week(DATE '2008-12-29')", BIGINT, 2009L);
        assertFunction("year_of_week(DATE '2009-12-31')", BIGINT, 2009L);
        assertFunction("year_of_week(DATE '2010-01-03')", BIGINT, 2009L);
        assertFunction("year_of_week(TIMESTAMP '2001-08-22 03:04:05.321 +07:09')", BIGINT, 2001L);
        assertFunction("year_of_week(TIMESTAMP '2010-01-03 03:04:05.321')", BIGINT, 2009L);
    }

    @Test
    public void testLastDayOfMonth()
    {
        assertFunction("last_day_of_month(" + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.withDayOfMonth(31)));
        assertFunction("last_day_of_month(DATE '2019-08-01')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));
        assertFunction("last_day_of_month(DATE '2019-08-31')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));

        assertFunction("last_day_of_month(" + TIMESTAMP_LITERAL + ")", DateType.DATE, toDate(DATE.withDayOfMonth(31)));
        assertFunction("last_day_of_month(TIMESTAMP '2019-08-01 00:00:00.000')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));
        assertFunction("last_day_of_month(TIMESTAMP '2019-08-01 17:00:00.000')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));
        assertFunction("last_day_of_month(TIMESTAMP '2019-08-01 23:59:59.999')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));
        assertFunction("last_day_of_month(TIMESTAMP '2019-08-31 23:59:59.999')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));

        assertFunction("last_day_of_month(" + WEIRD_TIMESTAMP_LITERAL + ")", DateType.DATE, toDate(DATE.withDayOfMonth(31)));
        ImmutableList.of("+05:45", "+00:00", "-05:45", "Asia/Tokyo", "Europe/London", "America/Los_Angeles", "America/Bahia_Banderas").forEach(timeZone -> {
            assertFunction("last_day_of_month(TIMESTAMP '2018-12-31 17:00:00.000 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2018, 12, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2018-12-31 20:00:00.000 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2018, 12, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2018-12-31 23:59:59.999 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2018, 12, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2019-01-01 00:00:00.000 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2019, 1, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2019-01-01 00:00:00.001 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2019, 1, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2019-01-01 03:00:00.000 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2019, 1, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2019-01-01 06:00:00.000 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2019, 1, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2019-08-01 00:00:00.000 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2019-08-01 17:00:00.000 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2019-08-01 23:59:59.999 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));
            assertFunction("last_day_of_month(TIMESTAMP '2019-08-31 23:59:59.999 " + timeZone + "')", DateType.DATE, toDate(LocalDate.of(2019, 8, 31)));
        });
    }

    @Test
    public void testExtractFromTimestamp()
    {
        assertFunction("extract(second FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getSecondOfMinute());
        assertFunction("extract(minute FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMinuteOfHour());
        assertFunction("extract(hour FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getHourOfDay());
        assertFunction("extract(day_of_week FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfWeek());
        assertFunction("extract(dow FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfWeek());
        assertFunction("extract(day FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_month FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_year FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfYear());
        assertFunction("extract(year_of_week FROM " + TIMESTAMP_LITERAL + ")", BIGINT, 2001L);
        assertFunction("extract(doy FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getDayOfYear());
        assertFunction("extract(week FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getWeekOfWeekyear());
        assertFunction("extract(month FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMonthOfYear());
        assertFunction("extract(quarter FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("extract(year FROM " + TIMESTAMP_LITERAL + ")", BIGINT, (long) TIMESTAMP.getYear());

        assertFunction("extract(second FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getSecondOfMinute());
        assertFunction("extract(minute FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction("extract(hour FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction("extract(day_of_week FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfWeek());
        assertFunction("extract(dow FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfWeek());
        assertFunction("extract(day FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_month FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction("extract(day_of_year FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfYear());
        assertFunction("extract(doy FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getDayOfYear());
        assertFunction("extract(week FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getWeekOfWeekyear());
        assertFunction("extract(month FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear());
        assertFunction("extract(quarter FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction("extract(year FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, (long) WEIRD_TIMESTAMP.getYear());
        assertFunction("extract(timezone_minute FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, 9L);
        assertFunction("extract(timezone_hour FROM " + WEIRD_TIMESTAMP_LITERAL + ")", BIGINT, 7L);
    }

    @Test
    public void testExtractFromTime()
    {
        assertFunction("extract(second FROM " + TIME_LITERAL + ")", BIGINT, 5L);
        assertFunction("extract(minute FROM " + TIME_LITERAL + ")", BIGINT, 4L);
        assertFunction("extract(hour FROM " + TIME_LITERAL + ")", BIGINT, 3L);

        assertFunction("extract(second FROM " + WEIRD_TIME_LITERAL + ")", BIGINT, 5L);
        assertFunction("extract(minute FROM " + WEIRD_TIME_LITERAL + ")", BIGINT, 4L);
        assertFunction("extract(hour FROM " + WEIRD_TIME_LITERAL + ")", BIGINT, 3L);
    }

    @Test
    public void testExtractFromDate()
    {
        assertFunction("extract(day_of_week FROM " + DATE_LITERAL + ")", BIGINT, 3L);
        assertFunction("extract(dow FROM " + DATE_LITERAL + ")", BIGINT, 3L);
        assertFunction("extract(day FROM " + DATE_LITERAL + ")", BIGINT, 22L);
        assertFunction("extract(day_of_month FROM " + DATE_LITERAL + ")", BIGINT, 22L);
        assertFunction("extract(day_of_year FROM " + DATE_LITERAL + ")", BIGINT, 234L);
        assertFunction("extract(doy FROM " + DATE_LITERAL + ")", BIGINT, 234L);
        assertFunction("extract(year_of_week FROM " + DATE_LITERAL + ")", BIGINT, 2001L);
        assertFunction("extract(yow FROM " + DATE_LITERAL + ")", BIGINT, 2001L);
        assertFunction("extract(week FROM " + DATE_LITERAL + ")", BIGINT, 34L);
        assertFunction("extract(month FROM " + DATE_LITERAL + ")", BIGINT, 8L);
        assertFunction("extract(quarter FROM " + DATE_LITERAL + ")", BIGINT, 3L);
        assertFunction("extract(year FROM " + DATE_LITERAL + ")", BIGINT, 2001L);

        assertFunction("extract(quarter FROM DATE '2001-01-01')", BIGINT, 1L);
        assertFunction("extract(quarter FROM DATE '2001-03-31')", BIGINT, 1L);
        assertFunction("extract(quarter FROM DATE '2001-04-01')", BIGINT, 2L);
        assertFunction("extract(quarter FROM DATE '2001-06-30')", BIGINT, 2L);
        assertFunction("extract(quarter FROM DATE '2001-07-01')", BIGINT, 3L);
        assertFunction("extract(quarter FROM DATE '2001-09-30')", BIGINT, 3L);
        assertFunction("extract(quarter FROM DATE '2001-10-01')", BIGINT, 4L);
        assertFunction("extract(quarter FROM DATE '2001-12-31')", BIGINT, 4L);

        assertFunction("extract(quarter FROM TIMESTAMP '2001-01-01 00:00:00.000')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-03-31 23:59:59.999')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-04-01 00:00:00.000')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-06-30 23:59:59.999')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-07-01 00:00:00.000')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-09-30 23:59:59.999')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-10-01 00:00:00.000')", BIGINT, 4L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-12-31 23:59:59.999')", BIGINT, 4L);

        assertFunction("extract(quarter FROM TIMESTAMP '2001-01-01 00:00:00.000 +06:00')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-03-31 23:59:59.999 +06:00')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-04-01 00:00:00.000 +06:00')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-06-30 23:59:59.999 +06:00')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-07-01 00:00:00.000 +06:00')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-09-30 23:59:59.999 +06:00')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-10-01 00:00:00.000 +06:00')", BIGINT, 4L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-12-31 23:59:59.999 +06:00')", BIGINT, 4L);
    }

    @Test
    public void testExtractFromInterval()
    {
        assertFunction("extract(second FROM INTERVAL '5' SECOND)", BIGINT, 5L);
        assertFunction("extract(second FROM INTERVAL '65' SECOND)", BIGINT, 5L);

        assertFunction("extract(minute FROM INTERVAL '4' MINUTE)", BIGINT, 4L);
        assertFunction("extract(minute FROM INTERVAL '64' MINUTE)", BIGINT, 4L);
        assertFunction("extract(minute FROM INTERVAL '247' SECOND)", BIGINT, 4L);

        assertFunction("extract(hour FROM INTERVAL '3' HOUR)", BIGINT, 3L);
        assertFunction("extract(hour FROM INTERVAL '27' HOUR)", BIGINT, 3L);
        assertFunction("extract(hour FROM INTERVAL '187' MINUTE)", BIGINT, 3L);

        assertFunction("extract(day FROM INTERVAL '2' DAY)", BIGINT, 2L);
        assertFunction("extract(day FROM INTERVAL '55' HOUR)", BIGINT, 2L);

        assertFunction("extract(month FROM INTERVAL '3' MONTH)", BIGINT, 3L);
        assertFunction("extract(month FROM INTERVAL '15' MONTH)", BIGINT, 3L);

        assertFunction("extract(year FROM INTERVAL '2' YEAR)", BIGINT, 2L);
        assertFunction("extract(year FROM INTERVAL '29' MONTH)", BIGINT, 2L);
    }

    @Test
    public void testTruncateTimestamp()
    {
        DateTime result = TIMESTAMP;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withHourOfDay(0);
        assertFunction("date_trunc('day', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withDayOfMonth(20);
        assertFunction("date_trunc('week', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withDayOfMonth(1);
        assertFunction("date_trunc('month', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withMonthOfYear(7);
        assertFunction("date_trunc('quarter', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withMonthOfYear(1);
        assertFunction("date_trunc('year', " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = WEIRD_TIMESTAMP;
        result = result.withMillisOfSecond(0);
        assertFunction("date_trunc('second', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withSecondOfMinute(0);
        assertFunction("date_trunc('minute', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withMinuteOfHour(0);
        assertFunction("date_trunc('hour', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withHourOfDay(0);
        assertFunction("date_trunc('day', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withDayOfMonth(20);
        assertFunction("date_trunc('week', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withDayOfMonth(1);
        assertFunction("date_trunc('month', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withMonthOfYear(7);
        assertFunction("date_trunc('quarter', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withMonthOfYear(1);
        assertFunction("date_trunc('year', " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));
    }

    @Test
    public void testTruncateTime()
    {
        LocalTime result = TIME;
        result = result.withNano(0);
        assertFunction("date_trunc('second', " + TIME_LITERAL + ")", TimeType.TIME, toTime(result));

        result = result.withSecond(0);
        assertFunction("date_trunc('minute', " + TIME_LITERAL + ")", TimeType.TIME, toTime(result));

        result = result.withMinute(0);
        assertFunction("date_trunc('hour', " + TIME_LITERAL + ")", TimeType.TIME, toTime(result));
    }

    @Test
    public void testTruncateTimeWithTimeZone()
    {
        OffsetTime result = WEIRD_TIME;
        result = result.withNano(0);
        assertFunction("date_trunc('second', " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(result));

        result = result.withSecond(0);
        assertFunction("date_trunc('minute', " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(result));

        result = result.withMinute(0);
        assertFunction("date_trunc('hour', " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(result));
    }

    @Test
    public void testTruncateDate()
    {
        DateTime result = DATE;
        assertFunction("date_trunc('day', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));

        result = result.withDayOfMonth(20);
        assertFunction("date_trunc('week', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));

        result = result.withDayOfMonth(1);
        assertFunction("date_trunc('month', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));

        result = result.withMonthOfYear(7);
        assertFunction("date_trunc('quarter', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));

        result = result.withMonthOfYear(1);
        assertFunction("date_trunc('year', " + DATE_LITERAL + ")", DateType.DATE, toDate(result));
    }

    @Test
    public void testAddFieldToTimestamp()
    {
        assertFunction("date_add('millisecond', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusMillis(3), session));
        assertFunction("date_add('second', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusSeconds(3), session));
        assertFunction("date_add('minute', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusMinutes(3), session));
        assertFunction("date_add('hour', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusHours(3), session));
        assertFunction("date_add('hour', 23, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusHours(23), session));
        assertFunction("date_add('hour', -4, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.minusHours(4), session));
        assertFunction("date_add('hour', -23, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.minusHours(23), session));
        assertFunction("date_add('day', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusDays(3), session));
        assertFunction("date_add('week', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusWeeks(3), session));
        assertFunction("date_add('month', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusMonths(3), session));
        assertFunction("date_add('quarter', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusMonths(3 * 3), session));
        assertFunction("date_add('year', 3, " + TIMESTAMP_LITERAL + ")", TimestampType.TIMESTAMP, sqlTimestampOf(TIMESTAMP.plusYears(3), session));

        assertFunction("date_add('millisecond', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMillis(3)));
        assertFunction("date_add('second', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusHours(3)));
        assertFunction("date_add('day', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusDays(3)));
        assertFunction("date_add('week', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + WEIRD_TIMESTAMP_LITERAL + ")", TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusYears(3)));
    }

    @Test
    public void testAddFieldToDate()
    {
        assertFunction("date_add('day', 0, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE));
        assertFunction("date_add('day', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusDays(3)));
        assertFunction("date_add('week', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusWeeks(3)));
        assertFunction("date_add('month', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusMonths(3)));
        assertFunction("date_add('quarter', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusMonths(3 * 3)));
        assertFunction("date_add('year', 3, " + DATE_LITERAL + ")", DateType.DATE, toDate(DATE.plusYears(3)));
    }

    @Test
    public void testAddFieldToTime()
    {
        assertFunction("date_add('millisecond', 0, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME));
        assertFunction("date_add('millisecond', 3, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.plusNanos(3_000_000)));
        assertFunction("date_add('second', 3, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.plusHours(3)));
        assertFunction("date_add('hour', 23, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.plusHours(23)));
        assertFunction("date_add('hour', -4, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.minusHours(4)));
        assertFunction("date_add('hour', -23, " + TIME_LITERAL + ")", TimeType.TIME, toTime(TIME.minusHours(23)));
    }

    @Test
    public void testAddFieldToTimeWithTimeZone()
    {
        assertFunction("date_add('millisecond', 3, " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusNanos(3_000_000)));
        assertFunction("date_add('second', 3, " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusSeconds(3)));
        assertFunction("date_add('minute', 3, " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusMinutes(3)));
        assertFunction("date_add('hour', 3, " + WEIRD_TIME_LITERAL + ")", TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusHours(3)));
    }

    @Test
    public void testDateDiffTimestamp()
    {
        DateTime baseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, isLegacyTimestamp(session) ? DATE_TIME_ZONE : UTC_TIME_ZONE);
        String baseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678'";

        assertFunction("date_diff('millisecond', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, millisBetween(baseDateTime, TIMESTAMP));
        assertFunction("date_diff('second', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) secondsBetween(baseDateTime, TIMESTAMP).getSeconds());
        assertFunction("date_diff('minute', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) minutesBetween(baseDateTime, TIMESTAMP).getMinutes());
        assertFunction("date_diff('hour', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) hoursBetween(baseDateTime, TIMESTAMP).getHours());
        assertFunction("date_diff('day', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) daysBetween(baseDateTime, TIMESTAMP).getDays());
        assertFunction("date_diff('week', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) weeksBetween(baseDateTime, TIMESTAMP).getWeeks());
        assertFunction("date_diff('month', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) monthsBetween(baseDateTime, TIMESTAMP).getMonths());
        assertFunction("date_diff('quarter', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) monthsBetween(baseDateTime, TIMESTAMP).getMonths() / 3);
        assertFunction("date_diff('year', " + baseDateTimeLiteral + ", " + TIMESTAMP_LITERAL + ")", BIGINT, (long) yearsBetween(baseDateTime, TIMESTAMP).getYears());

        DateTime weirdBaseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, WEIRD_DATE_TIME_ZONE);
        String weirdBaseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678 +07:09'";

        assertFunction("date_diff('millisecond', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                millisBetween(weirdBaseDateTime, WEIRD_TIMESTAMP));
        assertFunction("date_diff('second', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) secondsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getSeconds());
        assertFunction("date_diff('minute', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) minutesBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMinutes());
        assertFunction("date_diff('hour', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) hoursBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getHours());
        assertFunction("date_diff('day', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) daysBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getDays());
        assertFunction("date_diff('week', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) weeksBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getWeeks());
        assertFunction("date_diff('month', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) monthsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMonths());
        assertFunction("date_diff('quarter', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) monthsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMonths() / 3);
        assertFunction("date_diff('year', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIMESTAMP_LITERAL + ")",
                BIGINT,
                (long) yearsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getYears());
    }

    @Test
    public void testDateDiffDate()
    {
        DateTime baseDateTime = new DateTime(1960, 5, 3, 0, 0, 0, 0, DateTimeZone.UTC);
        String baseDateTimeLiteral = "DATE '1960-05-03'";

        assertFunction("date_diff('day', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) daysBetween(baseDateTime, DATE).getDays());
        assertFunction("date_diff('week', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) weeksBetween(baseDateTime, DATE).getWeeks());
        assertFunction("date_diff('month', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) monthsBetween(baseDateTime, DATE).getMonths());
        assertFunction("date_diff('quarter', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) monthsBetween(baseDateTime, DATE).getMonths() / 3);
        assertFunction("date_diff('year', " + baseDateTimeLiteral + ", " + DATE_LITERAL + ")", BIGINT, (long) yearsBetween(baseDateTime, DATE).getYears());
    }

    @Test
    public void testDateDiffTime()
    {
        LocalTime baseDateTime = LocalTime.of(7, 2, 9, 678_000_000);
        String baseDateTimeLiteral = "TIME '07:02:09.678'";

        assertFunction("date_diff('millisecond', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", BIGINT, millisBetween(baseDateTime, TIME));
        assertFunction("date_diff('second', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", BIGINT, secondsBetween(baseDateTime, TIME));
        assertFunction("date_diff('minute', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", BIGINT, minutesBetween(baseDateTime, TIME));
        assertFunction("date_diff('hour', " + baseDateTimeLiteral + ", " + TIME_LITERAL + ")", BIGINT, hoursBetween(baseDateTime, TIME));
    }

    @Test
    public void testDateDiffTimeWithTimeZone()
    {
        OffsetTime weirdBaseDateTime = OffsetTime.of(7, 2, 9, 678_000_000, WEIRD_ZONE);
        String weirdBaseDateTimeLiteral = "TIME '07:02:09.678 +07:09'";

        assertFunction("date_diff('millisecond', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", BIGINT, millisBetween(weirdBaseDateTime, WEIRD_TIME));
        assertFunction("date_diff('second', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", BIGINT, secondsBetween(weirdBaseDateTime, WEIRD_TIME));
        assertFunction("date_diff('minute', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", BIGINT, minutesBetween(weirdBaseDateTime, WEIRD_TIME));
        assertFunction("date_diff('hour', " + weirdBaseDateTimeLiteral + ", " + WEIRD_TIME_LITERAL + ")", BIGINT, hoursBetween(weirdBaseDateTime, WEIRD_TIME));
    }

    @Test
    public void testParseDatetime()
    {
        assertFunction("parse_datetime('1960/01/22 03:04', 'YYYY/MM/DD HH:mm')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DATE_TIME_ZONE)));
        assertFunction("parse_datetime('1960/01/22 03:04 Asia/Oral', 'YYYY/MM/DD HH:mm ZZZZZ')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.forID("Asia/Oral"))));
        assertFunction("parse_datetime('1960/01/22 03:04 +0500', 'YYYY/MM/DD HH:mm Z')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.forOffsetHours(5))));
    }

    @Test
    public void testParseDatetimeOverflow()
    {
        String zoneId = "Z";
        String pattern = "yyyy/MM/dd HH:mm:ss.SSS Z";

        // Test the largest possible valid value.
        DateTime expected = new DateTime(73326, 9, 11, 20, 14, 45, 247, DateTimeZone.forID(zoneId));
        assertFunction(format("parse_datetime('73326/09/11 20:14:45.247 %s', '%s')", zoneId, pattern), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        // Test the smallest possible valid value.
        expected = new DateTime(-69387, 12, 31, 23, 59, 59, 999, DateTimeZone.forID(zoneId));
        assertFunction(format("parse_datetime('-69387/12/31 23:59:59.999 %s', '%s')", zoneId, pattern), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        // Test the values just outside the range of valid values.
        assertNumericOverflow(format("parse_datetime('73326/09/11 20:14:45.248 %s', '%s')", zoneId, pattern), "TimestampWithTimeZone overflow: 2251799813685248 ms");
        assertNumericOverflow(format("parse_datetime('-69388/01/01 00:00:00.000 %s', '%s')", zoneId, pattern), "TimestampWithTimeZone overflow: -2251841040000000 ms");
    }

    @Test
    public void testFormatDatetime()
    {
        assertFunction("format_datetime(" + TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm')", VARCHAR, "2001/08/22 03:04");
        assertFunction("format_datetime(" + WEIRD_TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm')", VARCHAR, "2001/08/22 03:04");
        assertFunction("format_datetime(" + WEIRD_TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')", VARCHAR, "2001/08/22 03:04 +07:09");
    }

    @Test
    public void testDateFormat()
    {
        String dateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321'";

        assertFunction("date_format(" + dateTimeLiteral + ", '%a')", VARCHAR, "Tue");
        assertFunction("date_format(" + dateTimeLiteral + ", '%b')", VARCHAR, "Jan");
        assertFunction("date_format(" + dateTimeLiteral + ", '%c')", VARCHAR, "1");
        assertFunction("date_format(" + dateTimeLiteral + ", '%d')", VARCHAR, "09");
        assertFunction("date_format(" + dateTimeLiteral + ", '%e')", VARCHAR, "9");
        assertFunction("date_format(" + dateTimeLiteral + ", '%f')", VARCHAR, "321000");
        assertFunction("date_format(" + dateTimeLiteral + ", '%H')", VARCHAR, "13");
        assertFunction("date_format(" + dateTimeLiteral + ", '%h')", VARCHAR, "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%I')", VARCHAR, "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%i')", VARCHAR, "04");
        assertFunction("date_format(" + dateTimeLiteral + ", '%j')", VARCHAR, "009");
        assertFunction("date_format(" + dateTimeLiteral + ", '%k')", VARCHAR, "13");
        assertFunction("date_format(" + dateTimeLiteral + ", '%l')", VARCHAR, "1");
        assertFunction("date_format(" + dateTimeLiteral + ", '%M')", VARCHAR, "January");
        assertFunction("date_format(" + dateTimeLiteral + ", '%m')", VARCHAR, "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%p')", VARCHAR, "PM");
        assertFunction("date_format(" + dateTimeLiteral + ", '%r')", VARCHAR, "01:04:05 PM");
        assertFunction("date_format(" + dateTimeLiteral + ", '%S')", VARCHAR, "05");
        assertFunction("date_format(" + dateTimeLiteral + ", '%s')", VARCHAR, "05");
        assertFunction("date_format(" + dateTimeLiteral + ", '%T')", VARCHAR, "13:04:05");
        assertFunction("date_format(" + dateTimeLiteral + ", '%v')", VARCHAR, "02");
        assertFunction("date_format(" + dateTimeLiteral + ", '%W')", VARCHAR, "Tuesday");
        assertFunction("date_format(" + dateTimeLiteral + ", '%Y')", VARCHAR, "2001");
        assertFunction("date_format(" + dateTimeLiteral + ", '%y')", VARCHAR, "01");
        assertFunction("date_format(" + dateTimeLiteral + ", '%%')", VARCHAR, "%");
        assertFunction("date_format(" + dateTimeLiteral + ", 'foo')", VARCHAR, "foo");
        assertFunction("date_format(" + dateTimeLiteral + ", '%g')", VARCHAR, "g");
        assertFunction("date_format(" + dateTimeLiteral + ", '%4')", VARCHAR, "4");
        assertFunction("date_format(" + dateTimeLiteral + ", '%x %v')", VARCHAR, "2001 02");
        assertFunction("date_format(" + dateTimeLiteral + ", '%Y\u5e74%m\u6708%d\u65e5')", VARCHAR, "2001\u5e7401\u670809\u65e5");

        String weirdDateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'";

        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%a')", VARCHAR, "Tue");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%b')", VARCHAR, "Jan");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%c')", VARCHAR, "1");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%d')", VARCHAR, "09");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%e')", VARCHAR, "9");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%f')", VARCHAR, "321000");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%H')", VARCHAR, "13");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%h')", VARCHAR, "01");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%I')", VARCHAR, "01");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%i')", VARCHAR, "04");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%j')", VARCHAR, "009");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%k')", VARCHAR, "13");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%l')", VARCHAR, "1");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%M')", VARCHAR, "January");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%m')", VARCHAR, "01");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%p')", VARCHAR, "PM");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%r')", VARCHAR, "01:04:05 PM");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%S')", VARCHAR, "05");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%s')", VARCHAR, "05");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%T')", VARCHAR, "13:04:05");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%v')", VARCHAR, "02");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%W')", VARCHAR, "Tuesday");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%Y')", VARCHAR, "2001");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%y')", VARCHAR, "01");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%%')", VARCHAR, "%");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", 'foo')", VARCHAR, "foo");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%g')", VARCHAR, "g");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%4')", VARCHAR, "4");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%x %v')", VARCHAR, "2001 02");
        assertFunction("date_format(" + weirdDateTimeLiteral + ", '%Y\u5e74%m\u6708%d\u65e5')", VARCHAR, "2001\u5e7401\u670809\u65e5");

        assertFunction("date_format(TIMESTAMP '2001-01-09 13:04:05.32', '%f')", VARCHAR, "320000");
        assertFunction("date_format(TIMESTAMP '2001-01-09 00:04:05.32', '%k')", VARCHAR, "0");

        assertInvalidFunction("date_format(DATE '2001-01-09', '%D')", "%D not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%U')", "%U not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%u')", "%u not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%V')", "%V not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%w')", "%w not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%X')", "%X not supported in date format string");
    }

    @Test
    public void testDateParse()
    {
        assertFunction("date_parse('2013', '%Y')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 1, 1, 0, 0, 0, 0, session));
        assertFunction("date_parse('2013-05', '%Y-%m')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 1, 0, 0, 0, 0, session));
        assertFunction("date_parse('2013-05-17', '%Y-%m-%d')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 0, 0, 0, 0, session));
        assertFunction("date_parse('2013-05-17 12:35:10', '%Y-%m-%d %h:%i:%s')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 0, 35, 10, 0, session));
        assertFunction("date_parse('2013-05-17 12:35:10 PM', '%Y-%m-%d %h:%i:%s %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 12, 35, 10, 0, session));
        assertFunction("date_parse('2013-05-17 12:35:10 AM', '%Y-%m-%d %h:%i:%s %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 0, 35, 10, 0, session));

        assertFunction("date_parse('2013-05-17 00:35:10', '%Y-%m-%d %H:%i:%s')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 0, 35, 10, 0, session));
        assertFunction("date_parse('2013-05-17 23:35:10', '%Y-%m-%d %H:%i:%s')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 23, 35, 10, 0, session));
        assertFunction("date_parse('abc 2013-05-17 fff 23:35:10 xyz', 'abc %Y-%m-%d fff %H:%i:%s xyz')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 23, 35, 10, 0, session));

        assertFunction("date_parse('2013 14', '%Y %y')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2014, 1, 1, 0, 0, 0, 0, session));

        assertFunction("date_parse('1998 53', '%x %v')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1998, 12, 28, 0, 0, 0, 0, session));

        assertFunction("date_parse('1.1', '%s.%f')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 100, session));
        assertFunction("date_parse('1.01', '%s.%f')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 10, session));
        assertFunction("date_parse('1.2006', '%s.%f')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 200, session));
        assertFunction("date_parse('59.123456789', '%s.%f')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 59, 123, session));

        assertFunction("date_parse('0', '%k')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 0, 0, session));

        assertFunction("date_parse('28-JAN-16 11.45.46.421000 PM','%d-%b-%y %l.%i.%s.%f %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2016, 1, 28, 23, 45, 46, 421, session));
        assertFunction("date_parse('11-DEC-70 11.12.13.456000 AM','%d-%b-%y %l.%i.%s.%f %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 12, 11, 11, 12, 13, 456, session));
        assertFunction("date_parse('31-MAY-69 04.59.59.999000 AM','%d-%b-%y %l.%i.%s.%f %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2069, 5, 31, 4, 59, 59, 999, session));

        assertInvalidFunction("date_parse('', '%D')", "%D not supported in date format string");
        assertInvalidFunction("date_parse('', '%U')", "%U not supported in date format string");
        assertInvalidFunction("date_parse('', '%u')", "%u not supported in date format string");
        assertInvalidFunction("date_parse('', '%V')", "%V not supported in date format string");
        assertInvalidFunction("date_parse('', '%w')", "%w not supported in date format string");
        assertInvalidFunction("date_parse('', '%X')", "%X not supported in date format string");

        assertInvalidFunction("date_parse('3.0123456789', '%s.%f')", "Invalid format: \"3.0123456789\" is malformed at \"9\"");
        assertInvalidFunction("date_parse('%Y-%m-%d', '')", "Both printing and parsing not supported");
    }

    @Test
    public void testLocale()
    {
        Locale locale = Locale.KOREAN;
        Session localeSession = Session.builder(this.session)
                .setTimeZoneKey(TIME_ZONE_KEY)
                .setLocale(locale)
                .build();

        try (FunctionAssertions localeAssertions = new FunctionAssertions(localeSession)) {
            String dateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321'";

            localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%a')", VARCHAR, "화");
            localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%W')", VARCHAR, "화요일");
            localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%p')", VARCHAR, "오후");
            localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%r')", VARCHAR, "01:04:05 오후");
            localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%b')", VARCHAR, "1월");
            localeAssertions.assertFunction("date_format(" + dateTimeLiteral + ", '%M')", VARCHAR, "1월");

            localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'EEE')", VARCHAR, "화");
            localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'EEEE')", VARCHAR, "화요일");
            localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'a')", VARCHAR, "오후");
            localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'MMM')", VARCHAR, "1월");
            localeAssertions.assertFunction("format_datetime(" + dateTimeLiteral + ", 'MMMM')", VARCHAR, "1월");

            localeAssertions.assertFunction("date_parse('2013-05-17 12:35:10 오후', '%Y-%m-%d %h:%i:%s %p')",
                    TimestampType.TIMESTAMP,
                    sqlTimestampOf(2013, 5, 17, 12, 35, 10, 0, localeSession));
            localeAssertions.assertFunction("date_parse('2013-05-17 12:35:10 오전', '%Y-%m-%d %h:%i:%s %p')",
                    TimestampType.TIMESTAMP,
                    sqlTimestampOf(2013, 5, 17, 0, 35, 10, 0, localeSession));

            localeAssertions.assertFunction("parse_datetime('2013-05-17 12:35:10 오후', 'yyyy-MM-dd hh:mm:ss a')",
                    TIMESTAMP_WITH_TIME_ZONE,
                    toTimestampWithTimeZone(new DateTime(2013, 5, 17, 12, 35, 10, 0, DATE_TIME_ZONE)));
            localeAssertions.assertFunction("parse_datetime('2013-05-17 12:35:10 오전', 'yyyy-MM-dd hh:mm:ss aaa')",
                    TIMESTAMP_WITH_TIME_ZONE,
                    toTimestampWithTimeZone(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE)));
        }
    }

    @Test
    public void testDateTimeOutputString()
    {
        // SqlDate
        assertFunctionString("date '2012-12-31'", DateType.DATE, "2012-12-31");
        assertFunctionString("date '0000-12-31'", DateType.DATE, "0000-12-31");
        assertFunctionString("date '0000-09-23'", DateType.DATE, "0000-09-23");
        assertFunctionString("date '0001-10-25'", DateType.DATE, "0001-10-25");
        assertFunctionString("date '1560-04-29'", DateType.DATE, "1560-04-29");

        // SqlTime
        assertFunctionString("time '00:00:00'", TimeType.TIME, "00:00:00.000");
        assertFunctionString("time '01:02:03'", TimeType.TIME, "01:02:03.000");
        assertFunctionString("time '23:23:23.233'", TimeType.TIME, "23:23:23.233");
        assertFunctionString("time '23:59:59.999'", TimeType.TIME, "23:59:59.999");

        // SqlTimeWithTimeZone
        assertFunctionString("time '00:00:00 UTC'", TIME_WITH_TIME_ZONE, "00:00:00.000 UTC");
        assertFunctionString("time '01:02:03 Asia/Shanghai'", TIME_WITH_TIME_ZONE, "01:02:03.000 Asia/Shanghai");
        assertFunctionString("time '23:23:23.233 America/Los_Angeles'", TIME_WITH_TIME_ZONE, "23:23:23.233 America/Los_Angeles");
        assertFunctionString(WEIRD_TIME_LITERAL, TIME_WITH_TIME_ZONE, "03:04:05.321 +07:09");
        assertFunctionString("time '23:59:59.999 Asia/Kathmandu'", TIME_WITH_TIME_ZONE, "23:59:59.999 Asia/Kathmandu");

        // SqlTimestamp
        assertFunctionString("timestamp '0000-01-02 01:02:03'", TimestampType.TIMESTAMP, "0000-01-02 01:02:03.000");
        assertFunctionString("timestamp '2012-12-31 00:00:00'", TimestampType.TIMESTAMP, "2012-12-31 00:00:00.000");
        assertFunctionString("timestamp '1234-05-06 23:23:23.233'", TimestampType.TIMESTAMP, "1234-05-06 23:23:23.233");
        // TODO: when tests are run on a non-UTC time zone machine, this breaks.
        // assertFunctionString("timestamp '2333-02-23 23:59:59.999'", TimestampType.TIMESTAMP, "2333-02-23 23:59:59.999");

        // SqlTimestampWithTimeZone
        assertFunctionString("timestamp '2012-12-31 00:00:00 UTC'", TIMESTAMP_WITH_TIME_ZONE, "2012-12-31 00:00:00.000 UTC");
        assertFunctionString("timestamp '0000-01-02 01:02:03 Asia/Shanghai'", TIMESTAMP_WITH_TIME_ZONE, "0000-01-02 01:02:03.000 Asia/Shanghai");
        assertFunctionString("timestamp '1234-05-06 23:23:23.233 America/Los_Angeles'", TIMESTAMP_WITH_TIME_ZONE, "1234-05-06 23:23:23.233 America/Los_Angeles");
        assertFunctionString("timestamp '2333-02-23 23:59:59.999 Asia/Tokyo'", TIMESTAMP_WITH_TIME_ZONE, "2333-02-23 23:59:59.999 Asia/Tokyo");
    }

    @Test
    public void testTimeWithTimeZoneAtTimeZone()
    {
        // this test does use hidden at_timezone function as it is equivalent of using SQL syntax AT TIME ZONE
        // but our test framework doesn't support that syntax directly.

        Session oldKathmanduTimeZoneOffsetSession =
                Session.builder(this.session)
                        .setTimeZoneKey(TIME_ZONE_KEY)
                        .setStartTime(new DateTime(1980, 1, 1, 10, 0, 0, DATE_TIME_ZONE).getMillis())
                        .build();

        TimeZoneKey europeWarsawTimeZoneKey = getTimeZoneKey("Europe/Warsaw");
        DateTimeZone europeWarsawTimeZone = getDateTimeZone(europeWarsawTimeZoneKey);
        Session europeWarsawSessionWinter =
                Session.builder(this.session)
                        .setTimeZoneKey(europeWarsawTimeZoneKey)
                        .setStartTime(new DateTime(2017, 1, 1, 10, 0, 0, europeWarsawTimeZone).getMillis())
                        .build();
        try (FunctionAssertions europeWarsawAssertionsWinter = new FunctionAssertions(europeWarsawSessionWinter);
                FunctionAssertions oldKathmanduTimeZoneOffsetAssertions = new FunctionAssertions(oldKathmanduTimeZoneOffsetSession)) {
            long millisTenOClockWarsawWinter = new DateTime(1970, 1, 1, 9, 0, 0, 0, UTC_TIME_ZONE).getMillis();

            // Simple shift to UTC
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'UTC')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, UTC_KEY));

            // Simple shift to fixed TZ
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', '+00:45')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, getTimeZoneKey("+00:45")));

            // Simple shift to geographical TZ
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'America/New_York')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, getTimeZoneKey("America/New_York")));

            // No shift but different time zone
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'Europe/Berlin')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, getTimeZoneKey("Europe/Berlin")));

            // Noop on UTC
            assertFunction("at_timezone(TIME '10:00 UTC', 'UTC')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 10, 0, 0, 0, UTC_TIME_ZONE).getMillis(), TimeZoneKey.UTC_KEY));

            // Noop on other TZ
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'Europe/Warsaw')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, europeWarsawTimeZoneKey));

            // Noop on other TZ on different session TZ
            assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'Europe/Warsaw')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, europeWarsawTimeZoneKey));

            // Shift through days back
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '2:00 Europe/Warsaw', 'America/New_York')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 20, 0, 0, 0, getDateTimeZone(getTimeZoneKey("America/New_York"))).getMillis(), getTimeZoneKey("America/New_York")));

            // Shift through days forward
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '22:00 America/New_York', 'Europe/Warsaw')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 4, 0, 0, 0, europeWarsawTimeZone).getMillis(), europeWarsawTimeZoneKey));

            // Shift backward on min value
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '00:00 +14:00', '+13:00')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 23, 0, 0, 0, getDateTimeZone(getTimeZoneKey("+13:00"))).getMillis(), getTimeZoneKey("+13:00")));

            // Shift backward on min value
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '00:00 +14:00', '-14:00')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 20, 0, 0, 0, getDateTimeZone(getTimeZoneKey("-14:00"))).getMillis(), getTimeZoneKey("-14:00")));

            // Shift backward on max value
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '23:59:59.999 +14:00', '+13:00')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 22, 59, 59, 999, getDateTimeZone(getTimeZoneKey("+13:00"))).getMillis(), getTimeZoneKey("+13:00")));

            // Shift forward on max value
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '23:59:59.999 +14:00', '-14:00')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 19, 59, 59, 999, getDateTimeZone(getTimeZoneKey("-14:00"))).getMillis(), getTimeZoneKey("-14:00")));

            // Asia/Kathmandu used +5:30 TZ until 1986 and than switched to +5:45
            // This test checks if we do use offset of time zone valid currently and not the historical one
            assertFunction("at_timezone(TIME '10:00 Asia/Kathmandu', 'UTC')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 4, 15, 0, 0, UTC_TIME_ZONE).getMillis(), TimeZoneKey.UTC_KEY));

            // Noop when time zone doesn't change
            TimeZoneKey kabul = TimeZoneKey.getTimeZoneKey("Asia/Kabul");
            assertFunction("at_timezone(TIME '10:00 Asia/Kabul', 'Asia/Kabul')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 10, 0, 0, 0, getDateTimeZone(kabul)).getMillis(), kabul));

            // This test checks if the TZ offset isn't calculated on other fixed point in time by checking if
            // session started in 1980 would get historical Asia/Kathmandu offset.
            oldKathmanduTimeZoneOffsetAssertions.assertFunction("at_timezone(TIME '10:00 Asia/Kathmandu', 'UTC')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 4, 30, 0, 0, UTC_TIME_ZONE).getMillis(), TimeZoneKey.UTC_KEY));

            // Check simple interval shift
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 +01:00', INTERVAL '2' HOUR)",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 11, 0, 0, 0, getDateTimeZone(getTimeZoneKey("+02:00"))).getMillis(), getTimeZoneKey("+02:00")));

            // Check to high interval shift
            europeWarsawAssertionsWinter.assertInvalidFunction("at_timezone(TIME '10:00 +01:00', INTERVAL '60' HOUR)",
                    StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                    "Invalid offset minutes 3600");
        }
    }

    @Test
    public void testParseDuration()
    {
        assertFunction("parse_duration('1234 ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("parse_duration('1234 us')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 1));
        assertFunction("parse_duration('1234 ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 234));
        assertFunction("parse_duration('1234 s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 20, 34, 0));
        assertFunction("parse_duration('1234 m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 0, 0));
        assertFunction("parse_duration('1234 h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 0, 0, 0));
        assertFunction("parse_duration('1234 d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 0, 0, 0, 0));
        assertFunction("parse_duration('1234.567 ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("parse_duration('1234.567 ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 235));
        assertFunction("parse_duration('1234.567 s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1234, 567));
        assertFunction("parse_duration('1234.567 m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 34, 20));
        assertFunction("parse_duration('1234.567 h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 34, 1, 200));
        assertFunction("parse_duration('1234.567 d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 13, 36, 28, 800));

        // without space
        assertFunction("parse_duration('1234ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("parse_duration('1234us')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 1));
        assertFunction("parse_duration('1234ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 234));
        assertFunction("parse_duration('1234s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 20, 34, 0));
        assertFunction("parse_duration('1234m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 0, 0));
        assertFunction("parse_duration('1234h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 0, 0, 0));
        assertFunction("parse_duration('1234d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 0, 0, 0, 0));
        assertFunction("parse_duration('1234.567ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("parse_duration('1234.567ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 235));
        assertFunction("parse_duration('1234.567s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1234, 567));
        assertFunction("parse_duration('1234.567m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 34, 20));
        assertFunction("parse_duration('1234.567h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 34, 1, 200));
        assertFunction("parse_duration('1234.567d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 13, 36, 28, 800));

        // trailing spaces
        assertFunction("parse_duration('1234 ns ')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("parse_duration('1234 us ')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 1));
        assertFunction("parse_duration('1234ms ')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 234));

        // invalid function calls
        assertInvalidFunction("parse_duration('')", "duration is empty");
        assertInvalidFunction("parse_duration('1f')", "Unknown time unit: f");
        assertInvalidFunction("parse_duration('abc')", "duration is not a valid data duration string: abc");

        // long milliseconds edge cases
        assertFunction("parse_duration('7702741401940153ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(89152099, 13, 25, 40, 153));
        assertFunction("parse_duration('9117756383778565ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(105529587, 18, 36, 18, 565));

        // Test precision for large values with fractional seconds
        assertFunction("parse_duration('7702741401940.153s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(89152099, 13, 25, 40, 153));
        assertFunction("parse_duration('7702741401940.153 s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(89152099, 13, 25, 40, 153));
    }

    @Test
    public void testIntervalDayToSecondToMilliseconds()
    {
        assertFunction("to_milliseconds(parse_duration('1ns'))", BigintType.BIGINT, 0L);
        assertFunction("to_milliseconds(parse_duration('1ms'))", BigintType.BIGINT, 1L);
        assertFunction("to_milliseconds(parse_duration('1s'))", BigintType.BIGINT, SECONDS.toMillis(1));
        assertFunction("to_milliseconds(parse_duration('1h'))", BigintType.BIGINT, HOURS.toMillis(1));
        assertFunction("to_milliseconds(parse_duration('1d'))", BigintType.BIGINT, DAYS.toMillis(1));
    }

    private static SqlDate toDate(LocalDate localDate)
    {
        return new SqlDate(toIntExact(localDate.toEpochDay()));
    }

    private static SqlDate toDate(DateTime dateDate)
    {
        long millis = dateDate.getMillis();
        return new SqlDate(toIntExact(MILLISECONDS.toDays(millis)));
    }

    private static long millisBetween(ReadableInstant start, ReadableInstant end)
    {
        requireNonNull(start, "start is null");
        requireNonNull(end, "end is null");
        return millis().getField(getInstantChronology(start)).getDifferenceAsLong(end.getMillis(), start.getMillis());
    }

    private static Seconds secondsBetween(ReadableInstant start, ReadableInstant end)
    {
        return Seconds.secondsBetween(start, end);
    }

    private static Minutes minutesBetween(ReadableInstant start, ReadableInstant end)
    {
        return Minutes.minutesBetween(start, end);
    }

    private static Hours hoursBetween(ReadableInstant start, ReadableInstant end)
    {
        return Hours.hoursBetween(start, end);
    }

    private static long millisBetween(LocalTime start, LocalTime end)
    {
        return NANOSECONDS.toMillis(end.toNanoOfDay() - start.toNanoOfDay());
    }

    private static long secondsBetween(LocalTime start, LocalTime end)
    {
        return NANOSECONDS.toSeconds(end.toNanoOfDay() - start.toNanoOfDay());
    }

    private static long minutesBetween(LocalTime start, LocalTime end)
    {
        return NANOSECONDS.toMinutes(end.toNanoOfDay() - start.toNanoOfDay());
    }

    private static long hoursBetween(LocalTime start, LocalTime end)
    {
        return NANOSECONDS.toHours(end.toNanoOfDay() - start.toNanoOfDay());
    }

    private static long millisBetween(OffsetTime start, OffsetTime end)
    {
        return millisUtc(end) - millisUtc(start);
    }

    private static long secondsBetween(OffsetTime start, OffsetTime end)
    {
        return MILLISECONDS.toSeconds(millisBetween(start, end));
    }

    private static long minutesBetween(OffsetTime start, OffsetTime end)
    {
        return MILLISECONDS.toMinutes(millisBetween(start, end));
    }

    private static long hoursBetween(OffsetTime start, OffsetTime end)
    {
        return MILLISECONDS.toHours(millisBetween(start, end));
    }

    private SqlTime toTime(LocalTime time)
    {
        return sqlTimeOf(time, session);
    }

    private static SqlTimeWithTimeZone toTimeWithTimeZone(OffsetTime offsetTime)
    {
        return new SqlTimeWithTimeZone(
                millisUtc(offsetTime),
                TimeZoneKey.getTimeZoneKey(offsetTime.getOffset().getId()));
    }

    private static long millisUtc(OffsetTime offsetTime)
    {
        return offsetTime.atDate(LocalDate.ofEpochDay(0)).toInstant().toEpochMilli();
    }

    private static SqlTimestampWithTimeZone toTimestampWithTimeZone(DateTime dateTime)
    {
        return new SqlTimestampWithTimeZone(dateTime.getMillis(), getTimeZoneKey(dateTime.getZone().getID()));
    }
}
