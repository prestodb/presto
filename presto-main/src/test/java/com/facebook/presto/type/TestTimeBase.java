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

import com.facebook.presto.Session;
import com.facebook.presto.common.type.SqlTimeWithTimeZone;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.testing.TestingSession;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.TimeZone;

import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimeOf;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;

public abstract class TestTimeBase
        extends AbstractTestFunctions
{
    protected static final TimeZoneKey TIME_ZONE_KEY = TestingSession.DEFAULT_TIME_ZONE_KEY;
    protected static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);

    public TestTimeBase(boolean legacyTimestamp)
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", String.valueOf(legacyTimestamp))
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build());
    }

    @Test
    public void testLiteral()
    {
        assertFunction("TIME '03:04:05.321'", TIME, sqlTimeOf(3, 4, 5, 321, session));
        assertFunction("TIME '03:04:05'", TIME, sqlTimeOf(3, 4, 5, 0, session));
        assertFunction("TIME '03:04'", TIME, sqlTimeOf(3, 4, 0, 0, session));
        assertInvalidFunction("TIME 'text'", SemanticErrorCode.INVALID_LITERAL, "line 1:1: 'text' is not a valid time literal");
    }

    @Test
    public void testSubtract()
    {
        functionAssertions.assertFunctionString("TIME '14:15:16.432' - TIME '03:04:05.321'", INTERVAL_DAY_TIME, "0 11:11:11.111");

        functionAssertions.assertFunctionString("TIME '03:04:05.321' - TIME '14:15:16.432'", INTERVAL_DAY_TIME, "-0 11:11:11.111");
    }

    @Test
    public void testEqual()
    {
        assertFunction("TIME '03:04:05.321' = TIME '03:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' = TIME '03:04:05.333'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("TIME '03:04:05.321' <> TIME '03:04:05.333'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' <> TIME '03:04:05.321'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("TIME '03:04:05.321' < TIME '03:04:05.333'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' < TIME '03:04:05.321'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321' < TIME '03:04:05'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("TIME '03:04:05.321' <= TIME '03:04:05.333'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' <= TIME '03:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' <= TIME '03:04:05'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("TIME '03:04:05.321' > TIME '03:04:05.111'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' > TIME '03:04:05.321'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321' > TIME '03:04:05.333'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("TIME '03:04:05.321' >= TIME '03:04:05.111'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' >= TIME '03:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' >= TIME '03:04:05.333'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.111' and TIME '03:04:05.333'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.321' and TIME '03:04:05.333'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.111' and TIME '03:04:05.321'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.321' and TIME '03:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.322' and TIME '03:04:05.333'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.311' and TIME '03:04:05.312'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321' between TIME '03:04:05.333' and TIME '03:04:05.111'", BOOLEAN, false);
    }

    @Test
    public void testCastToTimeWithTimeZone()
    {
        assertFunction("cast(TIME '03:04:05.321' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), TimeZone.getTimeZone(ZoneId.of(DATE_TIME_ZONE.getID()))));
    }

    @Test
    public void testCastToTimeWithTimeZoneWithTZWithRulesChanged()
    {
        TimeZoneKey timeZoneThatChangedSince1970 = getTimeZoneKey("Asia/Kathmandu");
        DateTimeZone dateTimeZoneThatChangedSince1970 = getDateTimeZone(timeZoneThatChangedSince1970);
        Session session = Session.builder(this.session)
                .setTimeZoneKey(timeZoneThatChangedSince1970)
                .build();
        try (FunctionAssertions localAssertions = new FunctionAssertions(session)) {
            localAssertions.assertFunction(
                    "cast(TIME '03:04:05.321' as time with time zone)",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, dateTimeZoneThatChangedSince1970).getMillis(), TimeZone.getTimeZone(ZoneId.of(dateTimeZoneThatChangedSince1970.getID()))));
        }
    }

    @Test
    public void testCastToTimeWithTimeZoneDSTIsNotAppliedWhenTimeCrossesDST()
    {
        // Australia/Sydney will switch DST a second after session start
        // For simplicity we have to use time zone that is going forward when entering DST zone with 1970-01-01
        Session session = Session.builder(this.session)
                .setTimeZoneKey(getTimeZoneKey("Australia/Sydney"))
                .setStartTime(new DateTime(2017, 10, 1, 1, 59, 59, 999, getDateTimeZone(getTimeZoneKey("Australia/Sydney"))).getMillis())
                .build();
        try (FunctionAssertions localAssertions = new FunctionAssertions(session)) {
            localAssertions.assertFunctionString("cast(TIME '12:00:00.000' as time with time zone)", TIME_WITH_TIME_ZONE, "12:00:00.000 Australia/Sydney");
        }
    }

    @Test
    public void testCastToTimestamp()
    {
        assertFunction("cast(TIME '03:04:05.321' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 3, 4, 5, 321, session));
    }

    @Test
    public void testCastToTimestampWithTimeZone()
    {
        assertFunction("cast(TIME '03:04:05.321' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testCastToSlice()
    {
        assertFunction("cast(TIME '03:04:05.321' as varchar)", VARCHAR, "03:04:05.321");
        assertFunction("cast(TIME '03:04:05' as varchar)", VARCHAR, "03:04:05.000");
        assertFunction("cast(TIME '03:04' as varchar)", VARCHAR, "03:04:00.000");
    }

    @Test
    public void testCastFromSlice()
    {
        assertFunction("cast('03:04:05.321' as time) = TIME '03:04:05.321'", BOOLEAN, true);
        assertFunction("cast('03:04:05' as time) = TIME '03:04:05.000'", BOOLEAN, true);
        assertFunction("cast('03:04' as time) = TIME '03:04:00.000'", BOOLEAN, true);
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as TIME)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "TIME '00:00:00'", BOOLEAN, false);
    }

    @Test
    public void testDistinctFrom()
    {
        assertFunction("TIME '12:34:56' IS DISTINCT FROM TIME '12:34:56'", BOOLEAN, false);
        assertFunction("TIME '12:34:56.1' IS DISTINCT FROM TIME '12:34:56.1'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEquals()
    {
        // equality
        assertFunction("TIME '12:34:56' <= TIME '12:34:56'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.1' <= TIME '12:34:56.1'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.12' <= TIME '12:34:56.12'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.123' <= TIME '12:34:56.123'", BOOLEAN, true);

        // less than
        assertFunction("TIME '12:34:56' <= TIME '12:34:57'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.1' <= TIME '12:34:56.2'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.12' <= TIME '12:34:56.13'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.123' <= TIME '12:34:56.124'", BOOLEAN, true);

        // false cases
        assertFunction("TIME '12:34:56' <= TIME '12:34:55'", BOOLEAN, false);
        assertFunction("TIME '12:34:56.1' <= TIME '12:34:56.0'", BOOLEAN, false);
        assertFunction("TIME '12:34:56.12' <= TIME '12:34:56.11'", BOOLEAN, false);
        assertFunction("TIME '12:34:56.123' <= TIME '12:34:56.122'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEquals()
    {
        // equality
        assertFunction("TIME '12:34:56' >= TIME '12:34:56'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.1' >= TIME '12:34:56.1'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.12' >= TIME '12:34:56.12'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.123' >= TIME '12:34:56.123'", BOOLEAN, true);

        // greater than
        assertFunction("TIME '12:34:56' >= TIME '12:34:57'", BOOLEAN, false);
        assertFunction("TIME '12:34:56.1' >= TIME '12:34:56.2'", BOOLEAN, false);
        assertFunction("TIME '12:34:56.12' >= TIME '12:34:56.13'", BOOLEAN, false);
        assertFunction("TIME '12:34:56.123' >= TIME '12:34:56.124'", BOOLEAN, false);

        // false cases
        assertFunction("TIME '12:34:56' >= TIME '12:34:55'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.1' >= TIME '12:34:56.0'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.12' >= TIME '12:34:56.11'", BOOLEAN, true);
        assertFunction("TIME '12:34:56.123' >= TIME '12:34:56.122'", BOOLEAN, true);
    }

    @Test
    public void testAddIntervalDayToSecond()
    {
        assertFunction("TIME '12:34:56' + INTERVAL '1.123' SECOND", TIME, sqlTimeOf(12, 34, 57, 123, session));
        assertFunction("TIME '12:34:56.1' + INTERVAL '1.123' SECOND", TIME, sqlTimeOf(12, 34, 57, 223, session));

        assertFunction("INTERVAL '1.123' SECOND + TIME '12:34:56'", TIME, sqlTimeOf(12, 34, 57, 123, session));
        assertFunction("INTERVAL '1.123' SECOND + TIME '12:34:56.1'", TIME, sqlTimeOf(12, 34, 57, 223, session));

        // carry
        assertFunction("TIME '12:59:59' + INTERVAL '1' SECOND", TIME, sqlTimeOf(13, 00, 00, 000, session));
        assertFunction("TIME '12:59:59.999' + INTERVAL '0.001' SECOND", TIME, sqlTimeOf(13, 00, 00, 000, session));
    }

    @Test
    public void testSubtractIntervalDayToSecond()
    {
        assertFunction("TIME '12:34:56' - INTERVAL '1.123' SECOND", TIME, sqlTimeOf(12, 34, 54, 877, session));

        // borrow
        assertFunction("TIME '13:00:00' - INTERVAL '1' SECOND", TIME, sqlTimeOf(12, 59, 59, 000, session));
        assertFunction("TIME '13:00:00' - INTERVAL '0.001' SECOND", TIME, sqlTimeOf(12, 59, 59, 999, session));

        // wrap-around
        assertFunction("TIME '12:34:56' - INTERVAL '13' HOUR", TIME, sqlTimeOf(23, 34, 56, 000, session));
    }
}
