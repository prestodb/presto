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
package io.prestosql.type;

import io.prestosql.Session;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.operator.scalar.FunctionAssertions;
import io.prestosql.spi.type.SqlTimeWithTimeZone;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.analyzer.SemanticErrorCode;
import io.prestosql.testing.TestingSession;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.prestosql.util.DateTimeZoneIndex.getDateTimeZone;

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
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), DATE_TIME_ZONE.toTimeZone()));
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
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, dateTimeZoneThatChangedSince1970).getMillis(), dateTimeZoneThatChangedSince1970.toTimeZone()));
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
}
