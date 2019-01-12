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

import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlTimeWithTimeZone;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.analyzer.SemanticErrorCode;
import io.prestosql.testing.TestingSession;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimeOf;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.prestosql.util.DateTimeZoneIndex.getDateTimeZone;
import static org.joda.time.DateTimeZone.UTC;

public abstract class TestTimestampBase
        extends AbstractTestFunctions
{
    protected static final TimeZoneKey TIME_ZONE_KEY = TestingSession.DEFAULT_TIME_ZONE_KEY;
    protected static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    protected static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(7 * 60 + 9);
    protected static final DateTimeZone WEIRD_ZONE = getDateTimeZone(WEIRD_TIME_ZONE_KEY);
    protected static final TimeZoneKey ORAL_TIME_ZONE_KEY = getTimeZoneKey("Asia/Oral");
    protected static final DateTimeZone ORAL_ZONE = getDateTimeZone(ORAL_TIME_ZONE_KEY);

    protected TestTimestampBase(boolean legacyTimestamp)
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", String.valueOf(legacyTimestamp))
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build());
    }

    @Test
    public void testSubtract()
    {
        functionAssertions.assertFunctionString("TIMESTAMP '2017-03-30 14:15:16.432' - TIMESTAMP '2016-03-29 03:04:05.321'",
                INTERVAL_DAY_TIME,
                "366 11:11:11.111");

        functionAssertions.assertFunctionString("TIMESTAMP '2016-03-29 03:04:05.321' - TIMESTAMP '2017-03-30 14:15:16.432'",
                INTERVAL_DAY_TIME,
                "-366 11:11:11.111");
    }

    @Test
    public void testLiteral()
    {
        assertFunction("TIMESTAMP '2013-03-30 01:05'", TIMESTAMP, sqlTimestampOf(2013, 3, 30, 1, 5, 0, 0, session));
        assertFunction("TIMESTAMP '2013-03-30 02:05'", TIMESTAMP, sqlTimestampOf(2013, 3, 30, 2, 5, 0, 0, session));
        assertFunction("TIMESTAMP '2013-03-30 03:05'", TIMESTAMP, sqlTimestampOf(2013, 3, 30, 3, 5, 0, 0, session));

        assertFunction("TIMESTAMP '2001-01-22 03:04:05.321'", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));
        assertFunction("TIMESTAMP '2001-01-22 03:04:05'", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 3, 4, 5, 0, session));
        assertFunction("TIMESTAMP '2001-01-22 03:04'", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 3, 4, 0, 0, session));
        assertFunction("TIMESTAMP '2001-01-22'", TIMESTAMP, sqlTimestampOf(2001, 1, 22, 0, 0, 0, 0, session));

        assertFunction("TIMESTAMP '2001-1-2 3:4:5.321'", TIMESTAMP, sqlTimestampOf(2001, 1, 2, 3, 4, 5, 321, session));
        assertFunction("TIMESTAMP '2001-1-2 3:4:5'", TIMESTAMP, sqlTimestampOf(2001, 1, 2, 3, 4, 5, 0, session));
        assertFunction("TIMESTAMP '2001-1-2 3:4'", TIMESTAMP, sqlTimestampOf(2001, 1, 2, 3, 4, 0, 0, session));
        assertFunction("TIMESTAMP '2001-1-2'", TIMESTAMP, sqlTimestampOf(2001, 1, 2, 0, 0, 0, 0, session));

        assertInvalidFunction("TIMESTAMP 'text'", SemanticErrorCode.INVALID_LITERAL, "line 1:1: 'text' is not a valid timestamp literal");
    }

    @Test
    public void testEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' = TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' = TIMESTAMP '2001-1-22'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' = TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' = TIMESTAMP '2001-1-11'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <> TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' <> TIMESTAMP '2001-1-11'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <> TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' <> TIMESTAMP '2001-1-22'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' < TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' < TIMESTAMP '2001-1-23'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' < TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' < TIMESTAMP '2001-1-22 03:04:05'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' < TIMESTAMP '2001-1-22'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' < TIMESTAMP '2001-1-20'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <= TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <= TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' <= TIMESTAMP '2001-1-23'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' <= TIMESTAMP '2001-1-22'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <= TIMESTAMP '2001-1-22 03:04:05'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' <= TIMESTAMP '2001-1-20'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' > TIMESTAMP '2001-1-22 03:04:05.111'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' > TIMESTAMP '2001-1-11'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' > TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' > TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' > TIMESTAMP '2001-1-22'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' > TIMESTAMP '2001-1-23'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' >= TIMESTAMP '2001-1-22 03:04:05.111'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' >= TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' >= TIMESTAMP '2001-1-11'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22' >= TIMESTAMP '2001-1-22'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' >= TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22' >= TIMESTAMP '2001-1-23'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.111' and TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.321' and TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.111' and TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.321' and TIMESTAMP '2001-1-22 03:04:05.321'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.322' and TIMESTAMP '2001-1-22 03:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.311' and TIMESTAMP '2001-1-22 03:04:05.312'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.333' and TIMESTAMP '2001-1-22 03:04:05.111'", BOOLEAN, false);
    }

    @Test
    public void testCastToDate()
    {
        long millis = new DateTime(2001, 1, 22, 0, 0, UTC).getMillis();
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as date)", DATE, new SqlDate((int) TimeUnit.MILLISECONDS.toDays(millis)));
    }

    @Test
    public void testCastToTime()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as time)", TIME, sqlTimeOf(3, 4, 5, 321, session));
    }

    @Test
    public void testCastToTimeWithTimeZone()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        functionAssertions.assertFunctionString("cast(TIMESTAMP '2001-1-22 03:04:05.321' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                "03:04:05.321 " + DATE_TIME_ZONE.getID());
    }

    @Test
    public void testCastToTimestampWithTimeZone()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), DATE_TIME_ZONE.toTimeZone()));
        functionAssertions.assertFunctionString("cast(TIMESTAMP '2001-1-22 03:04:05.321' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                "2001-01-22 03:04:05.321 " + DATE_TIME_ZONE.getID());
    }

    @Test
    public void testCastToSlice()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as varchar)", VARCHAR, "2001-01-22 03:04:05.321");
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05' as varchar)", VARCHAR, "2001-01-22 03:04:05.000");
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04' as varchar)", VARCHAR, "2001-01-22 03:04:00.000");
        assertFunction("cast(TIMESTAMP '2001-1-22' as varchar)", VARCHAR, "2001-01-22 00:00:00.000");
    }

    @Test
    public void testCastFromSlice()
    {
        assertFunction("cast('2001-1-22 03:04:05.321' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));
        assertFunction("cast('2001-1-22 03:04:05' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 0, session));
        assertFunction("cast('2001-1-22 03:04' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 0, 0, session));
        assertFunction("cast('2001-1-22' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 0, 0, 0, 0, session));
        assertFunction("cast('\n\t 2001-1-22 03:04:05.321' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));
        assertFunction("cast('2001-1-22 03:04:05.321 \t\n' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));
        assertFunction("cast('\n\t 2001-1-22 03:04:05.321 \t\n' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, session));
    }

    @Test
    public void testGreatest()
    {
        assertFunction("greatest(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05')",
                TIMESTAMP,
                sqlTimestampOf(2013, 3, 30, 1, 5, 0, 0, session));
        assertFunction("greatest(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05', TIMESTAMP '2012-05-01 01:05')",
                TIMESTAMP,
                sqlTimestampOf(2013, 3, 30, 1, 5, 0, 0, session));
    }

    @Test
    public void testLeast()
    {
        assertFunction("least(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05')",
                TIMESTAMP,
                sqlTimestampOf(2012, 3, 30, 1, 5, 0, 0, session));
        assertFunction("least(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05', TIMESTAMP '2012-05-01 01:05')",
                TIMESTAMP,
                sqlTimestampOf(2012, 3, 30, 1, 5, 0, 0, session));
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as TIMESTAMP)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "TIMESTAMP '2012-03-30 01:05'", BOOLEAN, false);
    }
}
