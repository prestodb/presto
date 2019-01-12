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
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.util.DateTimeZoneIndex.getDateTimeZone;
import static org.joda.time.DateTimeZone.UTC;

public abstract class TestDateBase
        extends AbstractTestFunctions
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);

    protected TestDateBase(boolean legacyTimestamp)
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", String.valueOf(legacyTimestamp))
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build());
    }

    @Test
    public void testLiteral()
    {
        long millis = new DateTime(2001, 1, 22, 0, 0, UTC).getMillis();
        assertFunction("DATE '2001-1-22'", DATE, new SqlDate((int) TimeUnit.MILLISECONDS.toDays(millis)));
    }

    @Test
    public void testEqual()
    {
        assertFunction("DATE '2001-1-22' = DATE '2001-1-22'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' = DATE '2001-1-22'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' = DATE '2001-1-23'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' = DATE '2001-1-11'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("DATE '2001-1-22' <> DATE '2001-1-23'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' <> DATE '2001-1-11'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' <> DATE '2001-1-22'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("DATE '2001-1-22' < DATE '2001-1-23'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' < DATE '2001-1-22'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' < DATE '2001-1-20'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("DATE '2001-1-22' <= DATE '2001-1-22'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' <= DATE '2001-1-23'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' <= DATE '2001-1-20'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("DATE '2001-1-22' > DATE '2001-1-11'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' > DATE '2001-1-22'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' > DATE '2001-1-23'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("DATE '2001-1-22' >= DATE '2001-1-22'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' >= DATE '2001-1-11'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' >= DATE '2001-1-23'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("DATE '2001-1-22' between DATE '2001-1-11' and DATE '2001-1-23'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-11' and DATE '2001-1-22'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-22' and DATE '2001-1-23'", BOOLEAN, true);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-22' and DATE '2001-1-22'", BOOLEAN, true);

        assertFunction("DATE '2001-1-22' between DATE '2001-1-11' and DATE '2001-1-12'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-23' and DATE '2001-1-24'", BOOLEAN, false);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-23' and DATE '2001-1-11'", BOOLEAN, false);
    }

    @Test
    public void testCastToTimestamp()
    {
        assertFunction("cast(DATE '2001-1-22' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 0, 0, 0, 0, session));
    }

    @Test
    public void testCastToTimestampWithTimeZone()
    {
        assertFunction("cast(DATE '2001-1-22' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testCastToSlice()
    {
        assertFunction("cast(DATE '2001-1-22' as varchar)", VARCHAR, "2001-01-22");
    }

    @Test
    public void testCastFromSlice()
    {
        assertFunction("cast('2001-1-22' as date) = Date '2001-1-22'", BOOLEAN, true);
        assertFunction("cast('\n\t 2001-1-22' as date) = Date '2001-1-22'", BOOLEAN, true);
        assertFunction("cast('2001-1-22 \t\n' as date) = Date '2001-1-22'", BOOLEAN, true);
        assertFunction("cast('\n\t 2001-1-22 \t\n' as date) = Date '2001-1-22'", BOOLEAN, true);
    }

    @Test
    public void testGreatest()
    {
        int days = (int) TimeUnit.MILLISECONDS.toDays(new DateTime(2013, 3, 30, 0, 0, UTC).getMillis());
        assertFunction("greatest(DATE '2013-03-30', DATE '2012-05-23')", DATE, new SqlDate(days));
        assertFunction("greatest(DATE '2013-03-30', DATE '2012-05-23', DATE '2012-06-01')", DATE, new SqlDate(days));
    }

    @Test
    public void testLeast()
    {
        int days = (int) TimeUnit.MILLISECONDS.toDays(new DateTime(2012, 5, 23, 0, 0, UTC).getMillis());
        assertFunction("least(DATE '2013-03-30', DATE '2012-05-23')", DATE, new SqlDate(days));
        assertFunction("least(DATE '2013-03-30', DATE '2012-05-23', DATE '2012-06-01')", DATE, new SqlDate(days));
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as DATE)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "DATE '2013-10-27'", BOOLEAN, false);
    }
}
