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
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestingSqlTime.sqlTimeOf;
import static com.facebook.presto.testing.TestingSqlTime.sqlTimestampOf;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static org.joda.time.DateTimeZone.UTC;

public class TestTimestampWithTimeZone
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKeyForOffset(6 * 60 + 9);
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    private static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(7 * 60 + 9);
    private static final DateTimeZone WEIRD_ZONE = getDateTimeZone(WEIRD_TIME_ZONE_KEY);
    private static final TimeZoneKey BERLIN_TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone BERLIN_ZONE = getDateTimeZone(BERLIN_TIME_ZONE_KEY);

    private Session session;
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        session = testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build();
        functionAssertions = new FunctionAssertions(session);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(functionAssertions);
        functionAssertions = null;
    }

    private void assertFunction(String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }

    @Test
    public void testLiteral()
    {
        assertFunction("TIMESTAMP '2001-01-02 03:04:05.321 +07:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 03:04:05 +07:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 5, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 03:04 +07:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 0, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 +07:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 0, 0, 0, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2001-1-2 3:4:5.321+07:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-2 3:4:5+07:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 5, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-2 3:4+07:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 0, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-2+07:09'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 0, 0, 0, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2001-01-02 03:04:05.321 Europe/Berlin'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 03:04:05 Europe/Berlin'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 5, 0, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 03:04 Europe/Berlin'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 0, 0, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-02 Europe/Berlin'",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 0, 0, 0, 0, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
    }

    @Test
    public void testEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' = TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' = TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' = TIMESTAMP '2001-1-11 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <> TIMESTAMP '2001-1-11 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <> TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <> TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' < TIMESTAMP '2001-1-23 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 03:04:05 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' < TIMESTAMP '2001-1-22 02:04:05'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' < TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' < TIMESTAMP '2001-1-20 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <= TIMESTAMP '2001-1-23 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <= TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 03:04:05 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' <= TIMESTAMP '2001-1-22 02:04:05'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' <= TIMESTAMP '2001-1-20 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 03:04:05.111 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.111 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.111'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' > TIMESTAMP '2001-1-11 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' > TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' > TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' > TIMESTAMP '2001-1-23 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 03:04:05.111 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.111 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.111'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' >= TIMESTAMP '2001-1-11 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' >= TIMESTAMP '2001-1-22 +07:09'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' >= TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 +07:09' >= TIMESTAMP '2001-1-23 +07:09'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.111 +07:09' and TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.111 +06:09' and TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.111' and TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.321 +07:09' and TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.321 +06:09' and TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.321' and TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.111 +07:09' and TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.111 +06:09' and TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.111' and TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.321 +07:09' and TIMESTAMP '2001-1-22 03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.321 +06:09' and TIMESTAMP '2001-1-22 02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.321' and TIMESTAMP '2001-1-22 02:04:05.321'", BOOLEAN, true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.322 +07:09' and TIMESTAMP '2001-1-22 03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.322 +06:09' and TIMESTAMP '2001-1-22 02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.322' and TIMESTAMP '2001-1-22 02:04:05.333'", BOOLEAN, false);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.311 +07:09' and TIMESTAMP '2001-1-22 03:04:05.312 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.311 +06:09' and TIMESTAMP '2001-1-22 02:04:05.312 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.311' and TIMESTAMP '2001-1-22 02:04:05.312'", BOOLEAN, false);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 03:04:05.333 +07:09' and TIMESTAMP '2001-1-22 03:04:05.111 +07:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.333 +06:09' and TIMESTAMP '2001-1-22 02:04:05.111 +06:09'", BOOLEAN, false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321 +07:09' between TIMESTAMP '2001-1-22 02:04:05.333' and TIMESTAMP '2001-1-22 02:04:05.111'", BOOLEAN, false);
    }

    @Test
    public void testCastToDate()
            throws Exception
    {
        long millis = new DateTime(2001, 1, 22, 0, 0, UTC).getMillis();
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as date)", DATE, new SqlDate((int) TimeUnit.MILLISECONDS.toDays(millis)));
    }

    @Test
    public void testCastToTime()
            throws Exception
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as time)",
                TIME,
                sqlTimeOf(3, 4, 5, 321, WEIRD_ZONE, session.getTimeZoneKey(), session.toConnectorSession()));
    }

    @Test
    public void testCastToTimeWithTimeZone()
            throws Exception
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as time with time zone)",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testCastToTimestamp()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 22, 3, 4, 5, 321, WEIRD_ZONE, session.getTimeZoneKey(), session.toConnectorSession()));
    }

    @Test
    public void testCastToSlice()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as varchar)", VARCHAR, "2001-01-22 03:04:05.321 +07:09");
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05 +07:09' as varchar)", VARCHAR, "2001-01-22 03:04:05.000 +07:09");
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04 +07:09' as varchar)", VARCHAR, "2001-01-22 03:04:00.000 +07:09");
        assertFunction("cast(TIMESTAMP '2001-1-22 +07:09' as varchar)", VARCHAR, "2001-01-22 00:00:00.000 +07:09");
    }

    @Test
    public void testCastFromSlice()
    {
        assertFunction("cast('2001-1-22 03:04:05.321' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04:05' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("cast('2001-1-22 03:04:05.321 +07:09' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04:05 +07:09' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04 +07:09' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 0, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 +07:09' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 0, 0, 0, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));

        assertFunction("cast('2001-1-22 03:04:05.321 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04:05 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 0, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 0, 0, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 0, 0, 0, 0, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));

        assertFunction("cast('\n\t 2001-1-22 03:04:05.321 Europe/Berlin' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('2001-1-22 03:04:05.321 Europe/Berlin \t\n' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
        assertFunction("cast('\n\t 2001-1-22 03:04:05.321 Europe/Berlin \t\n' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 321, BERLIN_ZONE).getMillis(), BERLIN_TIME_ZONE_KEY));
    }

    @Test
    public void testGreatest()
            throws Exception
    {
        assertFunction(
                "greatest(TIMESTAMP '2002-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 01:04:05.321 +02:09', TIMESTAMP '2000-01-02 01:04:05.321 +02:09')",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2002, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction(
                "greatest(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 04:04:05.321 +10:09')",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testLeast()
            throws Exception
    {
        assertFunction(
                "least(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 01:04:05.321 +02:09', TIMESTAMP '2002-01-02 01:04:05.321 +02:09')",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction(
                "least(TIMESTAMP '2001-01-02 03:04:05.321 +07:09', TIMESTAMP '2001-01-02 01:04:05.321 +02:09')",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 2, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }
}
