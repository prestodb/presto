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
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestTimeWithTimeZone
{
    private static final DateTimeZone WEIRD_ZONE = DateTimeZone.forOffsetHoursMinutes(7, 9);
    private static final TimeZoneKey WEIRD_TIME_ZONE_KEY = getTimeZoneKeyForOffset(7 * 60 + 9);

    private Session session;
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        session = testSessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("+06:09"))
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
        assertFunction("TIME '03:04:05.321 +07:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04:05 +07:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '03:04 +07:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 0, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));

        assertFunction("TIME '3:4:5.321+07:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '3:4:5+07:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
        assertFunction("TIME '3:4+07:09'",
                TIME_WITH_TIME_ZONE,
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 0, 0, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testEqual()
    {
        assertFunction("TIME '03:04:05.321 +07:09' = TIME '03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' = TIME '02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' = TIME '02:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' = TIME '03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' = TIME '02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' = TIME '02:04:05.333'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("TIME '03:04:05.321 +07:09' <> TIME '03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' <> TIME '02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' <> TIME '02:04:05.333'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' <> TIME '03:04:05.321 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' <> TIME '02:04:05.321 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' <> TIME '02:04:05.321'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("TIME '03:04:05.321 +07:09' < TIME '03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' < TIME '02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' < TIME '02:04:05.333'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' < TIME '03:04:05.321 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' < TIME '02:04:05.321 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' < TIME '02:04:05.321'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' < TIME '03:04:05 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' < TIME '02:04:05 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' < TIME '02:04:05'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("TIME '03:04:05.321 +07:09' <= TIME '03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' <= TIME '02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' <= TIME '02:04:05.333'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' <= TIME '03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' <= TIME '02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' <= TIME '02:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' <= TIME '03:04:05 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' <= TIME '02:04:05 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' <= TIME '02:04:05'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("TIME '03:04:05.321 +07:09' > TIME '03:04:05.111 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' > TIME '02:04:05.111 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' > TIME '02:04:05.111'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' > TIME '03:04:05.321 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' > TIME '02:04:05.321 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' > TIME '02:04:05.321'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' > TIME '03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' > TIME '02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' > TIME '02:04:05.333'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("TIME '03:04:05.321 +07:09' >= TIME '03:04:05.111 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' >= TIME '02:04:05.111 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' >= TIME '02:04:05.111'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' >= TIME '03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' >= TIME '02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' >= TIME '02:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' >= TIME '03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' >= TIME '02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' >= TIME '02:04:05.333'", BOOLEAN, false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '03:04:05.111 +07:09' and TIME '03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.111 +06:09' and TIME '02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.111' and TIME '02:04:05.333'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' between TIME '03:04:05.321 +07:09' and TIME '03:04:05.333 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.321 +06:09' and TIME '02:04:05.333 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.321' and TIME '02:04:05.333'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' between TIME '03:04:05.111 +07:09' and TIME '03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.111 +06:09' and TIME '02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.111' and TIME '02:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' between TIME '03:04:05.321 +07:09' and TIME '03:04:05.321 +07:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.321 +06:09' and TIME '02:04:05.321 +06:09'", BOOLEAN, true);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.321' and TIME '02:04:05.321'", BOOLEAN, true);

        assertFunction("TIME '03:04:05.321 +07:09' between TIME '03:04:05.322 +07:09' and TIME '03:04:05.333 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.322 +06:09' and TIME '02:04:05.333 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.322' and TIME '02:04:05.333'", BOOLEAN, false);

        assertFunction("TIME '03:04:05.321 +07:09' between TIME '03:04:05.311 +07:09' and TIME '03:04:05.312 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.311 +06:09' and TIME '02:04:05.312 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.311' and TIME '02:04:05.312'", BOOLEAN, false);

        assertFunction("TIME '03:04:05.321 +07:09' between TIME '03:04:05.333 +07:09' and TIME '03:04:05.111 +07:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.333 +06:09' and TIME '02:04:05.111 +06:09'", BOOLEAN, false);
        assertFunction("TIME '03:04:05.321 +07:09' between TIME '02:04:05.333' and TIME '02:04:05.111'", BOOLEAN, false);
    }

    @Test
    public void testCastToTime()
            throws Exception
    {
        assertFunction("cast(TIME '03:04:05.321 +07:09' as time)",
                TIME,
                new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), session.getTimeZoneKey()));
    }

    @Test
    public void testCastToTimestamp()
            throws Exception
    {
        assertFunction("cast(TIME '03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP,
                new SqlTimestamp(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), session.getTimeZoneKey()));
    }

    @Test
    public void testCastToTimestampWithTimeZone()
            throws Exception
    {
        assertFunction("cast(TIME '03:04:05.321 +07:09' as timestamp with time zone)",
                TIMESTAMP_WITH_TIME_ZONE,
                new SqlTimestampWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, WEIRD_ZONE).getMillis(), WEIRD_TIME_ZONE_KEY));
    }

    @Test
    public void testCastToSlice()
    {
        assertFunction("cast(TIME '03:04:05.321 +07:09' as varchar)", VARCHAR, "03:04:05.321 +07:09");
        assertFunction("cast(TIME '03:04:05 +07:09' as varchar)", VARCHAR, "03:04:05.000 +07:09");
        assertFunction("cast(TIME '03:04 +07:09' as varchar)", VARCHAR, "03:04:00.000 +07:09");
    }
}
