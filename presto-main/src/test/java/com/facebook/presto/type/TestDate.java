package com.facebook.presto.type;
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

import com.facebook.presto.Session;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static java.util.Locale.ENGLISH;
import static org.joda.time.DateTimeZone.UTC;

public class TestDate
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);

    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        Session session = Session.builder()
                .setUser("user")
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setTimeZoneKey(TIME_ZONE_KEY)
                .setLocale(ENGLISH)
                .build();
        functionAssertions = new FunctionAssertions(session);
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        long millis = new DateTime(2001, 1, 22, 0, 0, UTC).getMillis();
        assertFunction("DATE '2001-1-22'", new SqlDate((int) TimeUnit.MILLISECONDS.toDays(millis)));
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' = DATE '2001-1-22'", true);
        assertFunction("DATE '2001-1-22' = DATE '2001-1-22'", true);

        assertFunction("DATE '2001-1-22' = DATE '2001-1-23'", false);
        assertFunction("DATE '2001-1-22' = DATE '2001-1-11'", false);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' <> DATE '2001-1-23'", true);
        assertFunction("DATE '2001-1-22' <> DATE '2001-1-11'", true);

        assertFunction("DATE '2001-1-22' <> DATE '2001-1-22'", false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' < DATE '2001-1-23'", true);

        assertFunction("DATE '2001-1-22' < DATE '2001-1-22'", false);
        assertFunction("DATE '2001-1-22' < DATE '2001-1-20'", false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' <= DATE '2001-1-22'", true);
        assertFunction("DATE '2001-1-22' <= DATE '2001-1-23'", true);

        assertFunction("DATE '2001-1-22' <= DATE '2001-1-20'", false);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' > DATE '2001-1-11'", true);

        assertFunction("DATE '2001-1-22' > DATE '2001-1-22'", false);
        assertFunction("DATE '2001-1-22' > DATE '2001-1-23'", false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' >= DATE '2001-1-22'", true);
        assertFunction("DATE '2001-1-22' >= DATE '2001-1-11'", true);

        assertFunction("DATE '2001-1-22' >= DATE '2001-1-23'", false);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("DATE '2001-1-22' between DATE '2001-1-11' and DATE '2001-1-23'", true);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-11' and DATE '2001-1-22'", true);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-22' and DATE '2001-1-23'", true);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-22' and DATE '2001-1-22'", true);

        assertFunction("DATE '2001-1-22' between DATE '2001-1-11' and DATE '2001-1-12'", false);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-23' and DATE '2001-1-24'", false);
        assertFunction("DATE '2001-1-22' between DATE '2001-1-23' and DATE '2001-1-11'", false);
    }

    @Test
    public void testCastToTimestamp()
            throws Exception
    {
        assertFunction("cast(DATE '2001-1-22' as timestamp)",
                new SqlTimestamp(new DateTime(2001, 1, 22, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testCastToTimestampWithTimeZone()
            throws Exception
    {
        assertFunction("cast(DATE '2001-1-22' as timestamp with time zone)",
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testCastToSlice()
            throws Exception
    {
        assertFunction("cast(DATE '2001-1-22' as varchar)", "2001-01-22");
    }

    @Test
    public void testCastFromSlice()
            throws Exception
    {
        assertFunction("cast('2001-1-22' as date) = Date '2001-1-22'", true);
    }

    @Test
    public void testGreatest()
            throws Exception
    {
        int days = (int) TimeUnit.MILLISECONDS.toDays(new DateTime(2013, 3, 30, 0, 0, UTC).getMillis());
        assertFunction("greatest(DATE '2013-03-30', DATE '2012-05-23')", new SqlDate(days));
        assertFunction("greatest(DATE '2013-03-30', DATE '2012-05-23', DATE '2012-06-01')", new SqlDate(days));
    }

    @Test
    public void testLeast()
            throws Exception
    {
        int days = (int) TimeUnit.MILLISECONDS.toDays(new DateTime(2012, 5, 23, 0, 0, UTC).getMillis());
        assertFunction("least(DATE '2013-03-30', DATE '2012-05-23')", new SqlDate(days));
        assertFunction("least(DATE '2013-03-30', DATE '2012-05-23', DATE '2012-06-01')", new SqlDate(days));
    }
}
