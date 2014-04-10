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

import com.facebook.presto.operator.scalar.FunctionAssertions;
import org.joda.time.LocalDate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Date;

import static org.joda.time.DateTimeZone.UTC;

public class TestDate
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("DATE '2001-1-22'", new Date(new LocalDate(2001, 1, 22).toDateMidnight(UTC).getMillis()));
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
}
