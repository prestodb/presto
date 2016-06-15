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

import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.SqlIntervalYearMonth;
import com.facebook.presto.spi.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestIntervalYearMonth
{
    private static final int MAX_INT = Integer.MAX_VALUE;

    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    private void assertFunction(String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }

    @Test
    public void testObject()
            throws Exception
    {
        assertEquals(new SqlIntervalYearMonth(3, 11), new SqlIntervalYearMonth(47));
        assertEquals(new SqlIntervalYearMonth(-3, -11), new SqlIntervalYearMonth(-47));

        assertEquals(new SqlIntervalYearMonth(MAX_INT, 0), new SqlIntervalYearMonth(25_769_803_764L));
        assertEquals(new SqlIntervalYearMonth(MAX_INT, MAX_INT), new SqlIntervalYearMonth(27_917_287_411L));

        assertEquals(new SqlIntervalYearMonth(-MAX_INT, 0), new SqlIntervalYearMonth(-25_769_803_764L));
        assertEquals(new SqlIntervalYearMonth(-MAX_INT, -MAX_INT), new SqlIntervalYearMonth(-27_917_287_411L));
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("INTERVAL '124-30' YEAR TO MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(124, 30));
        assertFunction("INTERVAL '124' YEAR TO MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(124, 0));

        assertFunction("INTERVAL '124' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(124, 0));

        assertFunction("INTERVAL '30' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(0, 30));

        assertFunction(format("INTERVAL '%s' YEAR", MAX_INT), INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(MAX_INT, 0));
        assertFunction(format("INTERVAL '%s' MONTH", MAX_INT), INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(0, MAX_INT));
        assertFunction(format("INTERVAL '%s-%s' YEAR TO MONTH", MAX_INT, MAX_INT), INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(MAX_INT, MAX_INT));
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("INTERVAL '3' MONTH + INTERVAL '3' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(6));
        assertFunction("INTERVAL '6' YEAR + INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12 * 12));
        assertFunction("INTERVAL '3' MONTH + INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth((6 * 12) + (3)));
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("INTERVAL '6' MONTH - INTERVAL '3' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(3));
        assertFunction("INTERVAL '9' YEAR - INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(3 * 12));
        assertFunction("INTERVAL '3' MONTH - INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth((3) - (6 * 12)));
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("INTERVAL '6' MONTH * 2", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12));
        assertFunction("2 * INTERVAL '6' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12));
        assertFunction("INTERVAL '10' MONTH * 2.5", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(25));
        assertFunction("2.5 * INTERVAL '10' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(25));

        assertFunction("INTERVAL '6' YEAR * 2", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12 * 12));
        assertFunction("2 * INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12 * 12));
        assertFunction("INTERVAL '1' YEAR * 2.5", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth((long) (2.5 * 12)));
        assertFunction("2.5 * INTERVAL '1' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth((long) (2.5 * 12)));
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("INTERVAL '30' MONTH / 2", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(15));
        assertFunction("INTERVAL '60' MONTH / 2.5", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(24));

        assertFunction("INTERVAL '3' YEAR / 2", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(18));
        assertFunction("INTERVAL '4' YEAR / 4.8", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(10));
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("- INTERVAL '3' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(-3));
        assertFunction("- INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(-72));
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' MONTH = INTERVAL '3' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR = INTERVAL '6' YEAR", BOOLEAN, true);

        assertFunction("INTERVAL '3' MONTH = INTERVAL '4' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '7' YEAR = INTERVAL '6' YEAR", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' MONTH <> INTERVAL '4' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR <> INTERVAL '7' YEAR", BOOLEAN, true);

        assertFunction("INTERVAL '3' MONTH <> INTERVAL '3' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '6' YEAR <> INTERVAL '6' YEAR", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("INTERVAL '3' MONTH < INTERVAL '4' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR < INTERVAL '7' YEAR", BOOLEAN, true);

        assertFunction("INTERVAL '3' MONTH < INTERVAL '3' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '3' MONTH < INTERVAL '2' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '6' YEAR < INTERVAL '6' YEAR", BOOLEAN, false);
        assertFunction("INTERVAL '6' YEAR < INTERVAL '5' YEAR", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' MONTH <= INTERVAL '4' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '3' MONTH <= INTERVAL '3' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR <= INTERVAL '6' YEAR", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR <= INTERVAL '7' YEAR", BOOLEAN, true);

        assertFunction("INTERVAL '3' MONTH <= INTERVAL '2' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '6' YEAR <= INTERVAL '5' YEAR", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("INTERVAL '3' MONTH > INTERVAL '2' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR > INTERVAL '5' YEAR", BOOLEAN, true);

        assertFunction("INTERVAL '3' MONTH > INTERVAL '3' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '3' MONTH > INTERVAL '4' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '6' YEAR > INTERVAL '6' YEAR", BOOLEAN, false);
        assertFunction("INTERVAL '6' YEAR > INTERVAL '7' YEAR", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' MONTH >= INTERVAL '2' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '3' MONTH >= INTERVAL '3' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR >= INTERVAL '5' YEAR", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR >= INTERVAL '6' YEAR", BOOLEAN, true);

        assertFunction("INTERVAL '3' MONTH >= INTERVAL '4' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '6' YEAR >= INTERVAL '7' YEAR", BOOLEAN, false);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("INTERVAL '3' MONTH between INTERVAL '2' MONTH and INTERVAL '4' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '3' MONTH between INTERVAL '3' MONTH and INTERVAL '4' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '3' MONTH between INTERVAL '2' MONTH and INTERVAL '3' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '3' MONTH between INTERVAL '3' MONTH and INTERVAL '3' MONTH", BOOLEAN, true);

        assertFunction("INTERVAL '3' MONTH between INTERVAL '4' MONTH and INTERVAL '5' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '3' MONTH between INTERVAL '1' MONTH and INTERVAL '2' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '3' MONTH between INTERVAL '4' MONTH and INTERVAL '2' MONTH", BOOLEAN, false);
    }

    @Test
    public void testCastToSlice()
            throws Exception
    {
        assertFunction("cast(INTERVAL '124-30' YEAR TO MONTH as varchar)", VARCHAR, "126-6");
        assertFunction("cast(INTERVAL '124-30' YEAR TO MONTH as varchar)", VARCHAR, new SqlIntervalYearMonth(124, 30).toString());

        assertFunction("cast(INTERVAL '124' YEAR TO MONTH as varchar)", VARCHAR, new SqlIntervalYearMonth(124, 0).toString());
        assertFunction("cast(INTERVAL '124' YEAR as varchar)", VARCHAR, new SqlIntervalYearMonth(124, 0).toString());

        assertFunction("cast(INTERVAL '30' MONTH as varchar)", VARCHAR, new SqlIntervalYearMonth(0, 30).toString());
    }
}
