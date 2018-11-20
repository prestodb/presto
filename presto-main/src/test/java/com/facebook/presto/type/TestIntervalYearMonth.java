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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.Type;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestIntervalYearMonth
        extends AbstractTestFunctions
{
    private static final int MAX_SHORT = Short.MAX_VALUE;

    @Test
    public void testObject()
    {
        assertEquals(new SqlIntervalYearMonth(3, 11), new SqlIntervalYearMonth(47));
        assertEquals(new SqlIntervalYearMonth(-3, -11), new SqlIntervalYearMonth(-47));

        assertEquals(new SqlIntervalYearMonth(MAX_SHORT, 0), new SqlIntervalYearMonth(393_204));
        assertEquals(new SqlIntervalYearMonth(MAX_SHORT, MAX_SHORT), new SqlIntervalYearMonth(425_971));

        assertEquals(new SqlIntervalYearMonth(-MAX_SHORT, 0), new SqlIntervalYearMonth(-393_204));
        assertEquals(new SqlIntervalYearMonth(-MAX_SHORT, -MAX_SHORT), new SqlIntervalYearMonth(-425_971));
    }

    @Test
    public void testLiteral()
    {
        assertLiteral("INTERVAL '124-30' YEAR TO MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(124, 30));
        assertLiteral("INTERVAL '124' YEAR TO MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(124, 0));

        assertLiteral("INTERVAL '124' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(124, 0));

        assertLiteral("INTERVAL '30' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(0, 30));

        assertLiteral(format("INTERVAL '%s' YEAR", MAX_SHORT), INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(MAX_SHORT, 0));
        assertLiteral(format("INTERVAL '%s' MONTH", MAX_SHORT), INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(0, MAX_SHORT));
        assertLiteral(format("INTERVAL '%s-%s' YEAR TO MONTH", MAX_SHORT, MAX_SHORT), INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(MAX_SHORT, MAX_SHORT));
    }

    private void assertLiteral(String projection, Type expectedType, SqlIntervalYearMonth expectedValue)
    {
        assertFunction(projection, expectedType, expectedValue);

        projection = projection.replace("INTERVAL '", "INTERVAL '-");
        expectedValue = new SqlIntervalYearMonth(-expectedValue.getMonths());
        assertFunction(projection, expectedType, expectedValue);
    }

    @Test
    public void testInvalidLiteral()
    {
        assertInvalidFunction("INTERVAL '124X' YEAR", "Invalid INTERVAL YEAR value: 124X");
        assertInvalidFunction("INTERVAL '124-30' YEAR", "Invalid INTERVAL YEAR value: 124-30");
        assertInvalidFunction("INTERVAL '124-X' YEAR TO MONTH", "Invalid INTERVAL YEAR TO MONTH value: 124-X");
        assertInvalidFunction("INTERVAL '124--30' YEAR TO MONTH", "Invalid INTERVAL YEAR TO MONTH value: 124--30");
        assertInvalidFunction("INTERVAL '--124--30' YEAR TO MONTH", "Invalid INTERVAL YEAR TO MONTH value: --124--30");
    }

    @Test
    public void testAdd()
    {
        assertFunction("INTERVAL '3' MONTH + INTERVAL '3' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(6));
        assertFunction("INTERVAL '6' YEAR + INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12 * 12));
        assertFunction("INTERVAL '3' MONTH + INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth((6 * 12) + (3)));
    }

    @Test
    public void testSubtract()
    {
        assertFunction("INTERVAL '6' MONTH - INTERVAL '3' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(3));
        assertFunction("INTERVAL '9' YEAR - INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(3 * 12));
        assertFunction("INTERVAL '3' MONTH - INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth((3) - (6 * 12)));
    }

    @Test
    public void testMultiply()
    {
        assertFunction("INTERVAL '6' MONTH * 2", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12));
        assertFunction("2 * INTERVAL '6' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12));
        assertFunction("INTERVAL '10' MONTH * 2.5", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(25));
        assertFunction("2.5 * INTERVAL '10' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(25));

        assertFunction("INTERVAL '6' YEAR * 2", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12 * 12));
        assertFunction("2 * INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(12 * 12));
        assertFunction("INTERVAL '1' YEAR * 2.5", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth((int) (2.5 * 12)));
        assertFunction("2.5 * INTERVAL '1' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth((int) (2.5 * 12)));
    }

    @Test
    public void testDivide()
    {
        assertFunction("INTERVAL '30' MONTH / 2", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(15));
        assertFunction("INTERVAL '60' MONTH / 2.5", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(24));

        assertFunction("INTERVAL '3' YEAR / 2", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(18));
        assertFunction("INTERVAL '4' YEAR / 4.8", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(10));
    }

    @Test
    public void testNegation()
    {
        assertFunction("- INTERVAL '3' MONTH", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(-3));
        assertFunction("- INTERVAL '6' YEAR", INTERVAL_YEAR_MONTH, new SqlIntervalYearMonth(-72));
    }

    @Test
    public void testEqual()
    {
        assertFunction("INTERVAL '3' MONTH = INTERVAL '3' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR = INTERVAL '6' YEAR", BOOLEAN, true);

        assertFunction("INTERVAL '3' MONTH = INTERVAL '4' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '7' YEAR = INTERVAL '6' YEAR", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("INTERVAL '3' MONTH <> INTERVAL '4' MONTH", BOOLEAN, true);
        assertFunction("INTERVAL '6' YEAR <> INTERVAL '7' YEAR", BOOLEAN, true);

        assertFunction("INTERVAL '3' MONTH <> INTERVAL '3' MONTH", BOOLEAN, false);
        assertFunction("INTERVAL '6' YEAR <> INTERVAL '6' YEAR", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
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
    {
        assertFunction("cast(INTERVAL '124-30' YEAR TO MONTH as varchar)", VARCHAR, "126-6");
        assertFunction("cast(INTERVAL '124-30' YEAR TO MONTH as varchar)", VARCHAR, new SqlIntervalYearMonth(124, 30).toString());

        assertFunction("cast(INTERVAL '124' YEAR TO MONTH as varchar)", VARCHAR, new SqlIntervalYearMonth(124, 0).toString());
        assertFunction("cast(INTERVAL '124' YEAR as varchar)", VARCHAR, new SqlIntervalYearMonth(124, 0).toString());

        assertFunction("cast(INTERVAL '30' MONTH as varchar)", VARCHAR, new SqlIntervalYearMonth(0, 30).toString());
    }

    @Test
    public void testIndeterminate()
    {
        assertOperator(INDETERMINATE, "cast(null as INTERVAL YEAR TO MONTH)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "INTERVAL '124' YEAR TO MONTH", BOOLEAN, false);
    }
}
