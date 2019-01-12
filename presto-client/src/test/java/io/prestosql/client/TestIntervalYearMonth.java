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
package io.prestosql.client;

import org.testng.annotations.Test;

import static io.prestosql.client.IntervalYearMonth.formatMonths;
import static io.prestosql.client.IntervalYearMonth.parseMonths;
import static io.prestosql.client.IntervalYearMonth.toMonths;
import static org.testng.Assert.assertEquals;

public class TestIntervalYearMonth
{
    @Test
    public void testFormat()
    {
        assertMonths(0, "0-0");
        assertMonths(toMonths(0, 0), "0-0");

        assertMonths(3, "0-3");
        assertMonths(-3, "-0-3");
        assertMonths(toMonths(0, 3), "0-3");
        assertMonths(toMonths(0, -3), "-0-3");

        assertMonths(28, "2-4");
        assertMonths(-28, "-2-4");

        assertMonths(toMonths(2, 4), "2-4");
        assertMonths(toMonths(-2, -4), "-2-4");

        assertMonths(Integer.MAX_VALUE, "178956970-7");
        assertMonths(Integer.MIN_VALUE + 1, "-178956970-7");
        assertMonths(Integer.MIN_VALUE, "-178956970-8");
    }

    private static void assertMonths(int months, String formatted)
    {
        assertEquals(formatMonths(months), formatted);
        assertEquals(parseMonths(formatted), months);
    }

    @Test
    public void testMaxYears()
    {
        int years = Integer.MAX_VALUE / 12;
        assertEquals(toMonths(years, 0), years * 12);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testOverflow()
    {
        int days = (Integer.MAX_VALUE / 12) + 1;
        toMonths(days, 0);
    }
}
