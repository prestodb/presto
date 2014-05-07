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
import com.facebook.presto.spi.type.SqlIntervalDayTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestIntervalDayTime
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
        assertFunction("INTERVAL '12 10:45:32.123' DAY TO SECOND", new SqlIntervalDayTime(12, 10, 45, 32, 123));
        assertFunction("INTERVAL '12 10:45:32.12' DAY TO SECOND", new SqlIntervalDayTime(12, 10, 45, 32, 120));
        assertFunction("INTERVAL '12 10:45:32' DAY TO SECOND", new SqlIntervalDayTime(12, 10, 45, 32, 0));
        assertFunction("INTERVAL '12 10:45' DAY TO SECOND", new SqlIntervalDayTime(12, 10, 45, 0, 0));
        assertFunction("INTERVAL '12 10' DAY TO SECOND", new SqlIntervalDayTime(12, 10, 0, 0, 0));
        assertFunction("INTERVAL '12' DAY TO SECOND", new SqlIntervalDayTime(12, 0, 0, 0, 0));

        assertFunction("INTERVAL '12 10:45' DAY TO MINUTE", new SqlIntervalDayTime(12, 10, 45, 0, 0));
        assertFunction("INTERVAL '12 10' DAY TO MINUTE", new SqlIntervalDayTime(12, 10, 0, 0, 0));
        assertFunction("INTERVAL '12' DAY TO MINUTE", new SqlIntervalDayTime(12, 0, 0, 0, 0));

        assertFunction("INTERVAL '12 10' DAY TO HOUR", new SqlIntervalDayTime(12, 10, 0, 0, 0));
        assertFunction("INTERVAL '12' DAY TO HOUR", new SqlIntervalDayTime(12, 0, 0, 0, 0));

        assertFunction("INTERVAL '12' DAY", new SqlIntervalDayTime(12, 0, 0, 0, 0));

        assertFunction("INTERVAL '10:45:32.123' HOUR TO SECOND", new SqlIntervalDayTime(0, 10, 45, 32, 123));
        assertFunction("INTERVAL '10:45:32.12' HOUR TO SECOND", new SqlIntervalDayTime(0, 10, 45, 32, 120));
        assertFunction("INTERVAL '10:45:32' HOUR TO SECOND", new SqlIntervalDayTime(0, 10, 45, 32, 0));
        assertFunction("INTERVAL '10:45' HOUR TO SECOND", new SqlIntervalDayTime(0, 10, 45, 0, 0));
        assertFunction("INTERVAL '10' HOUR TO SECOND", new SqlIntervalDayTime(0, 10, 0, 0, 0));

        assertFunction("INTERVAL '10:45' HOUR TO MINUTE", new SqlIntervalDayTime(0, 10, 45, 0, 0));
        assertFunction("INTERVAL '10' HOUR TO MINUTE", new SqlIntervalDayTime(0, 10, 0, 0, 0));

        assertFunction("INTERVAL '10' HOUR", new SqlIntervalDayTime(0, 10, 0, 0, 0));

        assertFunction("INTERVAL '45:32.123' MINUTE TO SECOND", new SqlIntervalDayTime(0, 0, 45, 32, 123));
        assertFunction("INTERVAL '45:32.12' MINUTE TO SECOND", new SqlIntervalDayTime(0, 0, 45, 32, 120));
        assertFunction("INTERVAL '45:32' MINUTE TO SECOND", new SqlIntervalDayTime(0, 0, 45, 32, 0));
        assertFunction("INTERVAL '45' MINUTE TO SECOND", new SqlIntervalDayTime(0, 0, 45, 0, 0));

        assertFunction("INTERVAL '45' MINUTE", new SqlIntervalDayTime(0, 0, 45, 0, 0));

        assertFunction("INTERVAL '32.123' SECOND", new SqlIntervalDayTime(0, 0, 0, 32, 123));
        assertFunction("INTERVAL '32.12' SECOND", new SqlIntervalDayTime(0, 0, 0, 32, 120));
        assertFunction("INTERVAL '32' SECOND", new SqlIntervalDayTime(0, 0, 0, 32, 0));
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND + INTERVAL '3' SECOND", new SqlIntervalDayTime(6 * 1000));
        assertFunction("INTERVAL '6' DAY + INTERVAL '6' DAY", new SqlIntervalDayTime(12 * 24 * 60 * 60 * 1000));
        assertFunction("INTERVAL '3' SECOND + INTERVAL '6' DAY", new SqlIntervalDayTime((6 * 24 * 60 * 60 * 1000) + (3 * 1000)));
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("INTERVAL '6' SECOND - INTERVAL '3' SECOND", new SqlIntervalDayTime(3 * 1000));
        assertFunction("INTERVAL '9' DAY - INTERVAL '6' DAY", new SqlIntervalDayTime(3 * 24 * 60 * 60 * 1000));
        assertFunction("INTERVAL '3' SECOND - INTERVAL '6' DAY", new SqlIntervalDayTime((3 * 1000) - (6 * 24 * 60 * 60 * 1000)));
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("INTERVAL '6' SECOND * 2", new SqlIntervalDayTime(12 * 1000));
        assertFunction("2 * INTERVAL '6' SECOND", new SqlIntervalDayTime(12 * 1000));
        assertFunction("INTERVAL '1' SECOND * 2.5", new SqlIntervalDayTime(2500));
        assertFunction("2.5 * INTERVAL '1' SECOND", new SqlIntervalDayTime(2500));

        assertFunction("INTERVAL '6' DAY * 2", new SqlIntervalDayTime(12 * 24 * 60 * 60 * 1000));
        assertFunction("2 * INTERVAL '6' DAY", new SqlIntervalDayTime(12 * 24 * 60 * 60 * 1000));
        assertFunction("INTERVAL '1' DAY * 2.5", new SqlIntervalDayTime((long) (2.5 * 24 * 60 * 60 * 1000)));
        assertFunction("2.5 * INTERVAL '1' DAY", new SqlIntervalDayTime((long) (2.5 * 24 * 60 * 60 * 1000)));
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND / 2", new SqlIntervalDayTime(1500));
        assertFunction("INTERVAL '6' SECOND / 2.5", new SqlIntervalDayTime(2400));

        assertFunction("INTERVAL '3' DAY / 2", new SqlIntervalDayTime((long) (1.5 * 24 * 60 * 60 * 1000)));
        assertFunction("INTERVAL '4' DAY / 2.5", new SqlIntervalDayTime((long) (1.6 * 24 * 60 * 60 * 1000)));
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("- INTERVAL '3' SECOND", new SqlIntervalDayTime(-3 * 1000));
        assertFunction("- INTERVAL '6' DAY", new SqlIntervalDayTime(-6 * 24 * 60 * 60 * 1000));
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND = INTERVAL '3' SECOND", true);
        assertFunction("INTERVAL '6' DAY = INTERVAL '6' DAY", true);

        assertFunction("INTERVAL '3' SECOND = INTERVAL '4' SECOND", false);
        assertFunction("INTERVAL '7' DAY = INTERVAL '6' DAY", false);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND <> INTERVAL '4' SECOND", true);
        assertFunction("INTERVAL '6' DAY <> INTERVAL '7' DAY", true);

        assertFunction("INTERVAL '3' SECOND <> INTERVAL '3' SECOND", false);
        assertFunction("INTERVAL '6' DAY <> INTERVAL '6' DAY", false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND < INTERVAL '4' SECOND", true);
        assertFunction("INTERVAL '6' DAY < INTERVAL '7' DAY", true);

        assertFunction("INTERVAL '3' SECOND < INTERVAL '3' SECOND", false);
        assertFunction("INTERVAL '3' SECOND < INTERVAL '2' SECOND", false);
        assertFunction("INTERVAL '6' DAY < INTERVAL '6' DAY", false);
        assertFunction("INTERVAL '6' DAY < INTERVAL '5' DAY", false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND <= INTERVAL '4' SECOND", true);
        assertFunction("INTERVAL '3' SECOND <= INTERVAL '3' SECOND", true);
        assertFunction("INTERVAL '6' DAY <= INTERVAL '6' DAY", true);
        assertFunction("INTERVAL '6' DAY <= INTERVAL '7' DAY", true);

        assertFunction("INTERVAL '3' SECOND <= INTERVAL '2' SECOND", false);
        assertFunction("INTERVAL '6' DAY <= INTERVAL '5' DAY", false);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND > INTERVAL '2' SECOND", true);
        assertFunction("INTERVAL '6' DAY > INTERVAL '5' DAY", true);

        assertFunction("INTERVAL '3' SECOND > INTERVAL '3' SECOND", false);
        assertFunction("INTERVAL '3' SECOND > INTERVAL '4' SECOND", false);
        assertFunction("INTERVAL '6' DAY > INTERVAL '6' DAY", false);
        assertFunction("INTERVAL '6' DAY > INTERVAL '7' DAY", false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND >= INTERVAL '2' SECOND", true);
        assertFunction("INTERVAL '3' SECOND >= INTERVAL '3' SECOND", true);
        assertFunction("INTERVAL '6' DAY >= INTERVAL '5' DAY", true);
        assertFunction("INTERVAL '6' DAY >= INTERVAL '6' DAY", true);

        assertFunction("INTERVAL '3' SECOND >= INTERVAL '4' SECOND", false);
        assertFunction("INTERVAL '6' DAY >= INTERVAL '7' DAY", false);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND between INTERVAL '2' SECOND and INTERVAL '4' SECOND", true);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '3' SECOND and INTERVAL '4' SECOND", true);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '2' SECOND and INTERVAL '3' SECOND", true);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '3' SECOND and INTERVAL '3' SECOND", true);

        assertFunction("INTERVAL '3' SECOND between INTERVAL '4' SECOND and INTERVAL '5' SECOND", false);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '1' SECOND and INTERVAL '2' SECOND", false);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '4' SECOND and INTERVAL '2' SECOND", false);
    }

    @Test
    public void testCastToSlice()
            throws Exception
    {
        assertFunction("cast(INTERVAL '12 10:45:32.123' DAY TO SECOND as varchar)", "12 10:45:32.123");
        assertFunction("cast(INTERVAL '12 10:45:32.123' DAY TO SECOND as varchar)", new SqlIntervalDayTime(12, 10, 45, 32, 123).toString());
        assertFunction("cast(INTERVAL '12 10:45:32.12' DAY TO SECOND as varchar)", new SqlIntervalDayTime(12, 10, 45, 32, 120).toString());
        assertFunction("cast(INTERVAL '12 10:45:32' DAY TO SECOND as varchar)", new SqlIntervalDayTime(12, 10, 45, 32, 0).toString());
        assertFunction("cast(INTERVAL '12 10:45' DAY TO SECOND as varchar)", new SqlIntervalDayTime(12, 10, 45, 0, 0).toString());
        assertFunction("cast(INTERVAL '12 10' DAY TO SECOND as varchar)", new SqlIntervalDayTime(12, 10, 0, 0, 0).toString());
        assertFunction("cast(INTERVAL '12' DAY TO SECOND as varchar)", new SqlIntervalDayTime(12, 0, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '12 10:45' DAY TO MINUTE as varchar)", new SqlIntervalDayTime(12, 10, 45, 0, 0).toString());
        assertFunction("cast(INTERVAL '12 10' DAY TO MINUTE as varchar)", new SqlIntervalDayTime(12, 10, 0, 0, 0).toString());
        assertFunction("cast(INTERVAL '12' DAY TO MINUTE as varchar)", new SqlIntervalDayTime(12, 0, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '12 10' DAY TO HOUR as varchar)", new SqlIntervalDayTime(12, 10, 0, 0, 0).toString());
        assertFunction("cast(INTERVAL '12' DAY TO HOUR as varchar)", new SqlIntervalDayTime(12, 0, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '12' DAY as varchar)", new SqlIntervalDayTime(12, 0, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '10:45:32.123' HOUR TO SECOND as varchar)", new SqlIntervalDayTime(0, 10, 45, 32, 123).toString());
        assertFunction("cast(INTERVAL '10:45:32.12' HOUR TO SECOND as varchar)", new SqlIntervalDayTime(0, 10, 45, 32, 120).toString());
        assertFunction("cast(INTERVAL '10:45:32' HOUR TO SECOND as varchar)", new SqlIntervalDayTime(0, 10, 45, 32, 0).toString());
        assertFunction("cast(INTERVAL '10:45' HOUR TO SECOND as varchar)", new SqlIntervalDayTime(0, 10, 45, 0, 0).toString());
        assertFunction("cast(INTERVAL '10' HOUR TO SECOND as varchar)", new SqlIntervalDayTime(0, 10, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '10:45' HOUR TO MINUTE as varchar)", new SqlIntervalDayTime(0, 10, 45, 0, 0).toString());
        assertFunction("cast(INTERVAL '10' HOUR TO MINUTE as varchar)", new SqlIntervalDayTime(0, 10, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '10' HOUR as varchar)", new SqlIntervalDayTime(0, 10, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '45:32.123' MINUTE TO SECOND as varchar)", new SqlIntervalDayTime(0, 0, 45, 32, 123).toString());
        assertFunction("cast(INTERVAL '45:32.12' MINUTE TO SECOND as varchar)", new SqlIntervalDayTime(0, 0, 45, 32, 120).toString());
        assertFunction("cast(INTERVAL '45:32' MINUTE TO SECOND as varchar)", new SqlIntervalDayTime(0, 0, 45, 32, 0).toString());
        assertFunction("cast(INTERVAL '45' MINUTE TO SECOND as varchar)", new SqlIntervalDayTime(0, 0, 45, 0, 0).toString());

        assertFunction("cast(INTERVAL '45' MINUTE as varchar)", new SqlIntervalDayTime(0, 0, 45, 0, 0).toString());

        assertFunction("cast(INTERVAL '32.123' SECOND as varchar)", new SqlIntervalDayTime(0, 0, 0, 32, 123).toString());
        assertFunction("cast(INTERVAL '32.12' SECOND as varchar)", new SqlIntervalDayTime(0, 0, 0, 32, 120).toString());
        assertFunction("cast(INTERVAL '32' SECOND as varchar)", new SqlIntervalDayTime(0, 0, 0, 32, 0).toString());
    }
}
