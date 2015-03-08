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
import com.facebook.presto.spi.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestIntervalDayTime
{
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
    public void testLiteral()
            throws Exception
    {
        assertFunction("INTERVAL '12 10:45:32.123' DAY TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 10, 45, 32, 123));
        assertFunction("INTERVAL '12 10:45:32.12' DAY TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 10, 45, 32, 120));
        assertFunction("INTERVAL '12 10:45:32' DAY TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 10, 45, 32, 0));
        assertFunction("INTERVAL '12 10:45' DAY TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 10, 45, 0, 0));
        assertFunction("INTERVAL '12 10' DAY TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 10, 0, 0, 0));
        assertFunction("INTERVAL '12' DAY TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 0, 0, 0, 0));

        assertFunction("INTERVAL '12 10:45' DAY TO MINUTE", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 10, 45, 0, 0));
        assertFunction("INTERVAL '12 10' DAY TO MINUTE", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 10, 0, 0, 0));
        assertFunction("INTERVAL '12' DAY TO MINUTE", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 0, 0, 0, 0));

        assertFunction("INTERVAL '12 10' DAY TO HOUR", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 10, 0, 0, 0));
        assertFunction("INTERVAL '12' DAY TO HOUR", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 0, 0, 0, 0));

        assertFunction("INTERVAL '12' DAY", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12, 0, 0, 0, 0));

        assertFunction("INTERVAL '10:45:32.123' HOUR TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 10, 45, 32, 123));
        assertFunction("INTERVAL '10:45:32.12' HOUR TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 10, 45, 32, 120));
        assertFunction("INTERVAL '10:45:32' HOUR TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 10, 45, 32, 0));
        assertFunction("INTERVAL '10:45' HOUR TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 10, 45, 0, 0));
        assertFunction("INTERVAL '10' HOUR TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 10, 0, 0, 0));

        assertFunction("INTERVAL '10:45' HOUR TO MINUTE", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 10, 45, 0, 0));
        assertFunction("INTERVAL '10' HOUR TO MINUTE", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 10, 0, 0, 0));

        assertFunction("INTERVAL '10' HOUR", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 10, 0, 0, 0));

        assertFunction("INTERVAL '45:32.123' MINUTE TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 45, 32, 123));
        assertFunction("INTERVAL '45:32.12' MINUTE TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 45, 32, 120));
        assertFunction("INTERVAL '45:32' MINUTE TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 45, 32, 0));
        assertFunction("INTERVAL '45' MINUTE TO SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 45, 0, 0));

        assertFunction("INTERVAL '45' MINUTE", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 45, 0, 0));

        assertFunction("INTERVAL '32.123' SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 32, 123));
        assertFunction("INTERVAL '32.12' SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 32, 120));
        assertFunction("INTERVAL '32' SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 32, 0));
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND + INTERVAL '3' SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(6 * 1000));
        assertFunction("INTERVAL '6' DAY + INTERVAL '6' DAY", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12 * 24 * 60 * 60 * 1000));
        assertFunction("INTERVAL '3' SECOND + INTERVAL '6' DAY", INTERVAL_DAY_TIME, new SqlIntervalDayTime((6 * 24 * 60 * 60 * 1000) + (3 * 1000)));
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("INTERVAL '6' SECOND - INTERVAL '3' SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(3 * 1000));
        assertFunction("INTERVAL '9' DAY - INTERVAL '6' DAY", INTERVAL_DAY_TIME, new SqlIntervalDayTime(3 * 24 * 60 * 60 * 1000));
        assertFunction("INTERVAL '3' SECOND - INTERVAL '6' DAY", INTERVAL_DAY_TIME, new SqlIntervalDayTime((3 * 1000) - (6 * 24 * 60 * 60 * 1000)));
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("INTERVAL '6' SECOND * 2", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12 * 1000));
        assertFunction("2 * INTERVAL '6' SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12 * 1000));
        assertFunction("INTERVAL '1' SECOND * 2.5", INTERVAL_DAY_TIME, new SqlIntervalDayTime(2500));
        assertFunction("2.5 * INTERVAL '1' SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(2500));

        assertFunction("INTERVAL '6' DAY * 2", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12 * 24 * 60 * 60 * 1000));
        assertFunction("2 * INTERVAL '6' DAY", INTERVAL_DAY_TIME, new SqlIntervalDayTime(12 * 24 * 60 * 60 * 1000));
        assertFunction("INTERVAL '1' DAY * 2.5", INTERVAL_DAY_TIME, new SqlIntervalDayTime((long) (2.5 * 24 * 60 * 60 * 1000)));
        assertFunction("2.5 * INTERVAL '1' DAY", INTERVAL_DAY_TIME, new SqlIntervalDayTime((long) (2.5 * 24 * 60 * 60 * 1000)));
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND / 2", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1500));
        assertFunction("INTERVAL '6' SECOND / 2.5", INTERVAL_DAY_TIME, new SqlIntervalDayTime(2400));

        assertFunction("INTERVAL '3' DAY / 2", INTERVAL_DAY_TIME, new SqlIntervalDayTime((long) (1.5 * 24 * 60 * 60 * 1000)));
        assertFunction("INTERVAL '4' DAY / 2.5", INTERVAL_DAY_TIME, new SqlIntervalDayTime((long) (1.6 * 24 * 60 * 60 * 1000)));
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("- INTERVAL '3' SECOND", INTERVAL_DAY_TIME, new SqlIntervalDayTime(-3 * 1000));
        assertFunction("- INTERVAL '6' DAY", INTERVAL_DAY_TIME, new SqlIntervalDayTime(-6 * 24 * 60 * 60 * 1000));
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND = INTERVAL '3' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '6' DAY = INTERVAL '6' DAY", BOOLEAN, true);

        assertFunction("INTERVAL '3' SECOND = INTERVAL '4' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '7' DAY = INTERVAL '6' DAY", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND <> INTERVAL '4' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '6' DAY <> INTERVAL '7' DAY", BOOLEAN, true);

        assertFunction("INTERVAL '3' SECOND <> INTERVAL '3' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '6' DAY <> INTERVAL '6' DAY", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND < INTERVAL '4' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '6' DAY < INTERVAL '7' DAY", BOOLEAN, true);

        assertFunction("INTERVAL '3' SECOND < INTERVAL '3' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '3' SECOND < INTERVAL '2' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '6' DAY < INTERVAL '6' DAY", BOOLEAN, false);
        assertFunction("INTERVAL '6' DAY < INTERVAL '5' DAY", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND <= INTERVAL '4' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '3' SECOND <= INTERVAL '3' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '6' DAY <= INTERVAL '6' DAY", BOOLEAN, true);
        assertFunction("INTERVAL '6' DAY <= INTERVAL '7' DAY", BOOLEAN, true);

        assertFunction("INTERVAL '3' SECOND <= INTERVAL '2' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '6' DAY <= INTERVAL '5' DAY", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND > INTERVAL '2' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '6' DAY > INTERVAL '5' DAY", BOOLEAN, true);

        assertFunction("INTERVAL '3' SECOND > INTERVAL '3' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '3' SECOND > INTERVAL '4' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '6' DAY > INTERVAL '6' DAY", BOOLEAN, false);
        assertFunction("INTERVAL '6' DAY > INTERVAL '7' DAY", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND >= INTERVAL '2' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '3' SECOND >= INTERVAL '3' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '6' DAY >= INTERVAL '5' DAY", BOOLEAN, true);
        assertFunction("INTERVAL '6' DAY >= INTERVAL '6' DAY", BOOLEAN, true);

        assertFunction("INTERVAL '3' SECOND >= INTERVAL '4' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '6' DAY >= INTERVAL '7' DAY", BOOLEAN, false);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("INTERVAL '3' SECOND between INTERVAL '2' SECOND and INTERVAL '4' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '3' SECOND and INTERVAL '4' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '2' SECOND and INTERVAL '3' SECOND", BOOLEAN, true);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '3' SECOND and INTERVAL '3' SECOND", BOOLEAN, true);

        assertFunction("INTERVAL '3' SECOND between INTERVAL '4' SECOND and INTERVAL '5' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '1' SECOND and INTERVAL '2' SECOND", BOOLEAN, false);
        assertFunction("INTERVAL '3' SECOND between INTERVAL '4' SECOND and INTERVAL '2' SECOND", BOOLEAN, false);
    }

    @Test
    public void testCastToSlice()
            throws Exception
    {
        assertFunction("cast(INTERVAL '12 10:45:32.123' DAY TO SECOND as varchar)", VARCHAR, "12 10:45:32.123");
        assertFunction("cast(INTERVAL '12 10:45:32.123' DAY TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(12, 10, 45, 32, 123).toString());
        assertFunction("cast(INTERVAL '12 10:45:32.12' DAY TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(12, 10, 45, 32, 120).toString());
        assertFunction("cast(INTERVAL '12 10:45:32' DAY TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(12, 10, 45, 32, 0).toString());
        assertFunction("cast(INTERVAL '12 10:45' DAY TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(12, 10, 45, 0, 0).toString());
        assertFunction("cast(INTERVAL '12 10' DAY TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(12, 10, 0, 0, 0).toString());
        assertFunction("cast(INTERVAL '12' DAY TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(12, 0, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '12 10:45' DAY TO MINUTE as varchar)", VARCHAR, new SqlIntervalDayTime(12, 10, 45, 0, 0).toString());
        assertFunction("cast(INTERVAL '12 10' DAY TO MINUTE as varchar)", VARCHAR, new SqlIntervalDayTime(12, 10, 0, 0, 0).toString());
        assertFunction("cast(INTERVAL '12' DAY TO MINUTE as varchar)", VARCHAR, new SqlIntervalDayTime(12, 0, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '12 10' DAY TO HOUR as varchar)", VARCHAR, new SqlIntervalDayTime(12, 10, 0, 0, 0).toString());
        assertFunction("cast(INTERVAL '12' DAY TO HOUR as varchar)", VARCHAR, new SqlIntervalDayTime(12, 0, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '12' DAY as varchar)", VARCHAR, new SqlIntervalDayTime(12, 0, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '10:45:32.123' HOUR TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 10, 45, 32, 123).toString());
        assertFunction("cast(INTERVAL '10:45:32.12' HOUR TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 10, 45, 32, 120).toString());
        assertFunction("cast(INTERVAL '10:45:32' HOUR TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 10, 45, 32, 0).toString());
        assertFunction("cast(INTERVAL '10:45' HOUR TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 10, 45, 0, 0).toString());
        assertFunction("cast(INTERVAL '10' HOUR TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 10, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '10:45' HOUR TO MINUTE as varchar)", VARCHAR, new SqlIntervalDayTime(0, 10, 45, 0, 0).toString());
        assertFunction("cast(INTERVAL '10' HOUR TO MINUTE as varchar)", VARCHAR, new SqlIntervalDayTime(0, 10, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '10' HOUR as varchar)", VARCHAR, new SqlIntervalDayTime(0, 10, 0, 0, 0).toString());

        assertFunction("cast(INTERVAL '45:32.123' MINUTE TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 0, 45, 32, 123).toString());
        assertFunction("cast(INTERVAL '45:32.12' MINUTE TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 0, 45, 32, 120).toString());
        assertFunction("cast(INTERVAL '45:32' MINUTE TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 0, 45, 32, 0).toString());
        assertFunction("cast(INTERVAL '45' MINUTE TO SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 0, 45, 0, 0).toString());

        assertFunction("cast(INTERVAL '45' MINUTE as varchar)", VARCHAR, new SqlIntervalDayTime(0, 0, 45, 0, 0).toString());

        assertFunction("cast(INTERVAL '32.123' SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 0, 0, 32, 123).toString());
        assertFunction("cast(INTERVAL '32.12' SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 0, 0, 32, 120).toString());
        assertFunction("cast(INTERVAL '32' SECOND as varchar)", VARCHAR, new SqlIntervalDayTime(0, 0, 0, 32, 0).toString());
    }
}
