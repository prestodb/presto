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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestDoubleOperators
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
        assertFunction("37.7", 37.7);
        assertFunction("17.1", 17.1);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("37.7 + 37.7", 37.7 + 37.7);
        assertFunction("37.7 + 17.1", 37.7 + 17.1);
        assertFunction("17.1 + 37.7", 17.1 + 37.7);
        assertFunction("17.1 + 17.1", 17.1 + 17.1);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("37.7 - 37.7", 37.7 - 37.7);
        assertFunction("37.7 - 17.1", 37.7 - 17.1);
        assertFunction("17.1 - 37.7", 17.1 - 37.7);
        assertFunction("17.1 - 17.1", 17.1 - 17.1);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("37.7 * 37.7", 37.7 * 37.7);
        assertFunction("37.7 * 17.1", 37.7 * 17.1);
        assertFunction("17.1 * 37.7", 17.1 * 37.7);
        assertFunction("17.1 * 17.1", 17.1 * 17.1);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("37.7 / 37.7", 37.7 / 37.7);
        assertFunction("37.7 / 17.1", 37.7 / 17.1);
        assertFunction("17.1 / 37.7", 17.1 / 37.7);
        assertFunction("17.1 / 17.1", 17.1 / 17.1);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("37.7 % 37.7", 37.7 % 37.7);
        assertFunction("37.7 % 17.1", 37.7 % 17.1);
        assertFunction("17.1 % 37.7", 17.1 % 37.7);
        assertFunction("17.1 % 17.1", 17.1 % 17.1);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-(37.7)", -37.7);
        assertFunction("-(17.1)", -17.1);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("37.7 = 37.7", true);
        assertFunction("37.7 = 17.1", false);
        assertFunction("17.1 = 37.7", false);
        assertFunction("17.1 = 17.1", true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("37.7 <> 37.7", false);
        assertFunction("37.7 <> 17.1", true);
        assertFunction("17.1 <> 37.7", true);
        assertFunction("17.1 <> 17.1", false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("37.7 < 37.7", false);
        assertFunction("37.7 < 17.1", false);
        assertFunction("17.1 < 37.7", true);
        assertFunction("17.1 < 17.1", false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("37.7 <= 37.7", true);
        assertFunction("37.7 <= 17.1", false);
        assertFunction("17.1 <= 37.7", true);
        assertFunction("17.1 <= 17.1", true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("37.7 > 37.7", false);
        assertFunction("37.7 > 17.1", true);
        assertFunction("17.1 > 37.7", false);
        assertFunction("17.1 > 17.1", false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("37.7 >= 37.7", true);
        assertFunction("37.7 >= 17.1", true);
        assertFunction("17.1 >= 37.7", false);
        assertFunction("17.1 >= 17.1", true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("37.7 BETWEEN 37.7 AND 37.7", true);
        assertFunction("37.7 BETWEEN 37.7 AND 17.1", false);

        assertFunction("37.7 BETWEEN 17.1 AND 37.7", true);
        assertFunction("37.7 BETWEEN 17.1 AND 17.1", false);

        assertFunction("17.1 BETWEEN 37.7 AND 37.7", false);
        assertFunction("17.1 BETWEEN 37.7 AND 17.1", false);

        assertFunction("17.1 BETWEEN 17.1 AND 37.7", true);
        assertFunction("17.1 BETWEEN 17.1 AND 17.1", true);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(37.7 as varchar)", "37.7");
        assertFunction("cast(17.1 as varchar)", "17.1");
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        assertFunction("cast(37.7 as bigint)", 38L);
        assertFunction("cast(17.1 as bigint)", 17L);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("cast(37.7 as boolean)", true);
        assertFunction("cast(17.1 as boolean)", true);
        assertFunction("cast(0.0 as boolean)", false);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast('37.7' as double)", 37.7);
        assertFunction("cast('17.1' as double)", 17.1);
    }
}
