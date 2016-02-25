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
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestBigintOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("37", BIGINT, 37L);
        assertFunction("17", BIGINT, 17L);
    }

    @Test
    public void testUnaryPlus()
            throws Exception
    {
        assertFunction("+37", BIGINT, 37L);
        assertFunction("+17", BIGINT, 17L);
    }

    @Test
    public void testUnaryMinus()
            throws Exception
    {
        assertFunction("-37", BIGINT, -37L);
        assertFunction("-17", BIGINT, -17L);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("37 + 37", BIGINT, 37 + 37L);
        assertFunction("37 + 17", BIGINT, 37 + 17L);
        assertFunction("17 + 37", BIGINT, 17 + 37L);
        assertFunction("17 + 17", BIGINT, 17 + 17L);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("37 - 37", BIGINT, 37 - 37L);
        assertFunction("37 - 17", BIGINT, 37 - 17L);
        assertFunction("17 - 37", BIGINT, 17 - 37L);
        assertFunction("17 - 17", BIGINT, 17 - 17L);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("37 * 37", BIGINT, 37 * 37L);
        assertFunction("37 * 17", BIGINT, 37 * 17L);
        assertFunction("17 * 37", BIGINT, 17 * 37L);
        assertFunction("17 * 17", BIGINT, 17 * 17L);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("37 / 37", BIGINT, 37 / 37L);
        assertFunction("37 / 17", BIGINT, 37 / 17L);
        assertFunction("17 / 37", BIGINT, 17 / 37L);
        assertFunction("17 / 17", BIGINT, 17 / 17L);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("37 % 37", BIGINT, 37 % 37L);
        assertFunction("37 % 17", BIGINT, 37 % 17L);
        assertFunction("17 % 37", BIGINT, 17 % 37L);
        assertFunction("17 % 17", BIGINT, 17 % 17L);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-(37)", BIGINT, -37L);
        assertFunction("-(17)", BIGINT, -17L);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("37 = 37", BOOLEAN, true);
        assertFunction("37 = 17", BOOLEAN, false);
        assertFunction("17 = 37", BOOLEAN, false);
        assertFunction("17 = 17", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("37 <> 37", BOOLEAN, false);
        assertFunction("37 <> 17", BOOLEAN, true);
        assertFunction("17 <> 37", BOOLEAN, true);
        assertFunction("17 <> 17", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("37 < 37", BOOLEAN, false);
        assertFunction("37 < 17", BOOLEAN, false);
        assertFunction("17 < 37", BOOLEAN, true);
        assertFunction("17 < 17", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("37 <= 37", BOOLEAN, true);
        assertFunction("37 <= 17", BOOLEAN, false);
        assertFunction("17 <= 37", BOOLEAN, true);
        assertFunction("17 <= 17", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("37 > 37", BOOLEAN, false);
        assertFunction("37 > 17", BOOLEAN, true);
        assertFunction("17 > 37", BOOLEAN, false);
        assertFunction("17 > 17", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("37 >= 37", BOOLEAN, true);
        assertFunction("37 >= 17", BOOLEAN, true);
        assertFunction("17 >= 37", BOOLEAN, false);
        assertFunction("17 >= 17", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("37 BETWEEN 37 AND 37", BOOLEAN, true);
        assertFunction("37 BETWEEN 37 AND 17", BOOLEAN, false);

        assertFunction("37 BETWEEN 17 AND 37", BOOLEAN, true);
        assertFunction("37 BETWEEN 17 AND 17", BOOLEAN, false);

        assertFunction("17 BETWEEN 37 AND 37", BOOLEAN, false);
        assertFunction("17 BETWEEN 37 AND 17", BOOLEAN, false);

        assertFunction("17 BETWEEN 17 AND 37", BOOLEAN, true);
        assertFunction("17 BETWEEN 17 AND 17", BOOLEAN, true);
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        assertFunction("cast(37 as bigint)", BIGINT, 37L);
        assertFunction("cast(17 as bigint)", BIGINT, 17L);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(37 as varchar)", VARCHAR, "37");
        assertFunction("cast(17 as varchar)", VARCHAR, "17");
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        assertFunction("cast(37 as double)", DOUBLE, 37.0);
        assertFunction("cast(17 as double)", DOUBLE, 17.0);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("cast(37 as boolean)", BOOLEAN, true);
        assertFunction("cast(17 as boolean)", BOOLEAN, true);
        assertFunction("cast(0 as boolean)", BOOLEAN, false);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast('37' as bigint)", BIGINT, 37L);
        assertFunction("cast('17' as bigint)", BIGINT, 17L);
    }
}
