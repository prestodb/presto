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
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestDoubleOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("37.7", DOUBLE, 37.7);
        assertFunction("17.1", DOUBLE, 17.1);
    }

    @Test
    public void testTypeConstructor()
            throws Exception
    {
        assertFunction("DOUBLE '12.34'", DOUBLE, 12.34);
        assertFunction("DOUBLE '-17.6'", DOUBLE, -17.6);
        assertFunction("DOUBLE '+754'", DOUBLE, 754.0);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("37.7 + 37.7", DOUBLE, 37.7 + 37.7);
        assertFunction("37.7 + 17.1", DOUBLE, 37.7 + 17.1);
        assertFunction("17.1 + 37.7", DOUBLE, 17.1 + 37.7);
        assertFunction("17.1 + 17.1", DOUBLE, 17.1 + 17.1);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("37.7 - 37.7", DOUBLE, 37.7 - 37.7);
        assertFunction("37.7 - 17.1", DOUBLE, 37.7 - 17.1);
        assertFunction("17.1 - 37.7", DOUBLE, 17.1 - 37.7);
        assertFunction("17.1 - 17.1", DOUBLE, 17.1 - 17.1);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("37.7 * 37.7", DOUBLE, 37.7 * 37.7);
        assertFunction("37.7 * 17.1", DOUBLE, 37.7 * 17.1);
        assertFunction("17.1 * 37.7", DOUBLE, 17.1 * 37.7);
        assertFunction("17.1 * 17.1", DOUBLE, 17.1 * 17.1);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("37.7 / 37.7", DOUBLE, 37.7 / 37.7);
        assertFunction("37.7 / 17.1", DOUBLE, 37.7 / 17.1);
        assertFunction("17.1 / 37.7", DOUBLE, 17.1 / 37.7);
        assertFunction("17.1 / 17.1", DOUBLE, 17.1 / 17.1);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("37.7 % 37.7", DOUBLE, 37.7 % 37.7);
        assertFunction("37.7 % 17.1", DOUBLE, 37.7 % 17.1);
        assertFunction("17.1 % 37.7", DOUBLE, 17.1 % 37.7);
        assertFunction("17.1 % 17.1", DOUBLE, 17.1 % 17.1);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-(37.7)", DOUBLE, -37.7);
        assertFunction("-(17.1)", DOUBLE, -17.1);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("37.7 = 37.7", BOOLEAN, true);
        assertFunction("37.7 = 17.1", BOOLEAN, false);
        assertFunction("17.1 = 37.7", BOOLEAN, false);
        assertFunction("17.1 = 17.1", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("37.7 <> 37.7", BOOLEAN, false);
        assertFunction("37.7 <> 17.1", BOOLEAN, true);
        assertFunction("17.1 <> 37.7", BOOLEAN, true);
        assertFunction("17.1 <> 17.1", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("37.7 < 37.7", BOOLEAN, false);
        assertFunction("37.7 < 17.1", BOOLEAN, false);
        assertFunction("17.1 < 37.7", BOOLEAN, true);
        assertFunction("17.1 < 17.1", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("37.7 <= 37.7", BOOLEAN, true);
        assertFunction("37.7 <= 17.1", BOOLEAN, false);
        assertFunction("17.1 <= 37.7", BOOLEAN, true);
        assertFunction("17.1 <= 17.1", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("37.7 > 37.7", BOOLEAN, false);
        assertFunction("37.7 > 17.1", BOOLEAN, true);
        assertFunction("17.1 > 37.7", BOOLEAN, false);
        assertFunction("17.1 > 17.1", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("37.7 >= 37.7", BOOLEAN, true);
        assertFunction("37.7 >= 17.1", BOOLEAN, true);
        assertFunction("17.1 >= 37.7", BOOLEAN, false);
        assertFunction("17.1 >= 17.1", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("37.7 BETWEEN 37.7 AND 37.7", BOOLEAN, true);
        assertFunction("37.7 BETWEEN 37.7 AND 17.1", BOOLEAN, false);

        assertFunction("37.7 BETWEEN 17.1 AND 37.7", BOOLEAN, true);
        assertFunction("37.7 BETWEEN 17.1 AND 17.1", BOOLEAN, false);

        assertFunction("17.1 BETWEEN 37.7 AND 37.7", BOOLEAN, false);
        assertFunction("17.1 BETWEEN 37.7 AND 17.1", BOOLEAN, false);

        assertFunction("17.1 BETWEEN 17.1 AND 37.7", BOOLEAN, true);
        assertFunction("17.1 BETWEEN 17.1 AND 17.1", BOOLEAN, true);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(37.7 as varchar)", VARCHAR, "37.7");
        assertFunction("cast(17.1 as varchar)", VARCHAR, "17.1");
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        assertFunction("cast(37.7 as bigint)", BIGINT, 38L);
        assertFunction("cast(17.1 as bigint)", BIGINT, 17L);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("cast(37.7 as boolean)", BOOLEAN, true);
        assertFunction("cast(17.1 as boolean)", BOOLEAN, true);
        assertFunction("cast(0.0 as boolean)", BOOLEAN, false);
    }

    @Test
    public void testCastToFloat()
            throws Exception
    {
        assertFunction("cast('754.1985' as float)", FLOAT, 754.1985f);
        assertFunction("cast('-754.2008' as float)", FLOAT, -754.2008f);
        assertFunction("cast('0.0' as float)", FLOAT, 0.0f);
        assertFunction("cast('-0.0' as float)", FLOAT, -0.0f);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast('37.7' as double)", DOUBLE, 37.7);
        assertFunction("cast('17.1' as double)", DOUBLE, 17.1);
    }
}
