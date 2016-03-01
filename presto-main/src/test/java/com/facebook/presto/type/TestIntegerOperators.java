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

import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestIntegerOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("INTEGER'37'", INTEGER, 37);
        assertFunction("INTEGER'17'", INTEGER, 17);
        assertInvalidCast("INTEGER'" + ((long) Integer.MAX_VALUE + 1L) + "'");
    }

    @Test
    public void testUnaryPlus()
            throws Exception
    {
        assertFunction("+INTEGER'37'", INTEGER, 37);
        assertFunction("+INTEGER'17'", INTEGER, 17);
    }

    @Test
    public void testUnaryMinus()
            throws Exception
    {
        assertFunction("INTEGER'-37'", INTEGER, -37);
        assertFunction("INTEGER'-17'", INTEGER, -17);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("INTEGER'37' + INTEGER'37'", INTEGER, 37 + 37);
        assertFunction("INTEGER'37' + INTEGER'17'", INTEGER, 37 + 17);
        assertFunction("INTEGER'17' + INTEGER'37'", INTEGER, 17 + 37);
        assertFunction("INTEGER'17' + INTEGER'17'", INTEGER, 17 + 17);
        assertInvalidFunction("INTEGER'" + Integer.MAX_VALUE + "' + INTEGER'1'", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("INTEGER'37' - INTEGER'37'", INTEGER, 0);
        assertFunction("INTEGER'37' - INTEGER'17'", INTEGER, 37 - 17);
        assertFunction("INTEGER'17' - INTEGER'37'", INTEGER, 17 - 37);
        assertFunction("INTEGER'17' - INTEGER'17'", INTEGER, 0);
        assertInvalidFunction("INTEGER'" + Integer.MIN_VALUE + "' - INTEGER'1'", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("INTEGER'37' * INTEGER'37'", INTEGER, 37 * 37);
        assertFunction("INTEGER'37' * INTEGER'17'", INTEGER, 37 * 17);
        assertFunction("INTEGER'17' * INTEGER'37'", INTEGER, 17 * 37);
        assertFunction("INTEGER'17' * INTEGER'17'", INTEGER, 17 * 17);
        assertInvalidFunction("INTEGER'" + Integer.MAX_VALUE + "' * INTEGER'2'", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("INTEGER'37' / INTEGER'37'", INTEGER, 1);
        assertFunction("INTEGER'37' / INTEGER'17'", INTEGER, 37 / 17);
        assertFunction("INTEGER'17' / INTEGER'37'", INTEGER, 17 / 37);
        assertFunction("INTEGER'17' / INTEGER'17'", INTEGER, 1);
        assertInvalidFunction("INTEGER'17' / INTEGER'0'", DIVISION_BY_ZERO);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("INTEGER'37' % INTEGER'37'", INTEGER, 0);
        assertFunction("INTEGER'37' % INTEGER'17'", INTEGER, 37 % 17);
        assertFunction("INTEGER'17' % INTEGER'37'", INTEGER, 17 % 37);
        assertFunction("INTEGER'17' % INTEGER'17'", INTEGER, 0);
        assertInvalidFunction("INTEGER'17' % INTEGER'0'", DIVISION_BY_ZERO);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-(INTEGER'37')", INTEGER, -37);
        assertFunction("-(INTEGER'17')", INTEGER, -17);
        assertFunction("-(INTEGER'" + Integer.MAX_VALUE + "')", INTEGER, Integer.MIN_VALUE + 1);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("INTEGER'37' = INTEGER'37'", BOOLEAN, true);
        assertFunction("INTEGER'37' = INTEGER'17'", BOOLEAN, false);
        assertFunction("INTEGER'17' = INTEGER'37'", BOOLEAN, false);
        assertFunction("INTEGER'17' = INTEGER'17'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("INTEGER'37' <> INTEGER'37'", BOOLEAN, false);
        assertFunction("INTEGER'37' <> INTEGER'17'", BOOLEAN, true);
        assertFunction("INTEGER'17' <> INTEGER'37'", BOOLEAN, true);
        assertFunction("INTEGER'17' <> INTEGER'17'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("INTEGER'37' < INTEGER'37'", BOOLEAN, false);
        assertFunction("INTEGER'37' < INTEGER'17'", BOOLEAN, false);
        assertFunction("INTEGER'17' < INTEGER'37'", BOOLEAN, true);
        assertFunction("INTEGER'17' < INTEGER'17'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("INTEGER'37' <= INTEGER'37'", BOOLEAN, true);
        assertFunction("INTEGER'37' <= INTEGER'17'", BOOLEAN, false);
        assertFunction("INTEGER'17' <= INTEGER'37'", BOOLEAN, true);
        assertFunction("INTEGER'17' <= INTEGER'17'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("INTEGER'37' > INTEGER'37'", BOOLEAN, false);
        assertFunction("INTEGER'37' > INTEGER'17'", BOOLEAN, true);
        assertFunction("INTEGER'17' > INTEGER'37'", BOOLEAN, false);
        assertFunction("INTEGER'17' > INTEGER'17'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("INTEGER'37' >= INTEGER'37'", BOOLEAN, true);
        assertFunction("INTEGER'37' >= INTEGER'17'", BOOLEAN, true);
        assertFunction("INTEGER'17' >= INTEGER'37'", BOOLEAN, false);
        assertFunction("INTEGER'17' >= INTEGER'17'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("INTEGER'37' BETWEEN INTEGER'37' AND INTEGER'37'", BOOLEAN, true);
        assertFunction("INTEGER'37' BETWEEN INTEGER'37' AND INTEGER'17'", BOOLEAN, false);

        assertFunction("INTEGER'37' BETWEEN INTEGER'17' AND INTEGER'37'", BOOLEAN, true);
        assertFunction("INTEGER'37' BETWEEN INTEGER'17' AND INTEGER'17'", BOOLEAN, false);

        assertFunction("INTEGER'17' BETWEEN INTEGER'37' AND INTEGER'37'", BOOLEAN, false);
        assertFunction("INTEGER'17' BETWEEN INTEGER'37' AND INTEGER'17'", BOOLEAN, false);

        assertFunction("INTEGER'17' BETWEEN INTEGER'17' AND INTEGER'37'", BOOLEAN, true);
        assertFunction("INTEGER'17' BETWEEN INTEGER'17' AND INTEGER'17'", BOOLEAN, true);
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        assertFunction("cast(INTEGER'37' as bigint)", BIGINT, 37L);
        assertFunction("cast(INTEGER'17' as bigint)", BIGINT, 17L);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(INTEGER'37' as varchar)", VARCHAR, "37");
        assertFunction("cast(INTEGER'17' as varchar)", VARCHAR, "17");
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        assertFunction("cast(INTEGER'37' as double)", DOUBLE, 37.0);
        assertFunction("cast(INTEGER'17' as double)", DOUBLE, 17.0);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("cast(INTEGER'37' as boolean)", BOOLEAN, true);
        assertFunction("cast(INTEGER'17' as boolean)", BOOLEAN, true);
        assertFunction("cast(INTEGER'0' as boolean)", BOOLEAN, false);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast('37' as integer)", INTEGER, 37);
        assertFunction("cast('17' as integer)", INTEGER, 17);
    }
}
