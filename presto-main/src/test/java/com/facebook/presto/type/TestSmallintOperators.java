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
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class TestSmallintOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("SMALLINT'37'", SMALLINT, (short) 37);
        assertFunction("SMALLINT'17'", SMALLINT, (short) 17);
        assertInvalidCast("SMALLINT'" + ((long) Short.MAX_VALUE + 1L) + "'");
    }

    @Test
    public void testUnaryPlus()
            throws Exception
    {
        assertFunction("+SMALLINT'37'", SMALLINT, (short) 37);
        assertFunction("+SMALLINT'17'", SMALLINT, (short) 17);
    }

    @Test
    public void testUnaryMinus()
            throws Exception
    {
        assertFunction("SMALLINT'-37'", SMALLINT, (short) -37);
        assertFunction("SMALLINT'-17'", SMALLINT, (short) -17);
        assertInvalidFunction("SMALLINT'-" + Short.MIN_VALUE + "'", INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("SMALLINT'37' + SMALLINT'37'", SMALLINT, (short) (37 + 37));
        assertFunction("SMALLINT'37' + SMALLINT'17'", SMALLINT, (short) (37 + 17));
        assertFunction("SMALLINT'17' + SMALLINT'37'", SMALLINT, (short) (17 + 37));
        assertFunction("SMALLINT'17' + SMALLINT'17'", SMALLINT, (short) (17 + 17));
        assertNumericOverflow(format("SMALLINT'%s' + SMALLINT'1'", Short.MAX_VALUE), "smallint addition overflow: 32767 + 1");
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("SMALLINT'37' - SMALLINT'37'", SMALLINT, (short) 0);
        assertFunction("SMALLINT'37' - SMALLINT'17'", SMALLINT, (short) (37 - 17));
        assertFunction("SMALLINT'17' - SMALLINT'37'", SMALLINT, (short) (17 - 37));
        assertFunction("SMALLINT'17' - SMALLINT'17'", SMALLINT, (short) 0);
        assertNumericOverflow(format("SMALLINT'%s' - SMALLINT'1'", Short.MIN_VALUE), "smallint subtraction overflow: -32768 - 1");
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("SMALLINT'37' * SMALLINT'37'", SMALLINT, (short) (37 * 37));
        assertFunction("SMALLINT'37' * SMALLINT'17'", SMALLINT, (short) (37 * 17));
        assertFunction("SMALLINT'17' * SMALLINT'37'", SMALLINT, (short) (17 * 37));
        assertFunction("SMALLINT'17' * SMALLINT'17'", SMALLINT, (short) (17 * 17));
        assertNumericOverflow(format("SMALLINT'%s' * SMALLINT'2'", Short.MAX_VALUE), "smallint multiplication overflow: 32767 * 2");
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("SMALLINT'37' / SMALLINT'37'", SMALLINT, (short) 1);
        assertFunction("SMALLINT'37' / SMALLINT'17'", SMALLINT, (short) (37 / 17));
        assertFunction("SMALLINT'17' / SMALLINT'37'", SMALLINT, (short) (17 / 37));
        assertFunction("SMALLINT'17' / SMALLINT'17'", SMALLINT, (short) 1);
        assertInvalidFunction("SMALLINT'17' / SMALLINT'0'", DIVISION_BY_ZERO);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("SMALLINT'37' % SMALLINT'37'", SMALLINT, (short) 0);
        assertFunction("SMALLINT'37' % SMALLINT'17'", SMALLINT, (short) (37 % 17));
        assertFunction("SMALLINT'17' % SMALLINT'37'", SMALLINT, (short) (17 % 37));
        assertFunction("SMALLINT'17' % SMALLINT'17'", SMALLINT, (short) 0);
        assertInvalidFunction("SMALLINT'17' % SMALLINT'0'", DIVISION_BY_ZERO);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-(SMALLINT'37')", SMALLINT, (short) -37);
        assertFunction("-(SMALLINT'17')", SMALLINT, (short) -17);
        assertFunction("-(SMALLINT'" + Short.MAX_VALUE + "')", SMALLINT, (short) (Short.MIN_VALUE + 1));
        assertNumericOverflow(format("-(SMALLINT'%s')", Short.MIN_VALUE), "smallint negation overflow: -32768");
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("SMALLINT'37' = SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'37' = SMALLINT'17'", BOOLEAN, false);
        assertFunction("SMALLINT'17' = SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'17' = SMALLINT'17'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("SMALLINT'37' <> SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'37' <> SMALLINT'17'", BOOLEAN, true);
        assertFunction("SMALLINT'17' <> SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'17' <> SMALLINT'17'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("SMALLINT'37' < SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'37' < SMALLINT'17'", BOOLEAN, false);
        assertFunction("SMALLINT'17' < SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'17' < SMALLINT'17'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("SMALLINT'37' <= SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'37' <= SMALLINT'17'", BOOLEAN, false);
        assertFunction("SMALLINT'17' <= SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'17' <= SMALLINT'17'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("SMALLINT'37' > SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'37' > SMALLINT'17'", BOOLEAN, true);
        assertFunction("SMALLINT'17' > SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'17' > SMALLINT'17'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("SMALLINT'37' >= SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'37' >= SMALLINT'17'", BOOLEAN, true);
        assertFunction("SMALLINT'17' >= SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'17' >= SMALLINT'17'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("SMALLINT'37' BETWEEN SMALLINT'37' AND SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'37' BETWEEN SMALLINT'37' AND SMALLINT'17'", BOOLEAN, false);

        assertFunction("SMALLINT'37' BETWEEN SMALLINT'17' AND SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'37' BETWEEN SMALLINT'17' AND SMALLINT'17'", BOOLEAN, false);

        assertFunction("SMALLINT'17' BETWEEN SMALLINT'37' AND SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'17' BETWEEN SMALLINT'37' AND SMALLINT'17'", BOOLEAN, false);

        assertFunction("SMALLINT'17' BETWEEN SMALLINT'17' AND SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'17' BETWEEN SMALLINT'17' AND SMALLINT'17'", BOOLEAN, true);
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        assertFunction("cast(SMALLINT'37' as bigint)", BIGINT, 37L);
        assertFunction("cast(SMALLINT'17' as bigint)", BIGINT, 17L);
    }

    @Test
    public void testCastToInteger()
            throws Exception
    {
        assertFunction("cast(SMALLINT'37' as integer)", INTEGER, 37);
        assertFunction("cast(SMALLINT'17' as integer)", INTEGER, 17);
    }

    @Test
    public void testCastToTinyint()
            throws Exception
    {
        assertFunction("cast(SMALLINT'37' as tinyint)", TINYINT, (byte) 37);
        assertFunction("cast(SMALLINT'17' as tinyint)", TINYINT, (byte) 17);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(SMALLINT'37' as varchar)", VARCHAR, "37");
        assertFunction("cast(SMALLINT'17' as varchar)", VARCHAR, "17");
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        assertFunction("cast(SMALLINT'37' as double)", DOUBLE, 37.0);
        assertFunction("cast(SMALLINT'17' as double)", DOUBLE, 17.0);
    }

    @Test
    public void testCastToFloat()
            throws Exception
    {
        assertFunction("cast(SMALLINT'37' as float)", FLOAT, 37.0f);
        assertFunction("cast(SMALLINT'-32768' as float)", FLOAT, -32768.0f);
        assertFunction("cast(SMALLINT'0' as float)", FLOAT, 0.0f);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("cast(SMALLINT'37' as boolean)", BOOLEAN, true);
        assertFunction("cast(SMALLINT'17' as boolean)", BOOLEAN, true);
        assertFunction("cast(SMALLINT'0' as boolean)", BOOLEAN, false);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast('37' as smallint)", SMALLINT, (short) 37);
        assertFunction("cast('17' as smallint)", SMALLINT, (short) 17);
    }
}
