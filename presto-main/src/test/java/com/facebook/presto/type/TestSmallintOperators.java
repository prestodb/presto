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
import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class TestSmallintOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
    {
        assertFunction("SMALLINT'37'", SMALLINT, (short) 37);
        assertFunction("SMALLINT'17'", SMALLINT, (short) 17);
        assertInvalidCast("SMALLINT'" + ((long) Short.MAX_VALUE + 1L) + "'");
    }

    @Test
    public void testUnaryPlus()
    {
        assertFunction("+SMALLINT'37'", SMALLINT, (short) 37);
        assertFunction("+SMALLINT'17'", SMALLINT, (short) 17);
    }

    @Test
    public void testUnaryMinus()
    {
        assertFunction("SMALLINT'-37'", SMALLINT, (short) -37);
        assertFunction("SMALLINT'-17'", SMALLINT, (short) -17);
        assertInvalidFunction("SMALLINT'-" + Short.MIN_VALUE + "'", INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testAdd()
    {
        assertFunction("SMALLINT'37' + SMALLINT'37'", SMALLINT, (short) (37 + 37));
        assertFunction("SMALLINT'37' + SMALLINT'17'", SMALLINT, (short) (37 + 17));
        assertFunction("SMALLINT'17' + SMALLINT'37'", SMALLINT, (short) (17 + 37));
        assertFunction("SMALLINT'17' + SMALLINT'17'", SMALLINT, (short) (17 + 17));
        assertNumericOverflow(format("SMALLINT'%s' + SMALLINT'1'", Short.MAX_VALUE), "smallint addition overflow: 32767 + 1");
    }

    @Test
    public void testSubtract()
    {
        assertFunction("SMALLINT'37' - SMALLINT'37'", SMALLINT, (short) 0);
        assertFunction("SMALLINT'37' - SMALLINT'17'", SMALLINT, (short) (37 - 17));
        assertFunction("SMALLINT'17' - SMALLINT'37'", SMALLINT, (short) (17 - 37));
        assertFunction("SMALLINT'17' - SMALLINT'17'", SMALLINT, (short) 0);
        assertNumericOverflow(format("SMALLINT'%s' - SMALLINT'1'", Short.MIN_VALUE), "smallint subtraction overflow: -32768 - 1");
    }

    @Test
    public void testMultiply()
    {
        assertFunction("SMALLINT'37' * SMALLINT'37'", SMALLINT, (short) (37 * 37));
        assertFunction("SMALLINT'37' * SMALLINT'17'", SMALLINT, (short) (37 * 17));
        assertFunction("SMALLINT'17' * SMALLINT'37'", SMALLINT, (short) (17 * 37));
        assertFunction("SMALLINT'17' * SMALLINT'17'", SMALLINT, (short) (17 * 17));
        assertNumericOverflow(format("SMALLINT'%s' * SMALLINT'2'", Short.MAX_VALUE), "smallint multiplication overflow: 32767 * 2");
    }

    @Test
    public void testDivide()
    {
        assertFunction("SMALLINT'37' / SMALLINT'37'", SMALLINT, (short) 1);
        assertFunction("SMALLINT'37' / SMALLINT'17'", SMALLINT, (short) (37 / 17));
        assertFunction("SMALLINT'17' / SMALLINT'37'", SMALLINT, (short) (17 / 37));
        assertFunction("SMALLINT'17' / SMALLINT'17'", SMALLINT, (short) 1);
        assertInvalidFunction("SMALLINT'17' / SMALLINT'0'", DIVISION_BY_ZERO);
    }

    @Test
    public void testModulus()
    {
        assertFunction("SMALLINT'37' % SMALLINT'37'", SMALLINT, (short) 0);
        assertFunction("SMALLINT'37' % SMALLINT'17'", SMALLINT, (short) (37 % 17));
        assertFunction("SMALLINT'17' % SMALLINT'37'", SMALLINT, (short) (17 % 37));
        assertFunction("SMALLINT'17' % SMALLINT'17'", SMALLINT, (short) 0);
        assertInvalidFunction("SMALLINT'17' % SMALLINT'0'", DIVISION_BY_ZERO);
    }

    @Test
    public void testNegation()
    {
        assertFunction("-(SMALLINT'37')", SMALLINT, (short) -37);
        assertFunction("-(SMALLINT'17')", SMALLINT, (short) -17);
        assertFunction("-(SMALLINT'" + Short.MAX_VALUE + "')", SMALLINT, (short) (Short.MIN_VALUE + 1));
        assertNumericOverflow(format("-(SMALLINT'%s')", Short.MIN_VALUE), "smallint negation overflow: -32768");
    }

    @Test
    public void testEqual()
    {
        assertFunction("SMALLINT'37' = SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'37' = SMALLINT'17'", BOOLEAN, false);
        assertFunction("SMALLINT'17' = SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'17' = SMALLINT'17'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("SMALLINT'37' <> SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'37' <> SMALLINT'17'", BOOLEAN, true);
        assertFunction("SMALLINT'17' <> SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'17' <> SMALLINT'17'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("SMALLINT'37' < SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'37' < SMALLINT'17'", BOOLEAN, false);
        assertFunction("SMALLINT'17' < SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'17' < SMALLINT'17'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("SMALLINT'37' <= SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'37' <= SMALLINT'17'", BOOLEAN, false);
        assertFunction("SMALLINT'17' <= SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'17' <= SMALLINT'17'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("SMALLINT'37' > SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'37' > SMALLINT'17'", BOOLEAN, true);
        assertFunction("SMALLINT'17' > SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'17' > SMALLINT'17'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("SMALLINT'37' >= SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'37' >= SMALLINT'17'", BOOLEAN, true);
        assertFunction("SMALLINT'17' >= SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'17' >= SMALLINT'17'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
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
    {
        assertFunction("cast(SMALLINT'37' as bigint)", BIGINT, 37L);
        assertFunction("cast(SMALLINT'17' as bigint)", BIGINT, 17L);
    }

    @Test
    public void testCastToInteger()
    {
        assertFunction("cast(SMALLINT'37' as integer)", INTEGER, 37);
        assertFunction("cast(SMALLINT'17' as integer)", INTEGER, 17);
    }

    @Test
    public void testCastToTinyint()
    {
        assertFunction("cast(SMALLINT'37' as tinyint)", TINYINT, (byte) 37);
        assertFunction("cast(SMALLINT'17' as tinyint)", TINYINT, (byte) 17);
    }

    @Test
    public void testCastToVarchar()
    {
        assertFunction("cast(SMALLINT'37' as varchar)", VARCHAR, "37");
        assertFunction("cast(SMALLINT'17' as varchar)", VARCHAR, "17");
    }

    @Test
    public void testCastToDouble()
    {
        assertFunction("cast(SMALLINT'37' as double)", DOUBLE, 37.0);
        assertFunction("cast(SMALLINT'17' as double)", DOUBLE, 17.0);
    }

    @Test
    public void testCastToFloat()
    {
        assertFunction("cast(SMALLINT'37' as real)", REAL, 37.0f);
        assertFunction("cast(SMALLINT'-32768' as real)", REAL, -32768.0f);
        assertFunction("cast(SMALLINT'0' as real)", REAL, 0.0f);
    }

    @Test
    public void testCastToBoolean()
    {
        assertFunction("cast(SMALLINT'37' as boolean)", BOOLEAN, true);
        assertFunction("cast(SMALLINT'17' as boolean)", BOOLEAN, true);
        assertFunction("cast(SMALLINT'0' as boolean)", BOOLEAN, false);
    }

    @Test
    public void testCastFromVarchar()
    {
        assertFunction("cast('37' as smallint)", SMALLINT, (short) 37);
        assertFunction("cast('17' as smallint)", SMALLINT, (short) 17);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("CAST(NULL AS SMALLINT) IS DISTINCT FROM CAST(NULL AS SMALLINT)", BOOLEAN, false);
        assertFunction("SMALLINT'37' IS DISTINCT FROM SMALLINT'37'", BOOLEAN, false);
        assertFunction("SMALLINT'37' IS DISTINCT FROM SMALLINT'38'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM SMALLINT'37'", BOOLEAN, true);
        assertFunction("SMALLINT'37' IS DISTINCT FROM NULL", BOOLEAN, true);
    }

    @Test
    public void testIndeterminate()
            throws Exception
    {
        assertOperator(INDETERMINATE, "cast(null as smallint)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "cast(12 as smallint)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(0 as smallint)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(-23 as smallint)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(1.4 as smallint)", BOOLEAN, false);
    }
}
