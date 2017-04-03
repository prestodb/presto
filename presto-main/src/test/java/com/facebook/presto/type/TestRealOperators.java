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
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

public class TestRealOperators
        extends AbstractTestFunctions
{
    @Test
    public void testTypeConstructor()
            throws Exception
    {
        assertFunction("REAL'12.2'", REAL, 12.2f);
        assertFunction("REAL'-17.76'", REAL, -17.76f);
        assertFunction("REAL'NaN'", REAL, Float.NaN);
        assertFunction("REAL'Infinity'", REAL, Float.POSITIVE_INFINITY);
        assertFunction("REAL'-Infinity'", REAL, Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("REAL'12.34' + REAL'56.78'", REAL, 12.34f + 56.78f);
        assertFunction("REAL'-17.34' + REAL'-22.891'", REAL, -17.34f + -22.891f);
        assertFunction("REAL'-89.123' + REAL'754.0'", REAL, -89.123f + 754.0f);
        assertFunction("REAL'-0.0' + REAL'0.0'", REAL, -0.0f + 0.0f);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("REAL'12.34' - REAL'56.78'", REAL, 12.34f - 56.78f);
        assertFunction("REAL'-17.34' - REAL'-22.891'", REAL, -17.34f - -22.891f);
        assertFunction("REAL'-89.123' - REAL'754.0'", REAL, -89.123f - 754.0f);
        assertFunction("REAL'-0.0' - REAL'0.0'", REAL, -0.0f - 0.0f);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("REAL'12.34' * REAL'56.78'", REAL, 12.34f * 56.78f);
        assertFunction("REAL'-17.34' * REAL'-22.891'", REAL, -17.34f * -22.891f);
        assertFunction("REAL'-89.123' * REAL'754.0'", REAL, -89.123f * 754.0f);
        assertFunction("REAL'-0.0' * REAL'0.0'", REAL, -0.0f * 0.0f);
        assertFunction("REAL'-17.71' * REAL'-1.0'", REAL, -17.71f * -1.0f);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("REAL'12.34' / REAL'56.78'", REAL, 12.34f / 56.78f);
        assertFunction("REAL'-17.34' / REAL'-22.891'", REAL, -17.34f / -22.891f);
        assertFunction("REAL'-89.123' / REAL'754.0'", REAL, -89.123f / 754.0f);
        assertFunction("REAL'-0.0' / REAL'0.0'", REAL, -0.0f / 0.0f);
        assertFunction("REAL'-17.71' / REAL'-1.0'", REAL, -17.71f / -1.0f);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("REAL'12.34' % REAL'56.78'", REAL, 12.34f % 56.78f);
        assertFunction("REAL'-17.34' % REAL'-22.891'", REAL, -17.34f % -22.891f);
        assertFunction("REAL'-89.123' % REAL'754.0'", REAL, -89.123f % 754.0f);
        assertFunction("REAL'-0.0' % REAL'0.0'", REAL, -0.0f % 0.0f);
        assertFunction("REAL'-17.71' % REAL'-1.0'", REAL, -17.71f % -1.0f);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-REAL'12.34'", REAL, -12.34f);
        assertFunction("-REAL'-17.34'", REAL, 17.34f);
        assertFunction("-REAL'-0.0'", REAL, -(-0.0f));
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("REAL'12.34' = REAL'12.34'", BOOLEAN, true);
        assertFunction("REAL'12.340' = REAL'12.34'", BOOLEAN, true);
        assertFunction("REAL'-17.34' = REAL'-17.34'", BOOLEAN, true);
        assertFunction("REAL'71.17' = REAL'23.45'", BOOLEAN, false);
        assertFunction("REAL'-0.0' = REAL'0.0'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("REAL'12.34' <> REAL'12.34'", BOOLEAN, false);
        assertFunction("REAL'12.34' <> REAL'12.340'", BOOLEAN, false);
        assertFunction("REAL'-17.34' <> REAL'-17.34'", BOOLEAN, false);
        assertFunction("REAL'71.17' <> REAL'23.45'", BOOLEAN, true);
        assertFunction("REAL'-0.0' <> REAL'0.0'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("REAL'12.34' < REAL'754.123'", BOOLEAN, true);
        assertFunction("REAL'-17.34' < REAL'-16.34'", BOOLEAN, true);
        assertFunction("REAL'71.17' < REAL'23.45'", BOOLEAN, false);
        assertFunction("REAL'-0.0' < REAL'0.0'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("REAL'12.34' <= REAL'754.123'", BOOLEAN, true);
        assertFunction("REAL'-17.34' <= REAL'-17.34'", BOOLEAN, true);
        assertFunction("REAL'71.17' <= REAL'23.45'", BOOLEAN, false);
        assertFunction("REAL'-0.0' <= REAL'0.0'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("REAL'12.34' > REAL'754.123'", BOOLEAN, false);
        assertFunction("REAL'-17.34' > REAL'-17.34'", BOOLEAN, false);
        assertFunction("REAL'71.17' > REAL'23.45'", BOOLEAN, true);
        assertFunction("REAL'-0.0' > REAL'0.0'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("REAL'12.34' >= REAL'754.123'", BOOLEAN, false);
        assertFunction("REAL'-17.34' >= REAL'-17.34'", BOOLEAN, true);
        assertFunction("REAL'71.17' >= REAL'23.45'", BOOLEAN, true);
        assertFunction("REAL'-0.0' >= REAL'0.0'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("REAL'12.34' BETWEEN REAL'9.12' AND REAL'25.89'", BOOLEAN, true);
        assertFunction("REAL'-17.34' BETWEEN REAL'-17.34' AND REAL'-16.57'", BOOLEAN, true);
        assertFunction("REAL'-17.34' BETWEEN REAL'-18.98' AND REAL'-17.34'", BOOLEAN, true);
        assertFunction("REAL'0.0' BETWEEN REAL'-1.2' AND REAL'2.3'", BOOLEAN, true);
        assertFunction("REAL'56.78' BETWEEN REAL'12.34' AND REAL'34.56'", BOOLEAN, false);
        assertFunction("REAL'56.78' BETWEEN REAL'78.89' AND REAL'98.765'", BOOLEAN, false);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("CAST(REAL'754.1985' as VARCHAR)", VARCHAR, "754.1985");
        assertFunction("CAST(REAL'-754.2008' as VARCHAR)", VARCHAR, "-754.2008");
        assertFunction("CAST(REAL'Infinity' as VARCHAR)", VARCHAR, "Infinity");
        assertFunction("CAST(REAL'0.0' / REAL'0.0' as VARCHAR)", VARCHAR, "NaN");
        assertFunction("CAST(REAL'1.20' as VARCHAR(3))", createVarcharType(3), "1.2");
        assertNumericOverflow("CAST(REAL'17.1' as VARCHAR(3))", "Out of range for varchar(3): 17.1");
        assertNumericOverflow("CAST(REAL'1.0' as VARCHAR(2))", "Out of range for varchar(2): 1.0");
    }

    @Test
    public void testCastToChar()
            throws Exception
    {
        assertFunction("CAST(REAL'754.1985' as CHAR(8))", createCharType(8), "754.1985");
        assertFunction("CAST(REAL'-754.2008' as CHAR(9))", createCharType(9), "-754.2008");
        assertFunction("CAST(REAL'Infinity' as CHAR(8))", createCharType(8), "Infinity");
        assertFunction("CAST(REAL'0.0' / REAL'0.0' as CHAR(3))", createCharType(3), "NaN");
        assertNumericOverflow("CAST(37.7 as CHAR)", "Out of range for char(1): 37.7");
        assertNumericOverflow("CAST(1.0 as CHAR(2))", "Out of range for char(2): 1.0");
    }

    @Test
    public void testCastToBigInt()
            throws Exception
    {
        assertFunction("CAST(REAL'754.1985' as BIGINT)", BIGINT, 754L);
        assertFunction("CAST(REAL'-754.2008' as BIGINT)", BIGINT, -754L);
        assertFunction("CAST(REAL'1.98' as BIGINT)", BIGINT, 2L);
        assertFunction("CAST(REAL'-0.0' as BIGINT)", BIGINT, 0L);
    }

    @Test
    public void testCastToInteger()
            throws Exception
    {
        assertFunction("CAST(REAL'754.2008' AS INTEGER)", INTEGER, 754);
        assertFunction("CAST(REAL'-754.1985' AS INTEGER)", INTEGER, -754);
        assertFunction("CAST(REAL'9.99' AS INTEGER)", INTEGER, 10);
        assertFunction("CAST(REAL'-0.0' AS INTEGER)", INTEGER, 0);
    }

    @Test
    public void testCastToSmallint()
            throws Exception
    {
        assertFunction("CAST(REAL'754.2008' AS SMALLINT)", SMALLINT, (short) 754);
        assertFunction("CAST(REAL'-754.1985' AS SMALLINT)", SMALLINT, (short) -754);
        assertFunction("CAST(REAL'9.99' AS SMALLINT)", SMALLINT, (short) 10);
        assertFunction("CAST(REAL'-0.0' AS SMALLINT)", SMALLINT, (short) 0);
    }

    @Test
    public void testCastToTinyint()
            throws Exception
    {
        assertFunction("CAST(REAL'127.45' AS TINYINT)", TINYINT, (byte) 127);
        assertFunction("CAST(REAL'-128.234' AS TINYINT)", TINYINT, (byte) -128);
        assertFunction("CAST(REAL'9.99' AS TINYINT)", TINYINT, (byte) 10);
        assertFunction("CAST(REAL'-0.0' AS TINYINT)", TINYINT, (byte) 0);
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        assertFunction("CAST(REAL'754.1985' AS DOUBLE)", DOUBLE, (double) 754.1985f);
        assertFunction("CAST(REAL'-754.2008' AS DOUBLE)", DOUBLE, (double) -754.2008f);
        assertFunction("CAST(REAL'0.0' AS DOUBLE)", DOUBLE, (double) 0.0f);
        assertFunction("CAST(REAL'-0.0' AS DOUBLE)", DOUBLE, (double) -0.0f);
        assertFunction("CAST(CAST(REAL'754.1985' AS DOUBLE) AS REAL)", REAL, 754.1985f);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("CAST(REAL'754.1985' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(REAL'0.0' AS BOOLEAN)", BOOLEAN, false);
        assertFunction("CAST(REAL'-0.0' AS BOOLEAN)", BOOLEAN, false);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("CAST('754.1985' as REAL)", REAL, (float) 754.1985);
        assertFunction("CAST('-754.2008' as REAL)", REAL, (float) -754.2008);
    }

    @Test
    public void testCastFromChar()
            throws Exception
    {
        assertFunction("CAST(CAST('754.1985' as CHAR(8)) as REAL)", REAL, (float) 754.1985);
        assertFunction("CAST(CAST('-754.2008' as CHAR(9)) as REAL)", REAL, (float) -754.2008);
        assertFunction("CAST(CAST('-754.2008 ' as CHAR(10)) as REAL)", REAL, (float) -754.2008);
    }

    @Test
    public void testIsDistinctFrom()
            throws Exception
    {
        assertFunction("CAST(NULL AS REAL) IS DISTINCT FROM CAST(NULL AS REAL)", BOOLEAN, false);
        assertFunction("REAL'37.7' IS DISTINCT FROM REAL'37.7'", BOOLEAN, false);
        assertFunction("REAL'37.7' IS DISTINCT FROM REAL'37.8'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM REAL'37.7'", BOOLEAN, true);
        assertFunction("REAL'37.7' IS DISTINCT FROM NULL", BOOLEAN, true);
    }
}
