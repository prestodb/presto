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
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestFloatOperators
        extends AbstractTestFunctions
{
    @Test
    public void testTypeConstructor()
            throws Exception
    {
        assertFunction("FLOAT'12.2'", FLOAT, 12.2f);
        assertFunction("FLOAT'-17.76'", FLOAT, -17.76f);
        assertFunction("FLOAT'NaN'", FLOAT, Float.NaN);
        assertFunction("FLOAT'Infinity'", FLOAT, Float.POSITIVE_INFINITY);
        assertFunction("FLOAT'-Infinity'", FLOAT, Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("FLOAT'12.34' + FLOAT'56.78'", FLOAT, 12.34f + 56.78f);
        assertFunction("FLOAT'-17.34' + FLOAT'-22.891'", FLOAT, -17.34f + -22.891f);
        assertFunction("FLOAT'-89.123' + FLOAT'754.0'", FLOAT, -89.123f + 754.0f);
        assertFunction("FLOAT'-0.0' + FLOAT'0.0'", FLOAT, -0.0f + 0.0f);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("FLOAT'12.34' - FLOAT'56.78'", FLOAT, 12.34f - 56.78f);
        assertFunction("FLOAT'-17.34' - FLOAT'-22.891'", FLOAT, -17.34f - -22.891f);
        assertFunction("FLOAT'-89.123' - FLOAT'754.0'", FLOAT, -89.123f - 754.0f);
        assertFunction("FLOAT'-0.0' - FLOAT'0.0'", FLOAT, -0.0f - 0.0f);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("FLOAT'12.34' * FLOAT'56.78'", FLOAT, 12.34f * 56.78f);
        assertFunction("FLOAT'-17.34' * FLOAT'-22.891'", FLOAT, -17.34f * -22.891f);
        assertFunction("FLOAT'-89.123' * FLOAT'754.0'", FLOAT, -89.123f * 754.0f);
        assertFunction("FLOAT'-0.0' * FLOAT'0.0'", FLOAT, -0.0f * 0.0f);
        assertFunction("FLOAT'-17.71' * FLOAT'-1.0'", FLOAT, -17.71f * -1.0f);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("FLOAT'12.34' / FLOAT'56.78'", FLOAT, 12.34f / 56.78f);
        assertFunction("FLOAT'-17.34' / FLOAT'-22.891'", FLOAT, -17.34f / -22.891f);
        assertFunction("FLOAT'-89.123' / FLOAT'754.0'", FLOAT, -89.123f / 754.0f);
        assertFunction("FLOAT'-0.0' / FLOAT'0.0'", FLOAT, -0.0f / 0.0f);
        assertFunction("FLOAT'-17.71' / FLOAT'-1.0'", FLOAT, -17.71f / -1.0f);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("FLOAT'12.34' % FLOAT'56.78'", FLOAT, 12.34f % 56.78f);
        assertFunction("FLOAT'-17.34' % FLOAT'-22.891'", FLOAT, -17.34f % -22.891f);
        assertFunction("FLOAT'-89.123' % FLOAT'754.0'", FLOAT, -89.123f % 754.0f);
        assertFunction("FLOAT'-0.0' % FLOAT'0.0'", FLOAT, -0.0f % 0.0f);
        assertFunction("FLOAT'-17.71' % FLOAT'-1.0'", FLOAT, -17.71f % -1.0f);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-FLOAT'12.34'", FLOAT, -12.34f);
        assertFunction("-FLOAT'-17.34'", FLOAT, 17.34f);
        assertFunction("-FLOAT'-0.0'", FLOAT, -(-0.0f));
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("FLOAT'12.34' = FLOAT'12.34'", BOOLEAN, true);
        assertFunction("FLOAT'12.340' = FLOAT'12.34'", BOOLEAN, true);
        assertFunction("FLOAT'-17.34' = FLOAT'-17.34'", BOOLEAN, true);
        assertFunction("FLOAT'71.17' = FLOAT'23.45'", BOOLEAN, false);
        assertFunction("FLOAT'-0.0' = FLOAT'0.0'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("FLOAT'12.34' <> FLOAT'12.34'", BOOLEAN, false);
        assertFunction("FLOAT'12.34' <> FLOAT'12.340'", BOOLEAN, false);
        assertFunction("FLOAT'-17.34' <> FLOAT'-17.34'", BOOLEAN, false);
        assertFunction("FLOAT'71.17' <> FLOAT'23.45'", BOOLEAN, true);
        assertFunction("FLOAT'-0.0' <> FLOAT'0.0'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("FLOAT'12.34' < FLOAT'754.123'", BOOLEAN, true);
        assertFunction("FLOAT'-17.34' < FLOAT'-16.34'", BOOLEAN, true);
        assertFunction("FLOAT'71.17' < FLOAT'23.45'", BOOLEAN, false);
        assertFunction("FLOAT'-0.0' < FLOAT'0.0'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("FLOAT'12.34' <= FLOAT'754.123'", BOOLEAN, true);
        assertFunction("FLOAT'-17.34' <= FLOAT'-17.34'", BOOLEAN, true);
        assertFunction("FLOAT'71.17' <= FLOAT'23.45'", BOOLEAN, false);
        assertFunction("FLOAT'-0.0' <= FLOAT'0.0'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("FLOAT'12.34' > FLOAT'754.123'", BOOLEAN, false);
        assertFunction("FLOAT'-17.34' > FLOAT'-17.34'", BOOLEAN, false);
        assertFunction("FLOAT'71.17' > FLOAT'23.45'", BOOLEAN, true);
        assertFunction("FLOAT'-0.0' > FLOAT'0.0'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("FLOAT'12.34' >= FLOAT'754.123'", BOOLEAN, false);
        assertFunction("FLOAT'-17.34' >= FLOAT'-17.34'", BOOLEAN, true);
        assertFunction("FLOAT'71.17' >= FLOAT'23.45'", BOOLEAN, true);
        assertFunction("FLOAT'-0.0' >= FLOAT'0.0'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("FLOAT'12.34' BETWEEN FLOAT'9.12' AND FLOAT'25.89'", BOOLEAN, true);
        assertFunction("FLOAT'-17.34' BETWEEN FLOAT'-17.34' AND FLOAT'-16.57'", BOOLEAN, true);
        assertFunction("FLOAT'-17.34' BETWEEN FLOAT'-18.98' AND FLOAT'-17.34'", BOOLEAN, true);
        assertFunction("FLOAT'0.0' BETWEEN FLOAT'-1.2' AND FLOAT'2.3'", BOOLEAN, true);
        assertFunction("FLOAT'56.78' BETWEEN FLOAT'12.34' AND FLOAT'34.56'", BOOLEAN, false);
        assertFunction("FLOAT'56.78' BETWEEN FLOAT'78.89' AND FLOAT'98.765'", BOOLEAN, false);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("CAST(FLOAT'754.1985' as VARCHAR)", VARCHAR, "754.1985");
        assertFunction("CAST(FLOAT'-754.2008' as VARCHAR)", VARCHAR, "-754.2008");
        assertFunction("CAST(FLOAT'Infinity' as VARCHAR)", VARCHAR, "Infinity");
        assertFunction("CAST(FLOAT'0.0' / FLOAT'0.0' as VARCHAR)", VARCHAR, "NaN");
    }

    @Test
    public void testCastToBigInt()
            throws Exception
    {
        assertFunction("CAST(FLOAT'754.1985' as BIGINT)", BIGINT, 754L);
        assertFunction("CAST(FLOAT'-754.2008' as BIGINT)", BIGINT, -754L);
        assertFunction("CAST(FLOAT'1.98' as BIGINT)", BIGINT, 2L);
        assertFunction("CAST(FLOAT'-0.0' as BIGINT)", BIGINT, 0L);
    }

    @Test
    public void testCastToInteger()
            throws Exception
    {
        assertFunction("CAST(FLOAT'754.2008' AS INTEGER)", INTEGER, 754);
        assertFunction("CAST(FLOAT'-754.1985' AS INTEGER)", INTEGER, -754);
        assertFunction("CAST(FLOAT'9.99' AS INTEGER)", INTEGER, 10);
        assertFunction("CAST(FLOAT'-0.0' AS INTEGER)", INTEGER, 0);
    }

    @Test
    public void testCastToSmallint()
            throws Exception
    {
        assertFunction("CAST(FLOAT'754.2008' AS SMALLINT)", SMALLINT, (short) 754);
        assertFunction("CAST(FLOAT'-754.1985' AS SMALLINT)", SMALLINT, (short) -754);
        assertFunction("CAST(FLOAT'9.99' AS SMALLINT)", SMALLINT, (short) 10);
        assertFunction("CAST(FLOAT'-0.0' AS SMALLINT)", SMALLINT, (short) 0);
    }

    @Test
    public void testCastToTinyint()
            throws Exception
    {
        assertFunction("CAST(FLOAT'127.45' AS TINYINT)", TINYINT, (byte) 127);
        assertFunction("CAST(FLOAT'-128.234' AS TINYINT)", TINYINT, (byte) -128);
        assertFunction("CAST(FLOAT'9.99' AS TINYINT)", TINYINT, (byte) 10);
        assertFunction("CAST(FLOAT'-0.0' AS TINYINT)", TINYINT, (byte) 0);
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        assertFunction("CAST(FLOAT'754.1985' AS DOUBLE)", DOUBLE, (double) 754.1985f);
        assertFunction("CAST(FLOAT'-754.2008' AS DOUBLE)", DOUBLE, (double) -754.2008f);
        assertFunction("CAST(FLOAT'0.0' AS DOUBLE)", DOUBLE, (double) 0.0f);
        assertFunction("CAST(FLOAT'-0.0' AS DOUBLE)", DOUBLE, (double) -0.0f);
        assertFunction("CAST(CAST(FLOAT'754.1985' AS DOUBLE) AS FLOAT)", FLOAT, 754.1985f);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("CAST(FLOAT'754.1985' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(FLOAT'0.0' AS BOOLEAN)", BOOLEAN, false);
        assertFunction("CAST(FLOAT'-0.0' AS BOOLEAN)", BOOLEAN, false);
    }
}
