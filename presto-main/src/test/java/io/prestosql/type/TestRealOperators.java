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
package io.prestosql.type;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Float.isNaN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRealOperators
        extends AbstractTestFunctions
{
    @Test
    public void testTypeConstructor()
    {
        assertFunction("REAL'12.2'", REAL, 12.2f);
        assertFunction("REAL'-17.76'", REAL, -17.76f);
        assertFunction("REAL'NaN'", REAL, Float.NaN);
        assertFunction("REAL'Infinity'", REAL, Float.POSITIVE_INFINITY);
        assertFunction("REAL'-Infinity'", REAL, Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testAdd()
    {
        assertFunction("REAL'12.34' + REAL'56.78'", REAL, 12.34f + 56.78f);
        assertFunction("REAL'-17.34' + REAL'-22.891'", REAL, -17.34f + -22.891f);
        assertFunction("REAL'-89.123' + REAL'754.0'", REAL, -89.123f + 754.0f);
        assertFunction("REAL'-0.0' + REAL'0.0'", REAL, -0.0f + 0.0f);
    }

    @Test
    public void testSubtract()
    {
        assertFunction("REAL'12.34' - REAL'56.78'", REAL, 12.34f - 56.78f);
        assertFunction("REAL'-17.34' - REAL'-22.891'", REAL, -17.34f - -22.891f);
        assertFunction("REAL'-89.123' - REAL'754.0'", REAL, -89.123f - 754.0f);
        assertFunction("REAL'-0.0' - REAL'0.0'", REAL, -0.0f - 0.0f);
    }

    @Test
    public void testMultiply()
    {
        assertFunction("REAL'12.34' * REAL'56.78'", REAL, 12.34f * 56.78f);
        assertFunction("REAL'-17.34' * REAL'-22.891'", REAL, -17.34f * -22.891f);
        assertFunction("REAL'-89.123' * REAL'754.0'", REAL, -89.123f * 754.0f);
        assertFunction("REAL'-0.0' * REAL'0.0'", REAL, -0.0f * 0.0f);
        assertFunction("REAL'-17.71' * REAL'-1.0'", REAL, -17.71f * -1.0f);
    }

    @Test
    public void testDivide()
    {
        assertFunction("REAL'12.34' / REAL'56.78'", REAL, 12.34f / 56.78f);
        assertFunction("REAL'-17.34' / REAL'-22.891'", REAL, -17.34f / -22.891f);
        assertFunction("REAL'-89.123' / REAL'754.0'", REAL, -89.123f / 754.0f);
        assertFunction("REAL'-0.0' / REAL'0.0'", REAL, -0.0f / 0.0f);
        assertFunction("REAL'-17.71' / REAL'-1.0'", REAL, -17.71f / -1.0f);
    }

    @Test
    public void testModulus()
    {
        assertFunction("REAL'12.34' % REAL'56.78'", REAL, 12.34f % 56.78f);
        assertFunction("REAL'-17.34' % REAL'-22.891'", REAL, -17.34f % -22.891f);
        assertFunction("REAL'-89.123' % REAL'754.0'", REAL, -89.123f % 754.0f);
        assertFunction("REAL'-0.0' % REAL'0.0'", REAL, -0.0f % 0.0f);
        assertFunction("REAL'-17.71' % REAL'-1.0'", REAL, -17.71f % -1.0f);
    }

    @Test
    public void testNegation()
    {
        assertFunction("-REAL'12.34'", REAL, -12.34f);
        assertFunction("-REAL'-17.34'", REAL, 17.34f);
        assertFunction("-REAL'-0.0'", REAL, -(-0.0f));
    }

    @Test
    public void testEqual()
    {
        assertFunction("REAL'12.34' = REAL'12.34'", BOOLEAN, true);
        assertFunction("REAL'12.340' = REAL'12.34'", BOOLEAN, true);
        assertFunction("REAL'-17.34' = REAL'-17.34'", BOOLEAN, true);
        assertFunction("REAL'71.17' = REAL'23.45'", BOOLEAN, false);
        assertFunction("REAL'-0.0' = REAL'0.0'", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("REAL'12.34' <> REAL'12.34'", BOOLEAN, false);
        assertFunction("REAL'12.34' <> REAL'12.340'", BOOLEAN, false);
        assertFunction("REAL'-17.34' <> REAL'-17.34'", BOOLEAN, false);
        assertFunction("REAL'71.17' <> REAL'23.45'", BOOLEAN, true);
        assertFunction("REAL'-0.0' <> REAL'0.0'", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("REAL'12.34' < REAL'754.123'", BOOLEAN, true);
        assertFunction("REAL'-17.34' < REAL'-16.34'", BOOLEAN, true);
        assertFunction("REAL'71.17' < REAL'23.45'", BOOLEAN, false);
        assertFunction("REAL'-0.0' < REAL'0.0'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("REAL'12.34' <= REAL'754.123'", BOOLEAN, true);
        assertFunction("REAL'-17.34' <= REAL'-17.34'", BOOLEAN, true);
        assertFunction("REAL'71.17' <= REAL'23.45'", BOOLEAN, false);
        assertFunction("REAL'-0.0' <= REAL'0.0'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("REAL'12.34' > REAL'754.123'", BOOLEAN, false);
        assertFunction("REAL'-17.34' > REAL'-17.34'", BOOLEAN, false);
        assertFunction("REAL'71.17' > REAL'23.45'", BOOLEAN, true);
        assertFunction("REAL'-0.0' > REAL'0.0'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("REAL'12.34' >= REAL'754.123'", BOOLEAN, false);
        assertFunction("REAL'-17.34' >= REAL'-17.34'", BOOLEAN, true);
        assertFunction("REAL'71.17' >= REAL'23.45'", BOOLEAN, true);
        assertFunction("REAL'-0.0' >= REAL'0.0'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
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
    {
        assertFunction("CAST(REAL'754.1985' as VARCHAR)", VARCHAR, "754.1985");
        assertFunction("CAST(REAL'-754.2008' as VARCHAR)", VARCHAR, "-754.2008");
        assertFunction("CAST(REAL'Infinity' as VARCHAR)", VARCHAR, "Infinity");
        assertFunction("CAST(REAL'0.0' / REAL'0.0' as VARCHAR)", VARCHAR, "NaN");
    }

    @Test
    public void testCastToBigInt()
    {
        assertFunction("CAST(REAL'754.1985' as BIGINT)", BIGINT, 754L);
        assertFunction("CAST(REAL'-754.2008' as BIGINT)", BIGINT, -754L);
        assertFunction("CAST(REAL'1.98' as BIGINT)", BIGINT, 2L);
        assertFunction("CAST(REAL'-0.0' as BIGINT)", BIGINT, 0L);
    }

    @Test
    public void testCastToInteger()
    {
        assertFunction("CAST(REAL'754.2008' AS INTEGER)", INTEGER, 754);
        assertFunction("CAST(REAL'-754.1985' AS INTEGER)", INTEGER, -754);
        assertFunction("CAST(REAL'9.99' AS INTEGER)", INTEGER, 10);
        assertFunction("CAST(REAL'-0.0' AS INTEGER)", INTEGER, 0);
    }

    @Test
    public void testCastToSmallint()
    {
        assertFunction("CAST(REAL'754.2008' AS SMALLINT)", SMALLINT, (short) 754);
        assertFunction("CAST(REAL'-754.1985' AS SMALLINT)", SMALLINT, (short) -754);
        assertFunction("CAST(REAL'9.99' AS SMALLINT)", SMALLINT, (short) 10);
        assertFunction("CAST(REAL'-0.0' AS SMALLINT)", SMALLINT, (short) 0);
    }

    @Test
    public void testCastToTinyint()
    {
        assertFunction("CAST(REAL'127.45' AS TINYINT)", TINYINT, (byte) 127);
        assertFunction("CAST(REAL'-128.234' AS TINYINT)", TINYINT, (byte) -128);
        assertFunction("CAST(REAL'9.99' AS TINYINT)", TINYINT, (byte) 10);
        assertFunction("CAST(REAL'-0.0' AS TINYINT)", TINYINT, (byte) 0);
    }

    @Test
    public void testCastToDouble()
    {
        assertFunction("CAST(REAL'754.1985' AS DOUBLE)", DOUBLE, (double) 754.1985f);
        assertFunction("CAST(REAL'-754.2008' AS DOUBLE)", DOUBLE, (double) -754.2008f);
        assertFunction("CAST(REAL'0.0' AS DOUBLE)", DOUBLE, (double) 0.0f);
        assertFunction("CAST(REAL'-0.0' AS DOUBLE)", DOUBLE, (double) -0.0f);
        assertFunction("CAST(CAST(REAL'754.1985' AS DOUBLE) AS REAL)", REAL, 754.1985f);
    }

    @Test
    public void testCastToBoolean()
    {
        assertFunction("CAST(REAL'754.1985' AS BOOLEAN)", BOOLEAN, true);
        assertFunction("CAST(REAL'0.0' AS BOOLEAN)", BOOLEAN, false);
        assertFunction("CAST(REAL'-0.0' AS BOOLEAN)", BOOLEAN, false);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertFunction("CAST(NULL AS REAL) IS DISTINCT FROM CAST(NULL AS REAL)", BOOLEAN, false);
        assertFunction("REAL'37.7' IS DISTINCT FROM REAL'37.7'", BOOLEAN, false);
        assertFunction("REAL'37.7' IS DISTINCT FROM REAL'37.8'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM REAL'37.7'", BOOLEAN, true);
        assertFunction("REAL'37.7' IS DISTINCT FROM NULL", BOOLEAN, true);
        assertFunction("CAST(nan() AS REAL) IS DISTINCT FROM CAST(nan() AS REAL)", BOOLEAN, false);
    }

    @Test
    public void testIndeterminate()
            throws Exception
    {
        assertOperator(INDETERMINATE, "cast(null as real)", BOOLEAN, true);
        assertOperator(INDETERMINATE, "cast(-1.2 as real)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(1.2 as real)", BOOLEAN, false);
        assertOperator(INDETERMINATE, "cast(123 as real)", BOOLEAN, false);
    }

    @Test
    public void testNanHash()
    {
        int[] nanRepresentations = {floatToIntBits(Float.NaN), 0xffc00000, 0x7fc00000, 0x7fc01234, 0xffc01234};
        for (int nanRepresentation : nanRepresentations) {
            assertTrue(isNaN(intBitsToFloat(nanRepresentation)));
            assertEquals(RealOperators.hashCode(nanRepresentation), RealOperators.hashCode(nanRepresentations[0]));
        }
    }
}
