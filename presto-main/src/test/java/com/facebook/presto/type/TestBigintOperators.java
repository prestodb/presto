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

import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class TestBigintOperators
        extends AbstractTestFunctions
{
    @Test
    public void testLiteral()
            throws Exception
    {
        assertFunction("100000000037", BIGINT, 100000000037L);
        assertFunction("100000000017", BIGINT, 100000000017L);
    }

    @Test
    public void testUnaryPlus()
            throws Exception
    {
        assertFunction("+100000000037", BIGINT, 100000000037L);
        assertFunction("+100000000017", BIGINT, 100000000017L);
    }

    @Test
    public void testUnaryMinus()
            throws Exception
    {
        assertFunction("-100000000037", BIGINT, -100000000037L);
        assertFunction("-100000000017", BIGINT, -100000000017L);
    }

    @Test
    public void testAdd()
            throws Exception
    {
        assertFunction("37 + 100000000037", BIGINT, 37 + 100000000037L);
        assertFunction("37 + 100000000017", BIGINT, 37 + 100000000017L);
        assertFunction("100000000017 + 37", BIGINT, 100000000017L + 37L);
        assertFunction("100000000017 + 100000000017", BIGINT, 100000000017L + 100000000017L);
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertFunction("100000000037 - 37", BIGINT, 100000000037L - 37L);
        assertFunction("37 - 100000000017", BIGINT, 37 - 100000000017L);
        assertFunction("100000000017 - 37", BIGINT, 100000000017L - 37L);
        assertFunction("100000000017 - 100000000017", BIGINT, 100000000017L - 100000000017L);
    }

    @Test
    public void testMultiply()
            throws Exception
    {
        assertFunction("100000000037 * 37", BIGINT, 100000000037L * 37L);
        assertFunction("37 * 100000000017", BIGINT, 37 * 100000000017L);
        assertFunction("100000000017 * 37", BIGINT, 100000000017L * 37L);
        assertFunction("100000000017 * 10000017", BIGINT, 100000000017L * 10000017L);
    }

    @Test
    public void testDivide()
            throws Exception
    {
        assertFunction("100000000037 / 37", BIGINT, 100000000037L / 37L);
        assertFunction("37 / 100000000017", BIGINT, 37 / 100000000017L);
        assertFunction("100000000017 / 37", BIGINT, 100000000017L / 37L);
        assertFunction("100000000017 / 100000000017", BIGINT, 100000000017L / 100000000017L);
    }

    @Test
    public void testModulus()
            throws Exception
    {
        assertFunction("100000000037 % 37", BIGINT, 100000000037L % 37L);
        assertFunction("37 % 100000000017", BIGINT, 37 % 100000000017L);
        assertFunction("100000000017 % 37", BIGINT, 100000000017L % 37L);
        assertFunction("100000000017 % 100000000017", BIGINT, 100000000017L % 100000000017L);
    }

    @Test
    public void testNegation()
            throws Exception
    {
        assertFunction("-(100000000037)", BIGINT, -100000000037L);
        assertFunction("-(100000000017)", BIGINT, -100000000017L);
    }

    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("100000000037 = 100000000037", BOOLEAN, true);
        assertFunction("37 = 100000000017", BOOLEAN, false);
        assertFunction("100000000017 = 37", BOOLEAN, false);
        assertFunction("100000000017 = 100000000017", BOOLEAN, true);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("100000000037 <> 100000000037", BOOLEAN, false);
        assertFunction("37 <> 100000000017", BOOLEAN, true);
        assertFunction("100000000017 <> 37", BOOLEAN, true);
        assertFunction("100000000017 <> 100000000017", BOOLEAN, false);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("100000000037 < 100000000037", BOOLEAN, false);
        assertFunction("100000000037 < 100000000017", BOOLEAN, false);
        assertFunction("100000000017 < 100000000037", BOOLEAN, true);
        assertFunction("100000000017 < 100000000017", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("100000000037 <= 100000000037", BOOLEAN, true);
        assertFunction("100000000037 <= 100000000017", BOOLEAN, false);
        assertFunction("100000000017 <= 100000000037", BOOLEAN, true);
        assertFunction("100000000017 <= 100000000017", BOOLEAN, true);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("100000000037 > 100000000037", BOOLEAN, false);
        assertFunction("100000000037 > 100000000017", BOOLEAN, true);
        assertFunction("100000000017 > 100000000037", BOOLEAN, false);
        assertFunction("100000000017 > 100000000017", BOOLEAN, false);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("100000000037 >= 100000000037", BOOLEAN, true);
        assertFunction("100000000037 >= 100000000017", BOOLEAN, true);
        assertFunction("100000000017 >= 100000000037", BOOLEAN, false);
        assertFunction("100000000017 >= 100000000017", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("100000000037 BETWEEN 100000000037 AND 100000000037", BOOLEAN, true);
        assertFunction("100000000037 BETWEEN 100000000037 AND 100000000017", BOOLEAN, false);

        assertFunction("100000000037 BETWEEN 100000000017 AND 100000000037", BOOLEAN, true);
        assertFunction("100000000037 BETWEEN 100000000017 AND 100000000017", BOOLEAN, false);

        assertFunction("100000000017 BETWEEN 100000000037 AND 100000000037", BOOLEAN, false);
        assertFunction("100000000017 BETWEEN 100000000037 AND 100000000017", BOOLEAN, false);

        assertFunction("100000000017 BETWEEN 100000000017 AND 100000000037", BOOLEAN, true);
        assertFunction("100000000017 BETWEEN 100000000017 AND 100000000017", BOOLEAN, true);
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        assertFunction("cast(100000000037 as bigint)", BIGINT, 100000000037L);
        assertFunction("cast(100000000017 as bigint)", BIGINT, 100000000017L);
    }

    @Test
    public void testCastToVarchar()
            throws Exception
    {
        assertFunction("cast(37 as varchar)", VARCHAR, "37");
        assertFunction("cast(100000000017 as varchar)", VARCHAR, "100000000017");
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        assertFunction("cast(37 as double)", DOUBLE, 37.0);
        assertFunction("cast(100000000017 as double)", DOUBLE, 100000000017.0);
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        assertFunction("cast(37 as boolean)", BOOLEAN, true);
        assertFunction("cast(100000000017 as boolean)", BOOLEAN, true);
        assertFunction("cast(0 as boolean)", BOOLEAN, false);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast('100000000037' as bigint)", BIGINT, 100000000037L);
        assertFunction("cast('100000000017' as bigint)", BIGINT, 100000000017L);
    }

    @Test
    public void testOverflowAdd()
            throws Exception
    {
        assertInvalidFunction(format("%s + 1", Long.MAX_VALUE), NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testUnderflowSubtract()
            throws Exception
    {
        long minValue = Long.MIN_VALUE + 1; // due to https://github.com/facebook/presto/issues/4571 MIN_VALUE solely cannot be used
        assertInvalidFunction(format("%s - 2", minValue), NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testOverflowMultiply()
            throws Exception
    {
        assertInvalidFunction(format("%s * 2", Long.MAX_VALUE), NUMERIC_VALUE_OUT_OF_RANGE);
        //assertInvalidFunction(format("%s * -1", Long.MIN_VALUE), NUMERIC_VALUE_OUT_OF_RANGE); TODO: uncomment when https://github.com/facebook/presto/issues/4571 is fixed
    }

    @Test(enabled = false) // TODO: enable when https://github.com/facebook/presto/issues/4571 is fixed
    public void testOverflowDivide()
            throws Exception
    {
        assertInvalidFunction(format("%s / -1", Long.MIN_VALUE), NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test(enabled = false) // TODO: enable when https://github.com/facebook/presto/issues/4571 is fixed
    public void testNegateOverflow()
            throws Exception
    {
        assertInvalidFunction(format("-(%s)", Long.MIN_VALUE), NUMERIC_VALUE_OUT_OF_RANGE);
    }
}
