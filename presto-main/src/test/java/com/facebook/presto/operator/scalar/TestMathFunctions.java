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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.SqlDecimal;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestMathFunctions
        extends AbstractTestFunctions
{
    private static final double[] DOUBLE_VALUES = {123, -123, 123.45, -123.45};
    private static final long[] longLefts = {9, 10, 11, -9, -10, -11};
    private static final long[] longRights = {3, -3};
    private static final double[] doubleLefts = {9, 10, 11, -9, -10, -11, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1};
    private static final double[] doubleRights = {3, -3, 3.1, -3.1};

    @Test
    public void testAbs()
    {
        assertFunction("abs(123)", BIGINT, 123L);
        assertFunction("abs(-123)", BIGINT, 123L);
        assertFunction("abs(CAST(NULL AS BIGINT))", BIGINT, null);
        assertFunction("abs(DOUBLE '123.0')", DOUBLE, 123.0);
        assertFunction("abs(DOUBLE '-123.0')", DOUBLE, 123.0);
        assertFunction("abs(DOUBLE '123.45')", DOUBLE, 123.45);
        assertFunction("abs(DOUBLE '-123.45')", DOUBLE, 123.45);
        assertFunction("abs(CAST(NULL AS DOUBLE))", DOUBLE, null);
        assertInvalidFunction("abs(-9223372036854775807 - if(rand() < 10, 1, 1))", NUMERIC_VALUE_OUT_OF_RANGE);
        assertFunction("abs(DECIMAL '123.45')", createDecimalType(5, 2), SqlDecimal.of("12345", 5, 2));
        assertFunction("abs(DECIMAL '-123.45')", createDecimalType(5, 2), SqlDecimal.of("12345", 5, 2));
        assertFunction("abs(DECIMAL '1234567890123456.78')", createDecimalType(18, 2), SqlDecimal.of("123456789012345678", 18, 2));
        assertFunction("abs(DECIMAL '-1234567890123456.78')", createDecimalType(18, 2), SqlDecimal.of("123456789012345678", 18, 2));
        assertFunction("abs(CAST(NULL AS DECIMAL(1,0)))", createDecimalType(1, 0), null);
    }

    @Test
    public void testAcos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("acos(" + doubleValue + ")", DOUBLE, Math.acos(doubleValue));
        }
        assertFunction("acos(NULL)", DOUBLE, null);
    }

    @Test
    public void testAsin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("asin(" + doubleValue + ")", DOUBLE, Math.asin(doubleValue));
        }
        assertFunction("asin(NULL)", DOUBLE, null);
    }

    @Test
    public void testAtan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan(" + doubleValue + ")", DOUBLE, Math.atan(doubleValue));
        }
        assertFunction("atan(NULL)", DOUBLE, null);
    }

    @Test
    public void testAtan2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan2(" + doubleValue + ", " + doubleValue + ")", DOUBLE, Math.atan2(doubleValue, doubleValue));
        }
        assertFunction("atan2(NULL, NULL)", DOUBLE, null);
        assertFunction("atan2(1.0, NULL)", DOUBLE, null);
        assertFunction("atan2(NULL, 1.0)", DOUBLE, null);
    }

    @Test
    public void testCbrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cbrt(" + doubleValue + ")", DOUBLE, Math.cbrt(doubleValue));
        }
        assertFunction("cbrt(NULL)", DOUBLE, null);
    }

    @Test
    public void testCeil()
    {
        assertFunction("ceil(123)", BIGINT, 123);
        assertFunction("ceil(-123)", BIGINT, -123);
        assertFunction("ceil(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("ceil(DOUBLE '123.0')", DOUBLE, 123.0);
        assertFunction("ceil(DOUBLE '-123.0')", DOUBLE, -123.0);
        assertFunction("ceil(DOUBLE '123.45')", DOUBLE, 124.0);
        assertFunction("ceil(DOUBLE '-123.45')", DOUBLE, -123.0);
        assertFunction("ceil(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("ceil(DECIMAL '123.45')", createDecimalType(4), SqlDecimal.of("124"));
        assertFunction("ceil(DECIMAL '1234567890123456.78')", createDecimalType(17), SqlDecimal.of("1234567890123457"));
        assertFunction("ceiling(123)", BIGINT, 123);
        assertFunction("ceiling(-123)", BIGINT, -123);
        assertFunction("ceiling(CAST(NULL AS BIGINT))", BIGINT, null);
        assertFunction("ceiling(DOUBLE '123.0')", DOUBLE, 123.0);
        assertFunction("ceiling(DOUBLE '-123.0')", DOUBLE, -123.0);
        assertFunction("ceiling(DOUBLE '123.45')", DOUBLE, 124.0);
        assertFunction("ceiling(DOUBLE '-123.45')", DOUBLE, -123.0);
        assertFunction("ceiling(CAST(NULL AS DOUBLE))", DOUBLE, null);
        assertFunction("ceiling(DECIMAL '123')", createDecimalType(3), SqlDecimal.of("123"));
        assertFunction("ceiling(DECIMAL '-123')", createDecimalType(3), SqlDecimal.of("-123"));
        assertFunction("ceiling(DECIMAL '123.45')", createDecimalType(4), SqlDecimal.of("124"));
        assertFunction("ceiling(DECIMAL '-123.45')", createDecimalType(4), SqlDecimal.of("-123"));
        assertFunction("ceiling(DECIMAL '999.9')", createDecimalType(4), SqlDecimal.of("1000"));
        assertFunction("ceiling(DECIMAL '123456789012345678')", createDecimalType(18), SqlDecimal.of("123456789012345678"));
        assertFunction("ceiling(DECIMAL '-123456789012345678')", createDecimalType(18), SqlDecimal.of("-123456789012345678"));
        assertFunction("ceiling(DECIMAL '123456789012345678.00')", createDecimalType(19), SqlDecimal.of("123456789012345678"));
        assertFunction("ceiling(DECIMAL '123456789012345678.99')", createDecimalType(19), SqlDecimal.of("123456789012345679"));
        assertFunction("ceiling(DECIMAL '-123456789012345678.99')", createDecimalType(19), SqlDecimal.of("-123456789012345678"));
        assertFunction("ceiling(DECIMAL '999999999999999999.9')", createDecimalType(19), SqlDecimal.of("1000000000000000000"));
        assertFunction("ceiling(DECIMAL '1234567890123456.78')", createDecimalType(17), SqlDecimal.of("1234567890123457"));
        assertFunction("ceiling(DECIMAL '-1234567890123456.78')", createDecimalType(17), SqlDecimal.of("-1234567890123456"));
        assertFunction("ceiling(CAST(NULL AS DECIMAL(1,0)))", createDecimalType(1), null);
    }

    @Test
    public void testTruncate()
    {
        // DOUBLE
        final String maxDouble = Double.toString(Double.MAX_VALUE);
        final String minDouble = Double.toString(-Double.MAX_VALUE);
        assertFunction("truncate(17.18)", DOUBLE, 17.0);
        assertFunction("truncate(-17.18)", DOUBLE, -17.0);
        assertFunction("truncate(17.88)", DOUBLE, 17.0);
        assertFunction("truncate(-17.88)", DOUBLE, -17.0);
        assertFunction("truncate(NULL)", DOUBLE, null);
        assertFunction("truncate(CAST(NULL AS DOUBLE))", DOUBLE, null);
        assertFunction("truncate(" + maxDouble + ")", DOUBLE, Double.MAX_VALUE);
        assertFunction("truncate(" + minDouble + ")", DOUBLE, -Double.MAX_VALUE);

        // SHORT DECIMAL
        assertFunction("truncate(DECIMAL '1234', 1)", createDecimalType(4, 0), SqlDecimal.of("1234"));
        assertFunction("truncate(DECIMAL '1234', -1)", createDecimalType(4, 0), SqlDecimal.of("1230"));
        assertFunction("truncate(DECIMAL '1234.56', 1)", createDecimalType(6, 2), SqlDecimal.of("1234.50"));
        assertFunction("truncate(DECIMAL '1234.56', -1)", createDecimalType(6, 2), SqlDecimal.of("1230.00"));
        assertFunction("truncate(DECIMAL '-1234.56', 1)", createDecimalType(6, 2), SqlDecimal.of("-1234.50"));
        assertFunction("truncate(DECIMAL '1239.99', 1)", createDecimalType(6, 2), SqlDecimal.of("1239.90"));
        assertFunction("truncate(DECIMAL '-1239.99', 1)", createDecimalType(6, 2), SqlDecimal.of("-1239.90"));
        assertFunction("truncate(DECIMAL '1239.999', 2)", createDecimalType(7, 3), SqlDecimal.of("1239.990"));
        assertFunction("truncate(DECIMAL '1239.999', -2)", createDecimalType(7, 3), SqlDecimal.of("1200.000"));

        assertFunction("truncate(DECIMAL '1234', -4)", createDecimalType(4, 0), SqlDecimal.of("0000"));
        assertFunction("truncate(DECIMAL '1234.56', -4)", createDecimalType(6, 2), SqlDecimal.of("0000.00"));
        assertFunction("truncate(DECIMAL '-1234.56', -4)", createDecimalType(6, 2), SqlDecimal.of("0000.00"));

        assertFunction("truncate(DECIMAL '1234.56', 3)", createDecimalType(6, 2), SqlDecimal.of("1234.56"));
        assertFunction("truncate(DECIMAL '-1234.56', 3)", createDecimalType(6, 2), SqlDecimal.of("-1234.56"));

        // LONG DECIMAL
        assertFunction("truncate(DECIMAL '1234567890123456789012', 1)", createDecimalType(22, 0), SqlDecimal.of("1234567890123456789012"));
        assertFunction("truncate(DECIMAL '1234567890123456789012', -1)", createDecimalType(22, 0), SqlDecimal.of("1234567890123456789010"));
        assertFunction("truncate(DECIMAL '1234567890123456789012.23', 1)", createDecimalType(24, 2), SqlDecimal.of("1234567890123456789012.20"));
        assertFunction("truncate(DECIMAL '1234567890123456789012.23', -1)", createDecimalType(24, 2), SqlDecimal.of("1234567890123456789010.00"));
        assertFunction("truncate(DECIMAL '123456789012345678999.99', -1)", createDecimalType(23, 2), SqlDecimal.of("123456789012345678990.00"));
        assertFunction("truncate(DECIMAL '-123456789012345678999.99', -1)", createDecimalType(23, 2), SqlDecimal.of("-123456789012345678990.00"));
        assertFunction("truncate(DECIMAL '123456789012345678999.999', 2)", createDecimalType(24, 3), SqlDecimal.of("123456789012345678999.990"));
        assertFunction("truncate(DECIMAL '123456789012345678999.999', -2)", createDecimalType(24, 3), SqlDecimal.of("123456789012345678900.000"));

        assertFunction("truncate(DECIMAL '123456789012345678901', -21)", createDecimalType(21, 0), SqlDecimal.of("000000000000000000000"));
        assertFunction("truncate(DECIMAL '123456789012345678901.23', -21)", createDecimalType(23, 2), SqlDecimal.of("000000000000000000000.00"));

        assertFunction("truncate(DECIMAL '123456789012345678901.23', 3)", createDecimalType(23, 2), SqlDecimal.of("123456789012345678901.23"));
        assertFunction("truncate(DECIMAL '-123456789012345678901.23', 3)", createDecimalType(23, 2), SqlDecimal.of("-123456789012345678901.23"));

        // NULL DECIMAL
        assertFunction("truncate(CAST(NULL AS DECIMAL(1,0)), -1)", createDecimalType(1, 0), null);
        assertFunction("truncate(NULL, NULL)", createDecimalType(1, 0), null);

        // OUT OF RANGE DECIMAL
        assertInvalidFunction("truncate(DECIMAL '1234567890123456789012345678901234567890123', 0)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testCos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cos(" + doubleValue + ")", DOUBLE, Math.cos(doubleValue));
        }
        assertFunction("cos(NULL)", DOUBLE, null);
    }

    @Test
    public void testCosh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cosh(" + doubleValue + ")", DOUBLE, Math.cosh(doubleValue));
        }
        assertFunction("cosh(NULL)", DOUBLE, null);
    }

    @Test
    public void testDegrees()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction(String.format("degrees(%s)", doubleValue), DOUBLE, Math.toDegrees(doubleValue));
        }
        assertFunction("degrees(NULL)", DOUBLE, null);
    }

    @Test
    public void testE()
    {
        assertFunction("e()", DOUBLE, Math.E);
    }

    @Test
    public void testExp()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("exp(" + doubleValue + ")", DOUBLE, Math.exp(doubleValue));
        }
        assertFunction("exp(NULL)", DOUBLE, null);
    }

    @Test
    public void testFloor()
    {
        assertFunction("floor(123)", BIGINT, 123);
        assertFunction("floor(-123)", BIGINT, -123);
        assertFunction("floor(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("floor(DOUBLE '123.0')", DOUBLE, 123.0);
        assertFunction("floor(DOUBLE '-123.0')", DOUBLE, -123.0);
        assertFunction("floor(DOUBLE '123.45')", DOUBLE, 123.0);
        assertFunction("floor(DOUBLE '-123.45')", DOUBLE, -124.0);
        assertFunction("floor(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("floor(DECIMAL '123')", createDecimalType(3), SqlDecimal.of("123"));
        assertFunction("floor(DECIMAL '-123')", createDecimalType(3), SqlDecimal.of("-123"));
        assertFunction("floor(DECIMAL '123.45')", createDecimalType(4), SqlDecimal.of("123"));
        assertFunction("floor(DECIMAL '-123.45')", createDecimalType(4), SqlDecimal.of("-124"));
        assertFunction("floor(DECIMAL '-999.9')", createDecimalType(4), SqlDecimal.of("-1000"));
        assertFunction("floor(DECIMAL '123456789012345678')", createDecimalType(18), SqlDecimal.of("123456789012345678"));
        assertFunction("floor(DECIMAL '-123456789012345678')", createDecimalType(18), SqlDecimal.of("-123456789012345678"));
        assertFunction("floor(DECIMAL '123456789012345678.00')", createDecimalType(19), SqlDecimal.of("123456789012345678"));
        assertFunction("floor(DECIMAL '123456789012345678.99')", createDecimalType(19), SqlDecimal.of("123456789012345678"));
        assertFunction("floor(DECIMAL '-123456789012345678.99')", createDecimalType(19), SqlDecimal.of("-123456789012345679"));
        assertFunction("floor(DECIMAL '-999999999999999999.9')", createDecimalType(19), SqlDecimal.of("-1000000000000000000"));
        assertFunction("floor(DECIMAL '1234567890123456.78')", createDecimalType(17), SqlDecimal.of("1234567890123456"));
        assertFunction("floor(DECIMAL '-1234567890123456.78')", createDecimalType(17), SqlDecimal.of("-1234567890123457"));
        assertFunction("floor(CAST(NULL as DECIMAL(1,0)))", createDecimalType(1), null);
    }

    @Test
    public void testLn()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("ln(" + doubleValue + ")", DOUBLE, Math.log(doubleValue));
        }
        assertFunction("ln(NULL)", DOUBLE, null);
    }

    @Test
    public void testLog2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log2(" + doubleValue + ")", DOUBLE, Math.log(doubleValue) / Math.log(2));
        }
        assertFunction("log2(NULL)", DOUBLE, null);
    }

    @Test
    public void testLog10()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log10(" + doubleValue + ")", DOUBLE, Math.log10(doubleValue));
        }
        assertFunction("log10(NULL)", DOUBLE, null);
    }

    @Test
    public void testLog()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            for (double base : DOUBLE_VALUES) {
                assertFunction("log(" + doubleValue + ", " + base + ")", DOUBLE, Math.log(doubleValue) / Math.log(base));
            }
        }
        assertFunction("log(NULL, NULL)", DOUBLE, null);
        assertFunction("log(5.0, NULL)", DOUBLE, null);
        assertFunction("log(NULL, 5.0)", DOUBLE, null);
    }

    @Test
    public void testMod()
    {
        for (long left : longLefts) {
            for (long right : longRights) {
                assertFunction("mod(" + left + ", " + right + ")", BIGINT, left % right);
            }
        }

        for (long left : longLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(" + left + ", DOUBLE '" + right + "')", DOUBLE, left % right);
            }
        }

        for (double left : doubleLefts) {
            for (long right : longRights) {
                assertFunction("mod(DOUBLE '" + left + "', " + right + ")", DOUBLE, left % right);
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(DOUBLE '" + left + "', DOUBLE '" + right + "')", DOUBLE, left % right);
            }
        }

        assertFunction("mod(DECIMAL '0.0', DECIMAL '2.0')", createDecimalType(1, 1), SqlDecimal.of("0.0"));
        assertFunction("mod(DECIMAL '13.0', DECIMAL '5.0')", createDecimalType(2, 1), SqlDecimal.of("3.0"));
        assertFunction("mod(DECIMAL '-13.0', DECIMAL '5.0')", createDecimalType(2, 1), SqlDecimal.of("-3.0"));
        assertFunction("mod(DECIMAL '13.0', DECIMAL '-5.0')", createDecimalType(2, 1), SqlDecimal.of("3.0"));
        assertFunction("mod(DECIMAL '-13.0', DECIMAL '-5.0')", createDecimalType(2, 1), SqlDecimal.of("-3.0"));
        assertFunction("mod(DECIMAL '5.0', DECIMAL '2.5')", createDecimalType(2, 1), SqlDecimal.of("0.0"));
        assertFunction("mod(DECIMAL '5.0', DECIMAL '2.05')", createDecimalType(3, 2), SqlDecimal.of("0.90"));
        assertFunction("mod(DECIMAL '5.0', DECIMAL '2.55')", createDecimalType(3, 2), SqlDecimal.of("2.45"));
        assertFunction("mod(DECIMAL '5.0001', DECIMAL '2.55')", createDecimalType(5, 4), SqlDecimal.of("2.4501"));
        assertFunction("mod(DECIMAL '12345678901234567.90', DECIMAL '12345678901234567.89')", createDecimalType(19, 2), SqlDecimal.of("0.01"));
        assertFunction("mod(DECIMAL '5.0', CAST(NULL as DECIMAL(1,0)))", createDecimalType(2, 1), null);
        assertFunction("mod(CAST(NULL as DECIMAL(1,0)), DECIMAL '5.0')", createDecimalType(2, 1), null);
        assertInvalidFunction("mod(DECIMAL '5.0', DECIMAL '0')", DIVISION_BY_ZERO);
        assertFunction("mod(DOUBLE '5.0', NULL)", DOUBLE, null);
        assertFunction("mod(NULL, DOUBLE '5.0')", DOUBLE, null);
    }

    @Test
    public void testPi()
    {
        assertFunction("pi()", DOUBLE, Math.PI);
    }

    @Test
    public void testNaN()
    {
        assertFunction("nan()", DOUBLE, Double.NaN);
        assertFunction("CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE)", DOUBLE, Double.NaN);
    }

    @Test
    public void testInfinity()
    {
        assertFunction("infinity()", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("-rand() / 0.0", DOUBLE, Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testIsInfinite()
    {
        assertFunction("is_infinite(CAST(1.0 as DOUBLE) / CAST(0.0 as DOUBLE))", BOOLEAN, true);
        assertFunction("is_infinite(CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE))", BOOLEAN, false);
        assertFunction("is_infinite(CAST(1.0 as DOUBLE) / CAST(1.0 as DOUBLE))", BOOLEAN, false);
        assertFunction("is_infinite(NULL)", BOOLEAN, null);
    }

    @Test
    public void testIsFinite()
    {
        assertFunction("is_finite(100000)", BOOLEAN, true);
        assertFunction("is_finite(rand() / 0.0)", BOOLEAN, false);
        assertFunction("is_finite(NULL)", BOOLEAN, null);
    }

    @Test
    public void testIsNaN()
    {
        assertFunction("is_nan(CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE))", BOOLEAN, true);
        assertFunction("is_nan(CAST(0.0 as DOUBLE) / CAST(1.0 as DOUBLE))", BOOLEAN, false);
        assertFunction("is_nan(infinity() / infinity())", BOOLEAN, true);
        assertFunction("is_nan(nan())", BOOLEAN, true);
        assertFunction("is_nan(NULL)", BOOLEAN, null);
    }

    @Test
    public void testPower()
    {
        for (long left : longLefts) {
            for (long right : longRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }

        for (long left : longLefts) {
            for (double right : doubleRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }

        for (double left : doubleLefts) {
            for (long right : longRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("power(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }

        assertFunction("power(NULL, NULL)", DOUBLE, null);
        assertFunction("power(5.0, NULL)", DOUBLE, null);
        assertFunction("power(NULL, 5.0)", DOUBLE, null);

        // test alias
        assertFunction("pow(5.0, 2.0)", DOUBLE, 25.0);
    }

    @Test
    public void testRadians()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction(String.format("radians(%s)", doubleValue), DOUBLE, Math.toRadians(doubleValue));
        }
        assertFunction("radians(NULL)", DOUBLE, null);
    }

    @Test
    public void testRandom()
    {
        // random is non-deterministic
        functionAssertions.tryEvaluateWithAll("rand()", DOUBLE, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random()", DOUBLE, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("rand(1000)", BIGINT, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random(2000)", BIGINT, TEST_SESSION);

        assertInvalidFunction("rand(-1)", "bound must be positive");
    }

    @Test
    public void testRound()
    {
        assertFunction("round( 3)", BIGINT, 3);
        assertFunction("round(-3)", BIGINT, -3);
        assertFunction("round(CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("round(DOUBLE '3.0')", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.0')", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.499')", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.499')", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.5')", DOUBLE, 4.0);
        assertFunction("round(DOUBLE '-3.5')", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '-3.5001')", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '-3.99')", DOUBLE, -4.0);
        assertFunction("round(CAST(NULL as DOUBLE))", DOUBLE, null);
        assertFunction("round(DECIMAL '3')", createDecimalType(1, 0), SqlDecimal.of("3"));
        assertFunction("round(DECIMAL '-3')", createDecimalType(1, 0), SqlDecimal.of("-3"));
        assertFunction("round(DECIMAL '3.0')", createDecimalType(2, 0), SqlDecimal.of("3"));
        assertFunction("round(DECIMAL '-3.0')", createDecimalType(2, 0), SqlDecimal.of("-3"));
        assertFunction("round(DECIMAL '3.499')", createDecimalType(2, 0), SqlDecimal.of("3"));
        assertFunction("round(DECIMAL '-3.499')", createDecimalType(2, 0), SqlDecimal.of("-3"));
        assertFunction("round(DECIMAL '3.5')", createDecimalType(2, 0), SqlDecimal.of("4"));
        assertFunction("round(DECIMAL '-3.5')", createDecimalType(2, 0), SqlDecimal.of("-4"));
        assertFunction("round(DECIMAL '-3.5001')", createDecimalType(2, 0), SqlDecimal.of("-4"));
        assertFunction("round(DECIMAL '-3.99')", createDecimalType(2, 0), SqlDecimal.of("-4"));
        assertFunction("round(DECIMAL '9.9')", createDecimalType(2, 0), SqlDecimal.of("10"));
        assertFunction("round(DECIMAL '-9.9')", createDecimalType(2, 0), SqlDecimal.of("-10"));
        assertFunction("round(DECIMAL '123456789012345678')", createDecimalType(18, 0), SqlDecimal.of("123456789012345678"));
        assertFunction("round(DECIMAL '-123456789012345678')", createDecimalType(18, 0), SqlDecimal.of("-123456789012345678"));
        assertFunction("round(DECIMAL '123456789012345678.0')", createDecimalType(19, 0), SqlDecimal.of("123456789012345678"));
        assertFunction("round(DECIMAL '-123456789012345678.0')", createDecimalType(19, 0), SqlDecimal.of("-123456789012345678"));
        assertFunction("round(DECIMAL '123456789012345678.49')", createDecimalType(19, 0), SqlDecimal.of("123456789012345678"));
        assertFunction("round(DECIMAL '-123456789012345678.49')", createDecimalType(19, 0), SqlDecimal.of("-123456789012345678"));
        assertFunction("round(DECIMAL '123456789012345678.5')", createDecimalType(19, 0), SqlDecimal.of("123456789012345679"));
        assertFunction("round(DECIMAL '-123456789012345678.5')", createDecimalType(19, 0), SqlDecimal.of("-123456789012345679"));
        assertFunction("round(DECIMAL '123456789012345678.501')", createDecimalType(19, 0), SqlDecimal.of("123456789012345679"));
        assertFunction("round(DECIMAL '-123456789012345678.501')", createDecimalType(19, 0), SqlDecimal.of("-123456789012345679"));
        assertFunction("round(DECIMAL '999999999999999999.9')", createDecimalType(19, 0), SqlDecimal.of("1000000000000000000"));
        assertFunction("round(DECIMAL '-999999999999999999.9')", createDecimalType(19, 0), SqlDecimal.of("-1000000000000000000"));
        assertFunction("round(CAST(NULL as DECIMAL(1,0)))", createDecimalType(1, 0), null);

        assertFunction("round( 3, 0)", BIGINT, 3);
        assertFunction("round(-3, 0)", BIGINT, -3);
        assertFunction("round(DOUBLE '3.0', 0)", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.0', 0)", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.499', 0)", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.499', 0)", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.5', 0)", DOUBLE, 4.0);
        assertFunction("round(DOUBLE '-3.5', 0)", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '-3.5001', 0)", DOUBLE, -4.0);
        assertFunction("round(DOUBLE '-3.99', 0)", DOUBLE, -4.0);
        assertFunction("round(DECIMAL '3', 0)", createDecimalType(2, 0), SqlDecimal.of("3"));
        assertFunction("round(DECIMAL '-3', 0)", createDecimalType(2, 0), SqlDecimal.of("-3"));
        assertFunction("round(DECIMAL '3.0', 0)", createDecimalType(3, 1), SqlDecimal.of("3.0"));
        assertFunction("round(DECIMAL '-3.0', 0)", createDecimalType(3, 1), SqlDecimal.of("-3.0"));
        assertFunction("round(DECIMAL '3.499', 0)", createDecimalType(5, 3), SqlDecimal.of("3.000"));
        assertFunction("round(DECIMAL '-3.499', 0)", createDecimalType(5, 3), SqlDecimal.of("-3.000"));
        assertFunction("round(DECIMAL '3.5', 0)", createDecimalType(3, 1), SqlDecimal.of("4.0"));
        assertFunction("round(DECIMAL '-3.5', 0)", createDecimalType(3, 1), SqlDecimal.of("-4.0"));
        assertFunction("round(DECIMAL '-3.5001', 0)", createDecimalType(6, 4), SqlDecimal.of("-4.0000"));
        assertFunction("round(DECIMAL '-3.99', 0)", createDecimalType(4, 2), SqlDecimal.of("-4.00"));
        assertFunction("round(DECIMAL '9.9', 0)", createDecimalType(3, 1), SqlDecimal.of("10.0"));
        assertFunction("round(DECIMAL '-9.9', 0)", createDecimalType(3, 1), SqlDecimal.of("-10.0"));
        assertFunction("round(DECIMAL '123456789012345678', 0)", createDecimalType(19, 0), SqlDecimal.of("123456789012345678"));
        assertFunction("round(DECIMAL '-123456789012345678', 0)", createDecimalType(19, 0), SqlDecimal.of("-123456789012345678"));
        assertFunction("round(DECIMAL '123456789012345678.0', 0)", createDecimalType(20, 1), SqlDecimal.of("123456789012345678.0"));
        assertFunction("round(DECIMAL '-123456789012345678.0', 0)", createDecimalType(20, 1), SqlDecimal.of("-123456789012345678.0"));
        assertFunction("round(DECIMAL '123456789012345678.49', 0)", createDecimalType(21, 2), SqlDecimal.of("123456789012345678.00"));
        assertFunction("round(DECIMAL '-123456789012345678.49', 0)", createDecimalType(21, 2), SqlDecimal.of("-123456789012345678.00"));
        assertFunction("round(DECIMAL '123456789012345678.5', 0)", createDecimalType(20, 1), SqlDecimal.of("123456789012345679.0"));
        assertFunction("round(DECIMAL '-123456789012345678.5', 0)", createDecimalType(20, 1), SqlDecimal.of("-123456789012345679.0"));
        assertFunction("round(DECIMAL '123456789012345678.501', 0)", createDecimalType(22, 3), SqlDecimal.of("123456789012345679.000"));
        assertFunction("round(DECIMAL '-123456789012345678.501', 0)", createDecimalType(22, 3), SqlDecimal.of("-123456789012345679.000"));
        assertFunction("round(DECIMAL '999999999999999999.9', 0)", createDecimalType(20, 1), SqlDecimal.of("1000000000000000000.0"));
        assertFunction("round(DECIMAL '-999999999999999999.9', 0)", createDecimalType(20, 1), SqlDecimal.of("-1000000000000000000.0"));
        assertFunction("round(CAST(NULL as DECIMAL(1,0)), 0)", createDecimalType(2, 0), null);

        assertFunction("round( 3, 1)", BIGINT, 3);
        assertFunction("round(-3, 1)", BIGINT, -3);
        assertFunction("round(CAST(NULL as BIGINT), CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("round(-3, CAST(NULL as BIGINT))", BIGINT, null);
        assertFunction("round(CAST(NULL as BIGINT), 1)", BIGINT, null);
        assertFunction("round(DOUBLE '3.0', 1)", DOUBLE, 3.0);
        assertFunction("round(DOUBLE '-3.0', 1)", DOUBLE, -3.0);
        assertFunction("round(DOUBLE '3.499', 1)", DOUBLE, 3.5);
        assertFunction("round(DOUBLE '-3.499', 1)", DOUBLE, -3.5);
        assertFunction("round(DOUBLE '3.5', 1)", DOUBLE, 3.5);
        assertFunction("round(DOUBLE '-3.5', 1)", DOUBLE, -3.5);
        assertFunction("round(DOUBLE '-3.5001', 1)", DOUBLE, -3.5);
        assertFunction("round(DOUBLE '-3.99', 1)", DOUBLE, -4.0);
        assertFunction("round(CAST(NULL as DOUBLE), CAST(NULL as BIGINT))", DOUBLE, null);
        assertFunction("round(DOUBLE '-3.0', CAST(NULL as BIGINT))", DOUBLE, null);
        assertFunction("round(CAST(NULL as DOUBLE), 1)", DOUBLE, null);
        assertFunction("round(DECIMAL '3', 1)", createDecimalType(2, 0), SqlDecimal.of("3"));
        assertFunction("round(DECIMAL '-3', 1)", createDecimalType(2, 0), SqlDecimal.of("-3"));
        assertFunction("round(DECIMAL '3.0', 1)", createDecimalType(3, 1), SqlDecimal.of("3.0"));
        assertFunction("round(DECIMAL '-3.0', 1)", createDecimalType(3, 1), SqlDecimal.of("-3.0"));
        assertFunction("round(DECIMAL '3.449', 1)", createDecimalType(5, 3), SqlDecimal.of("3.400"));
        assertFunction("round(DECIMAL '-3.449', 1)", createDecimalType(5, 3), SqlDecimal.of("-3.400"));
        assertFunction("round(DECIMAL '3.450', 1)", createDecimalType(5, 3), SqlDecimal.of("3.500"));
        assertFunction("round(DECIMAL '-3.450', 1)", createDecimalType(5, 3), SqlDecimal.of("-3.500"));
        assertFunction("round(DECIMAL '3.99', 1)", createDecimalType(4, 2), SqlDecimal.of("4.00"));
        assertFunction("round(DECIMAL '-3.99', 1)", createDecimalType(4, 2), SqlDecimal.of("-4.00"));
        assertFunction("round(DECIMAL '9.99', 1)", createDecimalType(4, 2), SqlDecimal.of("10.00"));
        assertFunction("round(DECIMAL '-9.99', 1)", createDecimalType(4, 2), SqlDecimal.of("-10.00"));
        assertFunction("round(DECIMAL '123456789012345678', 1)", createDecimalType(19, 0), SqlDecimal.of("123456789012345678"));
        assertFunction("round(DECIMAL '-123456789012345678', 1)", createDecimalType(19, 0), SqlDecimal.of("-123456789012345678"));
        assertFunction("round(DECIMAL '123456789012345678.0', 1)", createDecimalType(20, 1), SqlDecimal.of("123456789012345678.0"));
        assertFunction("round(DECIMAL '-123456789012345678.0', 1)", createDecimalType(20, 1), SqlDecimal.of("-123456789012345678.0"));
        assertFunction("round(DECIMAL '123456789012345678.449', 1)", createDecimalType(22, 3), SqlDecimal.of("123456789012345678.400"));
        assertFunction("round(DECIMAL '-123456789012345678.449', 1)", createDecimalType(22, 3), SqlDecimal.of("-123456789012345678.400"));
        assertFunction("round(DECIMAL '123456789012345678.45', 1)", createDecimalType(21, 2), SqlDecimal.of("123456789012345678.50"));
        assertFunction("round(DECIMAL '-123456789012345678.45', 1)", createDecimalType(21, 2), SqlDecimal.of("-123456789012345678.50"));
        assertFunction("round(DECIMAL '123456789012345678.501', 1)", createDecimalType(22, 3), SqlDecimal.of("123456789012345678.500"));
        assertFunction("round(DECIMAL '-123456789012345678.501', 1)", createDecimalType(22, 3), SqlDecimal.of("-123456789012345678.500"));
        assertFunction("round(DECIMAL '999999999999999999.99', 1)", createDecimalType(21, 2), SqlDecimal.of("1000000000000000000.00"));
        assertFunction("round(DECIMAL '-999999999999999999.99', 1)", createDecimalType(21, 2), SqlDecimal.of("-1000000000000000000.00"));
        assertFunction("round(DECIMAL '0.00', 1)", createDecimalType(3, 2), SqlDecimal.of("0.00"));
        assertFunction("round(DECIMAL '000000000000000000.00', 1)", createDecimalType(3, 2), SqlDecimal.of("0.00"));

        assertFunction("round(CAST(NULL as DECIMAL(1,0)), CAST(NULL as BIGINT))", createDecimalType(2, 0), null);
        assertFunction("round(DECIMAL '-3.0', CAST(NULL as BIGINT))", createDecimalType(3, 1), null);
        assertFunction("round(CAST(NULL as DECIMAL(1,0)), 1)", createDecimalType(2, 0), null);

        assertFunction("round(DECIMAL '1234', 7)", createDecimalType(5, 0), SqlDecimal.of("1234"));
        assertFunction("round(DECIMAL '-1234', 7)", createDecimalType(5, 0), SqlDecimal.of("-1234"));
        assertFunction("round(DECIMAL '1234', -7)", createDecimalType(5, 0), SqlDecimal.of("0"));
        assertFunction("round(DECIMAL '-1234', -7)", createDecimalType(5, 0), SqlDecimal.of("0"));
        assertFunction("round(DECIMAL '1234.5678', 7)", createDecimalType(9, 4), SqlDecimal.of("1234.5678"));
        assertFunction("round(DECIMAL '-1234.5678', 7)", createDecimalType(9, 4), SqlDecimal.of("-1234.5678"));
        assertFunction("round(DECIMAL '1234.5678', -2)", createDecimalType(9, 4), SqlDecimal.of("1200.0000"));
        assertFunction("round(DECIMAL '-1234.5678', -2)", createDecimalType(9, 4), SqlDecimal.of("-1200.0000"));
        assertFunction("round(DECIMAL '1254.5678', -2)", createDecimalType(9, 4), SqlDecimal.of("1300.0000"));
        assertFunction("round(DECIMAL '-1254.5678', -2)", createDecimalType(9, 4), SqlDecimal.of("-1300.0000"));
        assertFunction("round(DECIMAL '1234.5678', -7)", createDecimalType(9, 4), SqlDecimal.of("0.0000"));
        assertFunction("round(DECIMAL '-1234.5678', -7)", createDecimalType(9, 4), SqlDecimal.of("0.0000"));
        assertFunction("round(DECIMAL '123456789012345678', 7)", createDecimalType(19, 0), SqlDecimal.of("123456789012345678"));
        assertFunction("round(DECIMAL '-123456789012345678', 7)", createDecimalType(19, 0), SqlDecimal.of("-123456789012345678"));
        assertFunction("round(DECIMAL '123456789012345678.99', 7)", createDecimalType(21, 2), SqlDecimal.of("123456789012345678.99"));
        assertFunction("round(DECIMAL '-123456789012345678.99', 7)", createDecimalType(21, 2), SqlDecimal.of("-123456789012345678.99"));
        assertFunction("round(DECIMAL '123456789012345611.99', -2)", createDecimalType(21, 2), SqlDecimal.of("123456789012345600.00"));
        assertFunction("round(DECIMAL '-123456789012345611.99', -2)", createDecimalType(21, 2), SqlDecimal.of("-123456789012345600.00"));
        assertFunction("round(DECIMAL '123456789012345678.99', -2)", createDecimalType(21, 2), SqlDecimal.of("123456789012345700.00"));
        assertFunction("round(DECIMAL '-123456789012345678.99', -2)", createDecimalType(21, 2), SqlDecimal.of("-123456789012345700.00"));
        assertFunction("round(DECIMAL '123456789012345678.99', -30)", createDecimalType(21, 2), SqlDecimal.of("0.00"));
        assertFunction("round(DECIMAL '-123456789012345678.99', -30)", createDecimalType(21, 2), SqlDecimal.of("0.00"));
        assertFunction("round(DECIMAL '99', -1)", createDecimalType(3, 0), SqlDecimal.of("100"));

        assertInvalidFunction("round(DECIMAL '9999999999999999999999999999999999999.9', 0)", NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testSin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sin(" + doubleValue + ")", DOUBLE, Math.sin(doubleValue));
        }
        assertFunction("sin(NULL)", DOUBLE, null);
    }

    @Test
    public void testSqrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sqrt(" + doubleValue + ")", DOUBLE, Math.sqrt(doubleValue));
        }
        assertFunction("sqrt(NULL)", DOUBLE, null);
    }

    @Test
    public void testTan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tan(" + doubleValue + ")", DOUBLE, Math.tan(doubleValue));
        }
        assertFunction("tan(NULL)", DOUBLE, null);
    }

    @Test
    public void testTanh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tanh(" + doubleValue + ")", DOUBLE, Math.tanh(doubleValue));
        }
        assertFunction("tanh(NULL)", DOUBLE, null);
    }

    @Test
    public void testGreatest()
            throws Exception
    {
        // bigint
        assertFunction("greatest(1, 2)", BIGINT, 2);
        assertFunction("greatest(-1, -2)", BIGINT, -1);
        assertFunction("greatest(5, 4, 3, 2, 1, 2, 3, 4, 1, 5)", BIGINT, 5);
        assertFunction("greatest(-1)", BIGINT, -1);
        assertFunction("greatest(5, 4, CAST(NULL as BIGINT), 3)", BIGINT, null);

        // double
        assertFunction("greatest(CAST(CAST(1.5 as DOUBLE) as DOUBLE), CAST(2.3 as DOUBLE))", DOUBLE, 2.3);
        assertFunction("greatest(-CAST(1.5 as DOUBLE), -CAST(2.3 as DOUBLE))", DOUBLE, -1.5);
        assertFunction("greatest(-CAST(1.5 as DOUBLE), -CAST(2.3 as DOUBLE), -5/3)", DOUBLE, -1.0);
        assertFunction("greatest(CAST(1.5 as DOUBLE), -CAST(1.0 as DOUBLE) / CAST(0.0 as DOUBLE), CAST(1.0 as DOUBLE) / CAST(0.0 as DOUBLE))", DOUBLE, Double.POSITIVE_INFINITY);
        assertFunction("greatest(5, 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);

        // decimal
        assertDecimalFunction("greatest(1.0, 2.0)", decimal("2.0"));
        assertDecimalFunction("greatest(1.0, -2.0)", decimal("1.0"));
        assertDecimalFunction("greatest(1.0, 1.1, 1.2, 1.3)", decimal("1.3"));

        // mixed
        assertFunction("greatest(1, CAST(2.0 as DOUBLE))", DOUBLE, 2.0);
        assertFunction("greatest(CAST(1.0 as DOUBLE), 2)", DOUBLE, 2.0);
        assertFunction("greatest(CAST(5.0 as DOUBLE), 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);
        assertFunction("greatest(CAST(5.0 as DOUBLE), 4, CAST(NULL as BIGINT), 3)", DOUBLE, null);
        assertFunction("greatest(1.0, CAST(2.0 as DOUBLE))", DOUBLE, 2.0);
        assertDecimalFunction("greatest(5, 4, 3.0, 2)", decimal("0000000000000000005.0"));

        // invalid
        assertInvalidFunction("greatest(CAST(1.5 as DOUBLE), CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE))", "Invalid argument to greatest(): NaN");
    }

    @Test
    public void testLeast()
            throws Exception
    {
        // bigint
        assertFunction("least(1, 2)", BIGINT, 1);
        assertFunction("least(-1, -2)", BIGINT, -2);
        assertFunction("least(5, 4, 3, 2, 1, 2, 3, 4, 1, 5)", BIGINT, 1);
        assertFunction("least(-1)", BIGINT, -1);
        assertFunction("least(5, 4, CAST(NULL as BIGINT), 3)", BIGINT, null);

        // double
        assertFunction("least(CAST(1.5 as DOUBLE), CAST(2.3 as DOUBLE))", DOUBLE, 1.5);
        assertFunction("least(-CAST(1.5 as DOUBLE), -CAST(2.3 as DOUBLE))", DOUBLE, -2.3);
        assertFunction("least(-CAST(1.5 as DOUBLE), -CAST(2.3 as DOUBLE), -5/3)", DOUBLE, -2.3);
        assertFunction("least(CAST(1.5 as DOUBLE), -CAST(1.0 as DOUBLE) / CAST(0.0 as DOUBLE), CAST(1.0 as DOUBLE) / CAST(0.0 as DOUBLE))", DOUBLE, Double.NEGATIVE_INFINITY);
        assertFunction("least(5, 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);

        // decimal
        assertDecimalFunction("least(1.0, 2.0)", decimal("1.0"));
        assertDecimalFunction("least(1.0, -2.0)", decimal("-2.0"));
        assertDecimalFunction("least(1.0, 1.1, 1.2, 1.3)", decimal("1.0"));

        // mixed
        assertFunction("least(1, CAST(2.0 as DOUBLE))", DOUBLE, 1.0);
        assertFunction("least(CAST(1.0 as DOUBLE), 2)", DOUBLE, 1.0);
        assertFunction("least(CAST(5.0 as DOUBLE), 4, CAST(NULL as DOUBLE), 3)", DOUBLE, null);
        assertFunction("least(CAST(5.0 as DOUBLE), 4, CAST(NULL as BIGINT), 3)", DOUBLE, null);
        assertFunction("least(1.0, CAST(2.0 as DOUBLE))", DOUBLE, 1.0);
        assertDecimalFunction("least(5, 4, 3.0, 2)", decimal("0000000000000000002.0"));

        // invalid
        assertInvalidFunction("least(CAST(1.5 as DOUBLE), CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE))", "Invalid argument to least(): NaN");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "\\QInvalid argument to greatest(): NaN\\E")
    public void testGreatestWithNaN()
            throws Exception
    {
        functionAssertions.tryEvaluate("greatest(CAST(1.5 as DOUBLE), CAST(0.0 as DOUBLE) / CAST(0.0 as DOUBLE))", DOUBLE);
    }

    @Test
    public void testToBase()
            throws Exception
    {
        assertFunction("to_base(2147483648, 16)", VARCHAR, "80000000");
        assertFunction("to_base(255, 2)", VARCHAR, "11111111");
        assertFunction("to_base(-2147483647, 16)", VARCHAR, "-7fffffff");
        assertFunction("to_base(NULL, 16)", VARCHAR, null);
        assertFunction("to_base(-2147483647, NULL)", VARCHAR, null);
        assertFunction("to_base(NULL, NULL)", VARCHAR, null);
        assertInvalidFunction("to_base(255, 1)", VARCHAR, "Radix must be between 2 and 36");
        assertInvalidFunction("to_base(255, 1)", "Radix must be between 2 and 36");
    }

    @Test
    public void testFromBase()
            throws Exception
    {
        assertFunction("from_base('80000000', 16)", BIGINT, 2147483648L);
        assertFunction("from_base('11111111', 2)", BIGINT, 255);
        assertFunction("from_base('-7fffffff', 16)", BIGINT, -2147483647);
        assertFunction("from_base('9223372036854775807', 10)", BIGINT, 9223372036854775807L);
        assertFunction("from_base('-9223372036854775808', 10)", BIGINT, -9223372036854775808L);
        assertFunction("from_base(NULL, 10)", BIGINT, null);
        assertFunction("from_base('-9223372036854775808', NULL)", BIGINT, null);
        assertFunction("from_base(NULL, NULL)", BIGINT, null);
        assertInvalidFunction("from_base('Z', 37)", "Radix must be between 2 and 36");
        assertInvalidFunction("from_base('Z', 35)", "Not a valid base-35 number: Z");
        assertInvalidFunction("from_base('9223372036854775808', 10)", "Not a valid base-10 number: 9223372036854775808");
        assertInvalidFunction("from_base('Z', 37)", BIGINT, "Radix must be between 2 and 36");
        assertInvalidFunction("from_base('Z', 35)", BIGINT, "Not a valid base-35 number: Z");
        assertInvalidFunction("from_base('9223372036854775808', 10)", BIGINT, "Not a valid base-10 number: 9223372036854775808");
    }
}
