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

import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
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
        assertFunction("abs(123.0)", DOUBLE, 123.0);
        assertFunction("abs(-123.0)", DOUBLE, 123.0);
        assertFunction("abs(123.45)", DOUBLE, 123.45);
        assertFunction("abs(-123.45)", DOUBLE, 123.45);
    }

    @Test
    public void testAcos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("acos(" + doubleValue + ")", DOUBLE, Math.acos(doubleValue));
        }
    }

    @Test
    public void testAsin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("asin(" + doubleValue + ")", DOUBLE, Math.asin(doubleValue));
        }
    }

    @Test
    public void testAtan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan(" + doubleValue + ")", DOUBLE, Math.atan(doubleValue));
        }
    }

    @Test
    public void testAtan2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan2(" + doubleValue + ", " + doubleValue + ")", DOUBLE, Math.atan2(doubleValue, doubleValue));
        }
    }

    @Test
    public void testCbrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cbrt(" + doubleValue + ")", DOUBLE, Math.cbrt(doubleValue));
        }
    }

    @Test
    public void testCeil()
    {
        assertFunction("ceil(123)", BIGINT, 123);
        assertFunction("ceil(-123)", BIGINT, -123);
        assertFunction("ceil(123.0)", DOUBLE, 123.0);
        assertFunction("ceil(-123.0)", DOUBLE, -123.0);
        assertFunction("ceil(123.45)", DOUBLE, 124.0);
        assertFunction("ceil(-123.45)", DOUBLE, -123.0);
        assertFunction("ceiling(123)", BIGINT, 123);
        assertFunction("ceiling(-123)", BIGINT, -123);
        assertFunction("ceiling(123.0)", DOUBLE, 123.0);
        assertFunction("ceiling(-123.0)", DOUBLE, -123.0);
        assertFunction("ceiling(123.45)", DOUBLE, 124.0);
        assertFunction("ceiling(-123.45)", DOUBLE, -123.0);
    }

    @Test
    public void testCos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cos(" + doubleValue + ")", DOUBLE, Math.cos(doubleValue));
        }
    }

    @Test
    public void testCosh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cosh(" + doubleValue + ")", DOUBLE, Math.cosh(doubleValue));
        }
    }

    @Test
    public void testDegrees()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction(String.format("degrees(%s)", doubleValue), DOUBLE, Math.toDegrees(doubleValue));
        }
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
    }

    @Test
    public void testFloor()
    {
        assertFunction("floor(123)", BIGINT, 123);
        assertFunction("floor(-123)", BIGINT, -123);
        assertFunction("floor(123.0)", DOUBLE, 123.0);
        assertFunction("floor(-123.0)", DOUBLE, -123.0);
        assertFunction("floor(123.45)", DOUBLE, 123.0);
        assertFunction("floor(-123.45)", DOUBLE, -124.0);
    }

    @Test
    public void testLn()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("ln(" + doubleValue + ")", DOUBLE, Math.log(doubleValue));
        }
    }

    @Test
    public void testLog2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log2(" + doubleValue + ")", DOUBLE, Math.log(doubleValue) / Math.log(2));
        }
    }

    @Test
    public void testLog10()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log10(" + doubleValue + ")", DOUBLE, Math.log10(doubleValue));
        }
    }

    @Test
    public void testLog()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            for (double base : DOUBLE_VALUES) {
                assertFunction("log(" + doubleValue + ", " + base + ")", DOUBLE, Math.log(doubleValue) / Math.log(base));
            }
        }
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
                assertFunction("mod(" + left + ", " + right + ")", DOUBLE, left % right);
            }
        }

        for (double left : doubleLefts) {
            for (long right : longRights) {
                assertFunction("mod(" + left + ", " + right + ")", DOUBLE, left % right);
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(" + left + ", " + right + ")", DOUBLE, left % right);
            }
        }
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
        assertFunction("0.0 / 0.0", DOUBLE, Double.NaN);
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
        assertFunction("is_infinite(1.0 / 0.0)", BOOLEAN, true);
        assertFunction("is_infinite(0.0 / 0.0)", BOOLEAN, false);
        assertFunction("is_infinite(1.0 / 1.0)", BOOLEAN, false);
    }

    @Test
    public void testIsFinite()
    {
        assertFunction("is_finite(100000)", BOOLEAN, true);
        assertFunction("is_finite(rand() / 0.0)", BOOLEAN, false);
    }

    @Test
    public void testIsNaN()
    {
        assertFunction("is_nan(0.0 / 0.0)", BOOLEAN, true);
        assertFunction("is_nan(0.0 / 1.0)", BOOLEAN, false);
        assertFunction("is_nan(infinity() / infinity())", BOOLEAN, true);
        assertFunction("is_nan(nan())", BOOLEAN, true);
    }

    @Test
    public void testPow()
    {
        for (long left : longLefts) {
            for (long right : longRights) {
                assertFunction("pow(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }

        for (long left : longLefts) {
            for (double right : doubleRights) {
                assertFunction("pow(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }

        for (double left : doubleLefts) {
            for (long right : longRights) {
                assertFunction("pow(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("pow(" + left + ", " + right + ")", DOUBLE, Math.pow(left, right));
            }
        }
    }

    @Test
    public void testRadians()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction(String.format("radians(%s)", doubleValue), DOUBLE, Math.toRadians(doubleValue));
        }
    }

    @Test
    public void testRandom()
    {
        // random is non-deterministic
        functionAssertions.tryEvaluateWithAll("rand()", DOUBLE, TEST_SESSION);
        functionAssertions.tryEvaluateWithAll("random()", DOUBLE, TEST_SESSION);
    }

    @Test
    public void testRound()
    {
        assertFunction("round( 3)", BIGINT, 3);
        assertFunction("round(-3)", BIGINT, -3);
        assertFunction("round( 3.0)", DOUBLE, 3.0);
        assertFunction("round(-3.0)", DOUBLE, -3.0);
        assertFunction("round( 3.499)", DOUBLE, 3.0);
        assertFunction("round(-3.499)", DOUBLE, -3.0);
        assertFunction("round( 3.5)", DOUBLE, 4.0);
        assertFunction("round(-3.5)", DOUBLE, -4.0);
        assertFunction("round(-3.5001)", DOUBLE, -4.0);
        assertFunction("round(-3.99)", DOUBLE, -4.0);

        assertFunction("round( 3, 0)", BIGINT, 3);
        assertFunction("round(-3, 0)", BIGINT, -3);
        assertFunction("round( 3.0, 0)", DOUBLE, 3.0);
        assertFunction("round(-3.0, 0)", DOUBLE, -3.0);
        assertFunction("round( 3.499, 0)", DOUBLE, 3.0);
        assertFunction("round(-3.499, 0)", DOUBLE, -3.0);
        assertFunction("round( 3.5, 0)", DOUBLE, 4.0);
        assertFunction("round(-3.5, 0)", DOUBLE, -4.0);
        assertFunction("round(-3.5001, 0)", DOUBLE, -4.0);
        assertFunction("round(-3.99, 0)", DOUBLE, -4.0);

        assertFunction("round( 3, 1)", BIGINT, 3);
        assertFunction("round(-3, 1)", BIGINT, -3);
        assertFunction("round( 3.0, 1)", DOUBLE, 3.0);
        assertFunction("round(-3.0, 1)", DOUBLE, -3.0);
        assertFunction("round( 3.499, 1)", DOUBLE, 3.5);
        assertFunction("round(-3.499, 1)", DOUBLE, -3.5);
        assertFunction("round( 3.5, 1)", DOUBLE, 3.5);
        assertFunction("round(-3.5, 1)", DOUBLE, -3.5);
        assertFunction("round(-3.5001, 1)", DOUBLE, -3.5);
        assertFunction("round(-3.99, 1)", DOUBLE, -4.0);
    }

    @Test
    public void testSin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sin(" + doubleValue + ")", DOUBLE, Math.sin(doubleValue));
        }
    }

    @Test
    public void testSqrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sqrt(" + doubleValue + ")", DOUBLE, Math.sqrt(doubleValue));
        }
    }

    @Test
    public void testTan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tan(" + doubleValue + ")", DOUBLE, Math.tan(doubleValue));
        }
    }

    @Test
    public void testTanh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tanh(" + doubleValue + ")", DOUBLE, Math.tanh(doubleValue));
        }
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

        // double
        assertFunction("greatest(1.5, 2.3)", DOUBLE, 2.3);
        assertFunction("greatest(-1.5, -2.3)", DOUBLE, -1.5);
        assertFunction("greatest(-1.5, -2.3, -5/3)", DOUBLE, -1.0);
        assertFunction("greatest(1.5, -1.0 / 0.0, 1.0 / 0.0)", DOUBLE, Double.POSITIVE_INFINITY);

        // mixed
        assertFunction("greatest(1, 2.0)", DOUBLE, 2.0);
        assertFunction("greatest(1.0, 2)", DOUBLE, 2.0);

        // invalid
        assertInvalidFunction("greatest(1.5, 0.0 / 0.0)", "Invalid argument to greatest(): NaN");
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

        // double
        assertFunction("least(1.5, 2.3)", DOUBLE, 1.5);
        assertFunction("least(-1.5, -2.3)", DOUBLE, -2.3);
        assertFunction("least(-1.5, -2.3, -5/3)", DOUBLE, -2.3);
        assertFunction("least(1.5, -1.0 / 0.0, 1.0 / 0.0)", DOUBLE, Double.NEGATIVE_INFINITY);

        // mixed
        assertFunction("least(1, 2.0)", DOUBLE, 1.0);
        assertFunction("least(1.0, 2)", DOUBLE, 1.0);

        // invalid
        assertInvalidFunction("least(1.5, 0.0 / 0.0)", "Invalid argument to least(): NaN");
    }

    @Test
    public void testToBase()
            throws Exception
    {
        assertFunction("to_base(2147483648, 16)", VARCHAR, "80000000");
        assertFunction("to_base(255, 2)", VARCHAR, "11111111");
        assertFunction("to_base(-2147483647, 16)", VARCHAR, "-7fffffff");
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
        assertInvalidFunction("from_base('Z', 37)", "Radix must be between 2 and 36");
        assertInvalidFunction("from_base('Z', 35)", "Not a valid base-35 number: Z");
        assertInvalidFunction("from_base('9223372036854775808', 10)", "Not a valid base-10 number: 9223372036854775808");
    }
}
