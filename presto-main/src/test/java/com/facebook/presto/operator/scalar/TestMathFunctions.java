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

import static com.facebook.presto.operator.scalar.FunctionAssertions.SESSION;
import static com.facebook.presto.operator.scalar.FunctionAssertions.assertFunction;
import static com.facebook.presto.operator.scalar.FunctionAssertions.executeProjectionWithAll;

public class TestMathFunctions
{
    private static final double[] DOUBLE_VALUES = {123, -123, 123.45, -123.45};
    private static final long[] longLefts = {9, 10, 11, -9, -10, -11};
    private static final long[] longRights = {3, -3};
    private static final double[] doubleLefts = {9, 10, 11, -9, -10, -11, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1};
    private static final double[] doubleRights = {3, -3, 3.1, -3.1};

    @Test
    public void testAbs()
    {
        assertFunction("abs(123)", 123L);
        assertFunction("abs(-123)", 123L);
        assertFunction("abs(123.0)", 123.0);
        assertFunction("abs(-123.0)", 123.0);
        assertFunction("abs(123.45)", 123.45);
        assertFunction("abs(-123.45)", 123.45);
    }

    @Test
    public void testAcos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("acos(" + doubleValue + ")", Math.acos(doubleValue));
        }
    }

    @Test
    public void testAsin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("asin(" + doubleValue + ")", Math.asin(doubleValue));
        }
    }

    @Test
    public void testAtan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan(" + doubleValue + ")", Math.atan(doubleValue));
        }
    }

    @Test
    public void testAtan2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("atan2(" + doubleValue + ", " + doubleValue + ")", Math.atan2(doubleValue, doubleValue));
        }
    }

    @Test
    public void testCbrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cbrt(" + doubleValue + ")", Math.cbrt(doubleValue));
        }
    }

    @Test
    public void testCeil()
    {
        assertFunction("ceil(123)", 123);
        assertFunction("ceil(-123)", -123);
        assertFunction("ceil(123.0)", 123.0);
        assertFunction("ceil(-123.0)", -123.0);
        assertFunction("ceil(123.45)", 124.0);
        assertFunction("ceil(-123.45)", -123.0);
        assertFunction("ceiling(123)", 123);
        assertFunction("ceiling(-123)", -123);
        assertFunction("ceiling(123.0)", 123.0);
        assertFunction("ceiling(-123.0)", -123.0);
        assertFunction("ceiling(123.45)", 124.0);
        assertFunction("ceiling(-123.45)", -123.0);
    }

    @Test
    public void testCos()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cos(" + doubleValue + ")", Math.cos(doubleValue));
        }
    }

    @Test
    public void testCosh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("cosh(" + doubleValue + ")", Math.cosh(doubleValue));
        }
    }

    @Test
    public void testE()
    {
        assertFunction("e()", Math.E);
    }

    @Test
    public void testExp()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("exp(" + doubleValue + ")", Math.exp(doubleValue));
        }
    }

    @Test
    public void testFloor()
    {
        assertFunction("floor(123)", 123);
        assertFunction("floor(-123)", -123);
        assertFunction("floor(123.0)", 123.0);
        assertFunction("floor(-123.0)", -123.0);
        assertFunction("floor(123.45)", 123.0);
        assertFunction("floor(-123.45)", -124.0);
    }

    @Test
    public void testLn()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("ln(" + doubleValue + ")", Math.log(doubleValue));
        }
    }

    @Test
    public void testLog2()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log2(" + doubleValue + ")", Math.log(doubleValue) / Math.log(2));
        }
    }

    @Test
    public void testLog10()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("log10(" + doubleValue + ")", Math.log10(doubleValue));
        }
    }

    @Test
    public void testLog()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            for (double base : DOUBLE_VALUES) {
                assertFunction("log(" + doubleValue + ", " + base + ")", Math.log(doubleValue) / Math.log(base));
            }
        }
    }

    @Test
    public void testMod()
    {
        for (long left : longLefts) {
            for (long right : longRights) {
                assertFunction("mod(" + left + ", " + right + ")", left % right);
            }
        }

        for (long left : longLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(" + left + ", " + right + ")", left % right);
            }
        }

        for (double left : doubleLefts) {
            for (long right : longRights) {
                assertFunction("mod(" + left + ", " + right + ")", left % right);
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("mod(" + left + ", " + right + ")", left % right);
            }
        }
    }

    @Test
    public void testPi()
    {
        assertFunction("pi()", Math.PI);
    }

    @Test
    public void testNaN()
    {
        assertFunction("nan()", Double.NaN);
        assertFunction("0.0 / 0.0", Double.NaN);
    }

    @Test
    public void testInfinity()
    {
        assertFunction("infinity()", Double.POSITIVE_INFINITY);
        assertFunction("-rand() / 0.0", Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testIsInfinite()
    {
        assertFunction("is_infinite(1.0 / 0.0)", true);
        assertFunction("is_infinite(0.0 / 0.0)", false);
        assertFunction("is_infinite(1.0 / 1.0)", false);
    }

    @Test
    public void testIsFinite()
    {
        assertFunction("is_finite(100000)", true);
        assertFunction("is_finite(rand() / 0.0)", false);
    }

    @Test
    public void testIsNaN()
    {
        assertFunction("is_nan(0.0 / 0.0)", true);
        assertFunction("is_nan(0.0 / 1.0)", false);
        assertFunction("is_nan(infinity() / infinity())", true);
        assertFunction("is_nan(nan())", true);
    }

    @Test
    public void testPow()
    {
        for (long left : longLefts) {
            for (long right : longRights) {
                assertFunction("pow(" + left + ", " + right + ")", Math.pow(left, right));
            }
        }

        for (long left : longLefts) {
            for (double right : doubleRights) {
                assertFunction("pow(" + left + ", " + right + ")", Math.pow(left, right));
            }
        }

        for (double left : doubleLefts) {
            for (long right : longRights) {
                assertFunction("pow(" + left + ", " + right + ")", Math.pow(left, right));
            }
        }

        for (double left : doubleLefts) {
            for (double right : doubleRights) {
                assertFunction("pow(" + left + ", " + right + ")", Math.pow(left, right));
            }
        }
    }

    @Test
    public void testRandom()
    {
        // random is non-deterministic
        executeProjectionWithAll("rand()", SESSION);
        executeProjectionWithAll("random()", SESSION);
    }

    @Test
    public void testRound()
    {
        assertFunction("round( 3)", 3);
        assertFunction("round(-3)", -3);
        assertFunction("round( 3.0)", 3.0);
        assertFunction("round(-3.0)", -3.0);
        assertFunction("round( 3.499)", 3.0);
        assertFunction("round(-3.499)", -3.0);
        assertFunction("round( 3.5)", 4.0);
        assertFunction("round(-3.5)", -4.0);
        assertFunction("round(-3.5001)", -4.0);
        assertFunction("round(-3.99)", -4.0);

        assertFunction("round( 3, 0)", 3);
        assertFunction("round(-3, 0)", -3);
        assertFunction("round( 3.0, 0)", 3.0);
        assertFunction("round(-3.0, 0)", -3.0);
        assertFunction("round( 3.499, 0)", 3.0);
        assertFunction("round(-3.499, 0)", -3.0);
        assertFunction("round( 3.5, 0)", 4.0);
        assertFunction("round(-3.5, 0)", -4.0);
        assertFunction("round(-3.5001, 0)", -4.0);
        assertFunction("round(-3.99, 0)", -4.0);

        assertFunction("round( 3, 1)", 3);
        assertFunction("round(-3, 1)", -3);
        assertFunction("round( 3.0, 1)", 3.0);
        assertFunction("round(-3.0, 1)", -3.0);
        assertFunction("round( 3.499, 1)", 3.5);
        assertFunction("round(-3.499, 1)", -3.5);
        assertFunction("round( 3.5, 1)", 3.5);
        assertFunction("round(-3.5, 1)", -3.5);
        assertFunction("round(-3.5001, 1)", -3.5);
        assertFunction("round(-3.99, 1)", -4.0);
    }

    @Test
    public void testSin()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sin(" + doubleValue + ")", Math.sin(doubleValue));
        }
    }

    @Test
    public void testSqrt()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("sqrt(" + doubleValue + ")", Math.sqrt(doubleValue));
        }
    }

    @Test
    public void testTan()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tan(" + doubleValue + ")", Math.tan(doubleValue));
        }
    }

    @Test
    public void testTanh()
    {
        for (double doubleValue : DOUBLE_VALUES) {
            assertFunction("tanh(" + doubleValue + ")", Math.tanh(doubleValue));
        }
    }
}
