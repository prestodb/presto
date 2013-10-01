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

import com.facebook.presto.operator.Description;
import com.google.common.primitives.Doubles;

import java.util.concurrent.ThreadLocalRandom;

public final class MathFunctions
{
    private MathFunctions() {}

    @Description("absolute value")
    @ScalarFunction
    public static long abs(long num)
    {
        return Math.abs(num);
    }

    @Description("absolute value")
    @ScalarFunction
    public static double abs(double num)
    {
        return Math.abs(num);
    }

    @Description("arc cosine")
    @ScalarFunction
    public static double acos(double num)
    {
        return Math.acos(num);
    }

    @Description("arc sine")
    @ScalarFunction
    public static double asin(double num)
    {
        return Math.asin(num);
    }

    @Description("arc tangent")
    @ScalarFunction
    public static double atan(double num)
    {
        return Math.atan(num);
    }

    @Description("arc tangent of given fraction")
    @ScalarFunction
    public static double atan2(double num1, double num2)
    {
        return Math.atan2(num1, num2);
    }

    @Description("cube root")
    @ScalarFunction
    public static double cbrt(double num)
    {
        return Math.cbrt(num);
    }

    @Description("round up to nearest integer")
    @ScalarFunction(alias = "ceil")
    public static long ceiling(long num)
    {
        return num;
    }

    @Description("round up to nearest integer")
    @ScalarFunction(alias = "ceil")
    public static double ceiling(double num)
    {
        return Math.ceil(num);
    }

    @Description("cosine")
    @ScalarFunction
    public static double cos(double num)
    {
        return Math.cos(num);
    }

    @Description("hyperbolic cosine")
    @ScalarFunction
    public static double cosh(double num)
    {
        return Math.cosh(num);
    }

    @Description("Euler's number")
    @ScalarFunction
    public static double e()
    {
        return Math.E;
    }

    @Description("Euler's number raised to the given power")
    @ScalarFunction
    public static double exp(double num)
    {
        return Math.exp(num);
    }

    @Description("round down to nearest integer")
    @ScalarFunction
    public static long floor(long num)
    {
        return num;
    }

    @Description("round down to nearest integer")
    @ScalarFunction
    public static double floor(double num)
    {
        return Math.floor(num);
    }

    @Description("natural logarithm")
    @ScalarFunction
    public static double ln(double num)
    {
        return Math.log(num);
    }

    @Description("logarithm to base 2")
    @ScalarFunction
    public static double log2(double num)
    {
        return Math.log(num) / Math.log(2);
    }

    @Description("logarithm to base 10")
    @ScalarFunction
    public static double log10(double num)
    {
        return Math.log10(num);
    }

    @Description("logarithm to given base")
    @ScalarFunction
    public static double log(double num, double base)
    {
        return Math.log(num) / Math.log(base);
    }

    @Description("remainder of given quotient")
    @ScalarFunction
    public static long mod(long num1, long num2)
    {
        return num1 % num2;
    }

    @Description("remainder of given quotient")
    @ScalarFunction
    public static double mod(double num1, double num2)
    {
        return num1 % num2;
    }

    @Description("the constant Pi")
    @ScalarFunction
    public static double pi()
    {
        return Math.PI;
    }

    @Description("value raised to the power of exponent")
    @ScalarFunction
    public static double pow(double num, double exponent)
    {
        return Math.pow(num, exponent);
    }

    @Description("a pseudo-random value")
    @ScalarFunction(alias = "rand", deterministic = false)
    public static double random()
    {
        return ThreadLocalRandom.current().nextDouble();
    }

    @Description("round to nearest integer")
    @ScalarFunction
    public static long round(long num)
    {
        return round(num, 0);
    }

    @Description("round to nearest integer")
    @ScalarFunction
    public static long round(long num, long decimals)
    {
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction
    public static double round(double num)
    {
        return round(num, 0);
    }

    @Description("round to given number of decimal places")
    @ScalarFunction
    public static double round(double num, long decimals)
    {
        if (num == 0.0) {
            return 0;
        }
        if (num < 0) {
            return -round(-num, decimals);
        }

        double factor = Math.pow(10, decimals);
        return Math.floor(num * factor + 0.5) / factor;
    }

    @Description("sine")
    @ScalarFunction
    public static double sin(double num)
    {
        return Math.sin(num);
    }

    @Description("square root")
    @ScalarFunction
    public static double sqrt(double num)
    {
        return Math.sqrt(num);
    }

    @Description("tangent")
    @ScalarFunction
    public static double tan(double num)
    {
        return Math.tan(num);
    }

    @Description("hyperbolic tangent")
    @ScalarFunction
    public static double tanh(double num)
    {
        return Math.tanh(num);
    }

    @Description("test if value is not-a-number")
    @ScalarFunction("is_nan")
    public static boolean isNaN(double num)
    {
        return Double.isNaN(num);
    }

    @Description("test if value is finite")
    @ScalarFunction
    public static boolean isFinite(double num)
    {
        return Doubles.isFinite(num);
    }

    @Description("test if value is infinite")
    @ScalarFunction
    public static boolean isInfinite(double num)
    {
        return Double.isInfinite(num);
    }

    @Description("constant representing not-a-number")
    @ScalarFunction("nan")
    public static double NaN()
    {
        return Double.NaN;
    }

    @Description("Infinity")
    @ScalarFunction
    public static double infinity()
    {
        return Double.POSITIVE_INFINITY;
    }
}
