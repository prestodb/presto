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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.type.SqlType;
import com.google.common.primitives.Doubles;

import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class MathFunctions
{
    private MathFunctions() {}

    @Description("absolute value")
    @ScalarFunction
    @SqlType(BigintType.NAME)
    public static long abs(@SqlType(BigintType.NAME) long num)
    {
        return Math.abs(num);
    }

    @Description("absolute value")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double abs(@SqlType(DoubleType.NAME) double num)
    {
        return Math.abs(num);
    }

    @Description("arc cosine")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double acos(@SqlType(DoubleType.NAME) double num)
    {
        return Math.acos(num);
    }

    @Description("arc sine")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double asin(@SqlType(DoubleType.NAME) double num)
    {
        return Math.asin(num);
    }

    @Description("arc tangent")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double atan(@SqlType(DoubleType.NAME) double num)
    {
        return Math.atan(num);
    }

    @Description("arc tangent of given fraction")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double atan2(@SqlType(DoubleType.NAME) double num1, @SqlType(DoubleType.NAME) double num2)
    {
        return Math.atan2(num1, num2);
    }

    @Description("cube root")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double cbrt(@SqlType(DoubleType.NAME) double num)
    {
        return Math.cbrt(num);
    }

    @Description("round up to nearest integer")
    @ScalarFunction(alias = "ceil")
    @SqlType(BigintType.NAME)
    public static long ceiling(@SqlType(BigintType.NAME) long num)
    {
        return num;
    }

    @Description("round up to nearest integer")
    @ScalarFunction(alias = "ceil")
    @SqlType(DoubleType.NAME)
    public static double ceiling(@SqlType(DoubleType.NAME) double num)
    {
        return Math.ceil(num);
    }

    @Description("cosine")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double cos(@SqlType(DoubleType.NAME) double num)
    {
        return Math.cos(num);
    }

    @Description("hyperbolic cosine")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double cosh(@SqlType(DoubleType.NAME) double num)
    {
        return Math.cosh(num);
    }

    @Description("Euler's number")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double e()
    {
        return Math.E;
    }

    @Description("Euler's number raised to the given power")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double exp(@SqlType(DoubleType.NAME) double num)
    {
        return Math.exp(num);
    }

    @Description("round down to nearest integer")
    @ScalarFunction
    @SqlType(BigintType.NAME)
    public static long floor(@SqlType(BigintType.NAME) long num)
    {
        return num;
    }

    @Description("round down to nearest integer")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double floor(@SqlType(DoubleType.NAME) double num)
    {
        return Math.floor(num);
    }

    @Description("natural logarithm")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double ln(@SqlType(DoubleType.NAME) double num)
    {
        return Math.log(num);
    }

    @Description("logarithm to base 2")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double log2(@SqlType(DoubleType.NAME) double num)
    {
        return Math.log(num) / Math.log(2);
    }

    @Description("logarithm to base 10")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double log10(@SqlType(DoubleType.NAME) double num)
    {
        return Math.log10(num);
    }

    @Description("logarithm to given base")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double log(@SqlType(DoubleType.NAME) double num, @SqlType(DoubleType.NAME) double base)
    {
        return Math.log(num) / Math.log(base);
    }

    @Description("remainder of given quotient")
    @ScalarFunction
    @SqlType(BigintType.NAME)
    public static long mod(@SqlType(BigintType.NAME) long num1, @SqlType(BigintType.NAME) long num2)
    {
        return num1 % num2;
    }

    @Description("remainder of given quotient")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double mod(@SqlType(DoubleType.NAME) double num1, @SqlType(DoubleType.NAME) double num2)
    {
        return num1 % num2;
    }

    @Description("the constant Pi")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double pi()
    {
        return Math.PI;
    }

    @Description("value raised to the power of exponent")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double pow(@SqlType(DoubleType.NAME) double num, @SqlType(DoubleType.NAME) double exponent)
    {
        return Math.pow(num, exponent);
    }

    @Description("a pseudo-random value")
    @ScalarFunction(alias = "rand", deterministic = false)
    @SqlType(DoubleType.NAME)
    public static double random()
    {
        return ThreadLocalRandom.current().nextDouble();
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(BigintType.NAME)
    public static long round(@SqlType(BigintType.NAME) long num)
    {
        return round(num, 0);
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(BigintType.NAME)
    public static long round(@SqlType(BigintType.NAME) long num, @SqlType(BigintType.NAME) long decimals)
    {
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double round(@SqlType(DoubleType.NAME) double num)
    {
        return round(num, 0);
    }

    @Description("round to given number of decimal places")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double round(@SqlType(DoubleType.NAME) double num, @SqlType(BigintType.NAME) long decimals)
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
    @SqlType(DoubleType.NAME)
    public static double sin(@SqlType(DoubleType.NAME) double num)
    {
        return Math.sin(num);
    }

    @Description("square root")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double sqrt(@SqlType(DoubleType.NAME) double num)
    {
        return Math.sqrt(num);
    }

    @Description("tangent")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double tan(@SqlType(DoubleType.NAME) double num)
    {
        return Math.tan(num);
    }

    @Description("hyperbolic tangent")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double tanh(@SqlType(DoubleType.NAME) double num)
    {
        return Math.tanh(num);
    }

    @Description("test if value is not-a-number")
    @ScalarFunction("is_nan")
    @SqlType(BooleanType.NAME)
    public static boolean isNaN(@SqlType(DoubleType.NAME) double num)
    {
        return Double.isNaN(num);
    }

    @Description("test if value is finite")
    @ScalarFunction
    @SqlType(BooleanType.NAME)
    public static boolean isFinite(@SqlType(DoubleType.NAME) double num)
    {
        return Doubles.isFinite(num);
    }

    @Description("test if value is infinite")
    @ScalarFunction
    @SqlType(BooleanType.NAME)
    public static boolean isInfinite(@SqlType(DoubleType.NAME) double num)
    {
        return Double.isInfinite(num);
    }

    @Description("constant representing not-a-number")
    @ScalarFunction("nan")
    @SqlType(DoubleType.NAME)
    public static double NaN()
    {
        return Double.NaN;
    }

    @Description("Infinity")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double infinity()
    {
        return Double.POSITIVE_INFINITY;
    }

    @Description("get the largest of the given values")
    @ScalarFunction
    @SqlType(BigintType.NAME)
    public static long greatest(@SqlType(BigintType.NAME) long value1, @SqlType(BigintType.NAME) long value2)
    {
        return value1 > value2 ? value1 : value2;
    }

    @Description("get the largest of the given values")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double greatest(@SqlType(DoubleType.NAME) double value1, @SqlType(DoubleType.NAME) double value2)
    {
        if (Double.isNaN(value1) || Double.isNaN(value2)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT.toErrorCode(), "Invalid argument to greatest(): NaN");
        }

        return value1 > value2 ? value1 : value2;
    }

    @Description("get the smallest of the given values")
    @ScalarFunction
    @SqlType(BigintType.NAME)
    public static long least(@SqlType(BigintType.NAME) long value1, @SqlType(BigintType.NAME) long value2)
    {
        return value1 < value2 ? value1 : value2;
    }

    @Description("get the smallest of the given values")
    @ScalarFunction
    @SqlType(DoubleType.NAME)
    public static double least(@SqlType(DoubleType.NAME) double value1, @SqlType(DoubleType.NAME) double value2)
    {
        if (Double.isNaN(value1) || Double.isNaN(value2)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT.toErrorCode(), "Invalid argument to least(): NaN");
        }

        return value1 < value2 ? value1 : value2;
    }
}
