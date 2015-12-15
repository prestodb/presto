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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.primitives.Doubles;
import io.airlift.slice.Slice;

import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.util.Failures.checkCondition;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Character.MIN_RADIX;
import static java.lang.String.format;

public final class MathFunctions
{
    private MathFunctions() {}

    @Description("absolute value")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long abs(@SqlType(StandardTypes.BIGINT) long num)
    {
        checkCondition(num != Long.MIN_VALUE, NUMERIC_VALUE_OUT_OF_RANGE, "Value -9223372036854775808 is out of range for abs()");
        return Math.abs(num);
    }

    @Description("absolute value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double abs(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.abs(num);
    }

    @Description("arc cosine")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double acos(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.acos(num);
    }

    @Description("arc sine")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double asin(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.asin(num);
    }

    @Description("arc tangent")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double atan(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.atan(num);
    }

    @Description("arc tangent of given fraction")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double atan2(@SqlType(StandardTypes.DOUBLE) double num1, @SqlType(StandardTypes.DOUBLE) double num2)
    {
        return Math.atan2(num1, num2);
    }

    @Description("cube root")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double cbrt(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.cbrt(num);
    }

    @Description("round up to nearest integer")
    @ScalarFunction(alias = "ceil")
    @SqlType(StandardTypes.BIGINT)
    public static long ceiling(@SqlType(StandardTypes.BIGINT) long num)
    {
        return num;
    }

    @Description("round up to nearest integer")
    @ScalarFunction(alias = "ceil")
    @SqlType(StandardTypes.DOUBLE)
    public static double ceiling(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.ceil(num);
    }

    @Description("cosine")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double cos(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.cos(num);
    }

    @Description("hyperbolic cosine")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double cosh(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.cosh(num);
    }

    @Description("converts an angle in radians to degrees")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double degrees(@SqlType(StandardTypes.DOUBLE) double radians)
    {
        return Math.toDegrees(radians);
    }

    @Description("Euler's number")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double e()
    {
        return Math.E;
    }

    @Description("Euler's number raised to the given power")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double exp(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.exp(num);
    }

    @Description("round down to nearest integer")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long floor(@SqlType(StandardTypes.BIGINT) long num)
    {
        return num;
    }

    @Description("round down to nearest integer")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double floor(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.floor(num);
    }

    @Description("natural logarithm")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double ln(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.log(num);
    }

    @Description("logarithm to base 2")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double log2(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.log(num) / Math.log(2);
    }

    @Description("logarithm to base 10")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double log10(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.log10(num);
    }

    @Description("logarithm to given base")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double log(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.DOUBLE) double base)
    {
        return Math.log(num) / Math.log(base);
    }

    @Description("remainder of given quotient")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long mod(@SqlType(StandardTypes.BIGINT) long num1, @SqlType(StandardTypes.BIGINT) long num2)
    {
        return num1 % num2;
    }

    @Description("remainder of given quotient")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double mod(@SqlType(StandardTypes.DOUBLE) double num1, @SqlType(StandardTypes.DOUBLE) double num2)
    {
        return num1 % num2;
    }

    @Description("the constant Pi")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double pi()
    {
        return Math.PI;
    }

    @Description("value raised to the power of exponent")
    @ScalarFunction(alias = "pow")
    @SqlType(StandardTypes.DOUBLE)
    public static double power(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.DOUBLE) double exponent)
    {
        return Math.pow(num, exponent);
    }

    @Description("converts an angle in degrees to radians")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double radians(@SqlType(StandardTypes.DOUBLE) double degrees)
    {
        return Math.toRadians(degrees);
    }

    @Description("a pseudo-random value")
    @ScalarFunction(alias = "rand", deterministic = false)
    @SqlType(StandardTypes.DOUBLE)
    public static double random()
    {
        return ThreadLocalRandom.current().nextDouble();
    }

    @Description("a pseudo-random number between 0 and value (exclusive)")
    @ScalarFunction(alias = "rand", deterministic = false)
    @SqlType(StandardTypes.BIGINT)
    public static long random(@SqlType(StandardTypes.BIGINT) long value)
    {
        return ThreadLocalRandom.current().nextLong(value);
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long round(@SqlType(StandardTypes.BIGINT) long num)
    {
        return round(num, 0);
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long round(@SqlType(StandardTypes.BIGINT) long num, @SqlType(StandardTypes.BIGINT) long decimals)
    {
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double round(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return round(num, 0);
    }

    @Description("round to given number of decimal places")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double round(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.BIGINT) long decimals)
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
    @SqlType(StandardTypes.DOUBLE)
    public static double sin(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.sin(num);
    }

    @Description("square root")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double sqrt(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.sqrt(num);
    }

    @Description("tangent")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double tan(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.tan(num);
    }

    @Description("hyperbolic tangent")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double tanh(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.tanh(num);
    }

    @Description("test if value is not-a-number")
    @ScalarFunction("is_nan")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isNaN(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Double.isNaN(num);
    }

    @Description("test if value is finite")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isFinite(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Doubles.isFinite(num);
    }

    @Description("test if value is infinite")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isInfinite(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Double.isInfinite(num);
    }

    @Description("constant representing not-a-number")
    @ScalarFunction("nan")
    @SqlType(StandardTypes.DOUBLE)
    public static double NaN()
    {
        return Double.NaN;
    }

    @Description("Infinity")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double infinity()
    {
        return Double.POSITIVE_INFINITY;
    }

    @Description("convert a number to a string in the given base")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice toBase(@SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long radix)
    {
        checkRadix(radix);
        return utf8Slice(Long.toString(value, (int) radix));
    }

    @Description("convert a string in the given base to a number")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long fromBase(@SqlType(StandardTypes.VARCHAR) Slice value, @SqlType(StandardTypes.BIGINT) long radix)
    {
        checkRadix(radix);
        try {
            return Long.parseLong(value.toStringUtf8(), (int) radix);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Not a valid base-%d number: %s", radix, value.toStringUtf8()), e);
        }
    }

    private static void checkRadix(long radix)
    {
        checkCondition(radix >= MIN_RADIX && radix <= MAX_RADIX,
                INVALID_FUNCTION_ARGUMENT, "Radix must be between %d and %d", MIN_RADIX, MAX_RADIX);
    }
}
