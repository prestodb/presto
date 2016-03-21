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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.SqlScalarFunctionBuilder.SpecializeContext;
import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.LiteralParameters;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import io.airlift.slice.Slice;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.longVariableExpression;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.type.Decimals.bigIntegerTenToNth;
import static com.facebook.presto.spi.type.Decimals.checkOverflow;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.type.DecimalOperators.modulusScalarFunction;
import static com.facebook.presto.type.DecimalOperators.modulusSignatureBuilder;
import static com.facebook.presto.util.Failures.checkCondition;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Character.MIN_RADIX;
import static java.lang.String.format;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;

public final class MathFunctions
{
    public static final SqlScalarFunction DECIMAL_TRUNCATE_FUNCTION = decimalTruncateNFunction();
    public static final SqlScalarFunction[] DECIMAL_CEILING_FUNCTIONS = {decimalCeilingFunction("ceiling"), decimalCeilingFunction("ceil")};
    public static final SqlScalarFunction DECIMAL_FLOOR_FUNCTION = decimalFloorFunction();
    public static final SqlScalarFunction DECIMAL_MOD_FUNCTION = decimalModFunction();
    public static final SqlScalarFunction[] DECIMAL_ROUND_FUNCTIONS = {decimalRoundFunction(), decimalRoundNFunction()};

    private MathFunctions() {}

    @Description("absolute value")
    @ScalarFunction
    @SqlType(BIGINT)
    public static long abs(@SqlType(BIGINT) long num)
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

    @ScalarFunction("abs")
    @Description("absolute value")
    public static final class Abs
    {
        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static long absShort(@SqlType("decimal(p, s)") long arg)
        {
            return arg > 0 ? arg : -arg;
        }

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static Slice absLong(@SqlType("decimal(p, s)") Slice arg)
        {
            return encodeUnscaledValue(decodeUnscaledValue(arg).abs());
        }
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
    @SqlType(BIGINT)
    public static long ceiling(@SqlType(BIGINT) long num)
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

    private static SqlScalarFunction decimalCeilingFunction(String name)
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .name(name)
                .literalParameters("num_precision", "num_scale", "return_precision")
                .longVariableConstraints(longVariableExpression("return_precision", "num_precision - num_scale + min(num_scale, 1)"))
                .argumentTypes("decimal(num_precision, num_scale)")
                .returnType("decimal(return_precision,0)")
                .build();
        return SqlScalarFunction.builder(MathFunctions.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("ceilingShortShortDecimal")
                        .withExtraParameters(MathFunctions::decimalTenToScaleAsLongExtraParameters)
                )
                .implementation(b -> b
                        .methods("ceilingLongShortDecimal", "ceilingLongLongDecimal")
                        .withExtraParameters(MathFunctions::decimalTenToScaleAsBigDecimalExtraParameters)
                )
                .build();
    }

    private static List<Object> decimalTenToScaleAsBigDecimalExtraParameters(SpecializeContext context)
    {
        return ImmutableList.of(bigIntegerTenToNth(context.getLiteral("num_scale").intValue()));
    }

    private static List<Object> decimalTenToScaleAsLongExtraParameters(SpecializeContext context)
    {
        return ImmutableList.of(longTenToNth(context.getLiteral("num_scale").intValue()));
    }

    public static long ceilingShortShortDecimal(long num, long divisor)
    {
        long increment = (num % divisor) > 0 ? 1 : 0;
        return num / divisor + increment;
    }

    public static long ceilingLongShortDecimal(Slice num, BigInteger divisor)
    {
        return ceiling(num, divisor).longValueExact();
    }

    public static Slice ceilingLongLongDecimal(Slice num, BigInteger divisor)
    {
        return encodeUnscaledValue(ceiling(num, divisor));
    }

    private static BigInteger ceiling(Slice num, BigInteger divisor)
    {
        BigInteger[] divideAndRemainder = decodeUnscaledValue(num).divideAndRemainder(divisor);
        return divideAndRemainder[0].add(BigInteger.valueOf(divideAndRemainder[1].signum() > 0 ? 1 : 0));
    }

    @Description("round to integer by dropping digits after decimal point")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double truncate(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.signum(num) * Math.floor(Math.abs(num));
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
    @SqlType(BIGINT)
    public static long floor(@SqlType(BIGINT) long num)
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

    private static SqlScalarFunction decimalFloorFunction()
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .name("floor")
                .literalParameters("num_precision", "num_scale", "return_precision")
                .longVariableConstraints(longVariableExpression("return_precision", "num_precision - num_scale + min(num_scale, 1)"))
                .argumentTypes("decimal(num_precision, num_scale)")
                .returnType("decimal(return_precision,0)")
                .build();
        return SqlScalarFunction.builder(MathFunctions.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("floorShortShortDecimal")
                        .withExtraParameters(MathFunctions::decimalTenToScaleAsLongExtraParameters)
                )
                .implementation(b -> b
                        .methods("floorLongShortDecimal", "floorLongLongDecimal")
                        .withExtraParameters(MathFunctions::decimalTenToScaleAsBigDecimalExtraParameters)
                )
                .build();
    }

    public static long floorShortShortDecimal(long num, long divisor)
    {
        long increment = (num % divisor) < 0 ? -1 : 0;
        return num / divisor + increment;
    }

    public static Slice floorLongLongDecimal(Slice num, BigInteger divisor)
    {
        return encodeUnscaledValue(floor(num, divisor));
    }

    public static long floorLongShortDecimal(Slice num, BigInteger divisor)
    {
        return floor(num, divisor).longValueExact();
    }

    private static BigInteger floor(Slice num, BigInteger divisor)
    {
        BigInteger[] divideAndRemainder = decodeUnscaledValue(num).divideAndRemainder(divisor);
        return divideAndRemainder[0].add(BigInteger.valueOf(divideAndRemainder[1].signum() < 0 ? -1 : 0));
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
    @SqlType(BIGINT)
    public static long mod(@SqlType(BIGINT) long num1, @SqlType(BIGINT) long num2)
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

    private static SqlScalarFunction decimalModFunction()
    {
        Signature signature = modulusSignatureBuilder()
                .kind(SCALAR)
                .name("mod")
                .build();
        return modulusScalarFunction(signature);
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
    @SqlType(BIGINT)
    public static long random(@SqlType(BIGINT) long value)
    {
        checkCondition(value > 0, INVALID_FUNCTION_ARGUMENT, "bound must be positive");
        return ThreadLocalRandom.current().nextLong(value);
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(BIGINT)
    public static long round(@SqlType(BIGINT) long num)
    {
        return round(num, 0);
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(BIGINT)
    public static long round(@SqlType(BIGINT) long num, @SqlType(BIGINT) long decimals)
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
    public static double round(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(BIGINT) long decimals)
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

    private static SqlScalarFunction decimalRoundFunction()
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .name("round")
                .literalParameters("num_precision", "num_scale", "result_precision")
                .longVariableConstraints(
                        longVariableExpression("result_precision", "min(38, num_precision - num_scale + min(1, num_scale))"))
                .argumentTypes("decimal(num_precision, num_scale)")
                .returnType("decimal(result_precision,0)")
                .build();
        return SqlScalarFunction.builder(MathFunctions.class)
                .signature(signature)
                .description("round to nearest integer")
                .implementation(b -> b
                        .methods("roundShortDecimal")
                        .withExtraParameters(MathFunctions::decimalRoundShortExtraParameters)
                )
                .implementation(b -> b
                        .methods("roundLongDecimal")
                        .withExtraParameters(MathFunctions::decimalRoundLongExtraParameters)
                )
                .build();
    }

    private static List<Object> decimalRoundShortExtraParameters(SpecializeContext context)
    {
        long scale = context.getLiteral("num_scale");
        long rescaleFactor = longTenToNth((int) scale);
        return ImmutableList.of(rescaleFactor, scale);
    }

    public static long roundShortDecimal(long num, long rescaleFactor, long inputScale)
    {
        if (num == 0) {
            return 0;
        }
        if (inputScale == 0) {
            return num;
        }
        if (num < 0) {
            return -roundShortDecimal(-num, rescaleFactor, inputScale);
        }

        long remainder = num % rescaleFactor;
        long remainderBoundary = rescaleFactor >> 1;
        int roundUp = remainder >= remainderBoundary ? 1 : 0;
        return num / rescaleFactor + roundUp;
    }

    private static List<Object> decimalRoundLongExtraParameters(SpecializeContext context)
    {
        long scale = context.getLiteral("num_scale");
        BigInteger rescaleFactor = bigIntegerTenToNth((int) scale);
        return ImmutableList.of(rescaleFactor, scale);
    }

    public static Slice roundLongDecimal(Slice numSlice, BigInteger rescaleFactor, long inputScale)
    {
        BigInteger num = decodeUnscaledValue(numSlice);
        if (num.signum() == 0) {
            return encodeUnscaledValue(0);
        }
        if (inputScale == 0) {
            return encodeUnscaledValue(num);
        }
        if (num.signum() < 0) {
            return encodeUnscaledValue(roundLongDecimal(num.negate(), rescaleFactor).negate());
        }
        return encodeUnscaledValue(roundLongDecimal(num, rescaleFactor));
    }

    private static BigInteger roundLongDecimal(BigInteger num, BigInteger rescaleFactor)
    {
        BigInteger[] divideAndRemainder = num.divideAndRemainder(rescaleFactor);
        BigInteger roundUp = divideAndRemainder[1].compareTo(rescaleFactor.shiftRight(1)) >= 0 ? ONE : ZERO;
        return divideAndRemainder[0].add(roundUp);
    }

    private static SqlScalarFunction decimalRoundNFunction()
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .name("round")
                .literalParameters("num_precision", "num_scale", "result_precision")
                .longVariableConstraints(
                        //result precision = increment the input precision only if the input number has a decimal point (scale > 0)
                        longVariableExpression("result_precision", "min(38, num_precision + 1)"))
                .argumentTypes("decimal(num_precision, num_scale)", BIGINT)
                .returnType("decimal(result_precision, num_scale)")
                .build();
        return SqlScalarFunction.builder(MathFunctions.class)
                .signature(signature)
                .description("round to given number of decimal places")
                .implementation(b -> b
                    .methods("roundNShortDecimal", "roundNLongDecimal")
                    .withExtraParameters(MathFunctions::decimalRoundNExtraParameters)
                )
                .build();
    }

    private static List<Object> decimalRoundNExtraParameters(SpecializeContext context)
    {
        return ImmutableList.of(context.getLiteral("num_precision"), context.getLiteral("num_scale"));
    }

    public static long roundNShortDecimal(long num, long roundScale, long inputPrecision, long inputScale)
    {
        if (num == 0 || inputPrecision - inputScale + roundScale <= 0) {
            return 0;
        }
        if (roundScale >= inputScale) {
            return num;
        }
        if (num < 0) {
            return -roundNShortDecimal(-num, roundScale, inputPrecision, inputScale);
        }

        long rescaleFactor = longTenToNth((int) (inputScale - roundScale));
        long remainder = num % rescaleFactor;
        int roundUp = (remainder >= rescaleFactor >> 1) ? 1 : 0;
        return (num / rescaleFactor + roundUp) * rescaleFactor;
    }

    public static Slice roundNLongDecimal(Slice num, long roundScale, long inputPrecision, long inputScale)
    {
        BigInteger unscaledVal = decodeUnscaledValue(num);
        if (unscaledVal.signum() == 0 || inputPrecision - inputScale + roundScale <= 0) {
            return encodeUnscaledValue(0);
        }
        if (roundScale >= inputScale) {
            return num;
        }
        BigInteger rescaleFactor = bigIntegerTenToNth((int) (inputScale - roundScale));
        if (unscaledVal.signum() < 0) {
            return encodeUnscaledValue(roundNLongDecimal(unscaledVal.negate(), rescaleFactor).negate());
        }
        return encodeUnscaledValue(roundNLongDecimal(unscaledVal, rescaleFactor));
    }

    public static BigInteger roundNLongDecimal(BigInteger num, BigInteger rescaleFactor)
    {
        BigInteger[] divideAndRemainder = num.divideAndRemainder(rescaleFactor);
        BigInteger roundUp = divideAndRemainder[1].compareTo(rescaleFactor.shiftRight(1)) >= 0 ? ONE : ZERO;
        BigInteger rounded = divideAndRemainder[0].add(roundUp).multiply(rescaleFactor);
        checkOverflow(rounded);
        return rounded;
    }

    private static SqlScalarFunction decimalTruncateNFunction()
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .name("truncate")
                .literalParameters("num_precision", "num_scale")
                .argumentTypes("decimal(num_precision, num_scale)", BIGINT)
                .returnType("decimal(num_precision, num_scale)")
                .build();
        return SqlScalarFunction.builder(MathFunctions.class)
                .signature(signature)
                .description("truncate decimal to N places after decimal point")
                .implementation(b -> b
                        .methods("truncateNShortDecimal", "truncateNLongDecimal")
                        .withExtraParameters(MathFunctions::decimalTruncateNExtraParameters)
                )
                .build();
    }

    private static List<Object> decimalTruncateNExtraParameters(SpecializeContext context)
    {
        return ImmutableList.of(context.getLiteral("num_precision"), context.getLiteral("num_scale"));
    }

    public static long truncateNShortDecimal(long num, long roundScale, long inputPrecision, long inputScale)
    {
        if (num == 0 || inputPrecision - inputScale + roundScale <= 0) {
            return 0;
        }
        if (roundScale >= inputScale) {
            return num;
        }
        if (num < 0) {
            return -truncateNShortDecimal(-num, roundScale, inputPrecision, inputScale);
        }

        long rescaleFactor = longTenToNth((int) (inputScale - roundScale));
        long remainder = num % rescaleFactor;
        return num - remainder;
    }

    public static Slice truncateNLongDecimal(Slice num, long roundScale, long inputPrecision, long inputScale)
    {
        BigInteger unscaledVal = decodeUnscaledValue(num);
        if (unscaledVal.signum() == 0 || inputPrecision - inputScale + roundScale <= 0) {
            return encodeUnscaledValue(0);
        }
        if (roundScale >= inputScale) {
            return num;
        }
        BigInteger rescaleFactor = bigIntegerTenToNth((int) (inputScale - roundScale));
        if (unscaledVal.signum() < 0) {
            return encodeUnscaledValue(truncateNLongDecimal(unscaledVal.negate(), rescaleFactor).negate());
        }
        return encodeUnscaledValue(truncateNLongDecimal(unscaledVal, rescaleFactor));
    }

    public static BigInteger truncateNLongDecimal(BigInteger num, BigInteger rescaleFactor)
    {
        BigInteger remainder = num.remainder(rescaleFactor);
        return num.subtract(remainder);
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
    public static Slice toBase(@SqlType(BIGINT) long value, @SqlType(BIGINT) long radix)
    {
        checkRadix(radix);
        return utf8Slice(Long.toString(value, (int) radix));
    }

    @Description("convert a string in the given base to a number")
    @ScalarFunction
    @SqlType(BIGINT)
    public static long fromBase(@SqlType(StandardTypes.VARCHAR) Slice value, @SqlType(BIGINT) long radix)
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
