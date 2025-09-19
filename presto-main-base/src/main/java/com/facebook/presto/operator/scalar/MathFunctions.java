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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.UnscaledDecimal128Arithmetic;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameter;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.Constraint;
import com.facebook.presto.util.SecureRandomGeneration;
import com.google.common.primitives.Doubles;
import io.airlift.slice.Slice;
import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.CauchyDistribution;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.FDistribution;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.WeibullDistribution;
import org.apache.commons.math3.special.Erf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.Decimals.longTenToNth;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.add;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.isNegative;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.isZero;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.negate;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.rescale;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.rescaleTruncate;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.subtract;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.throwIfOverflows;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLong;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.type.DecimalOperators.modulusScalarFunction;
import static com.facebook.presto.type.DecimalOperators.modulusSignatureBuilder;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.math.DoubleMath.roundToLong;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Character.MIN_RADIX;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;

public final class MathFunctions
{
    public static final SqlScalarFunction DECIMAL_MOD_FUNCTION = decimalModFunction();

    private static final Slice[] DECIMAL_HALF_UNSCALED_FOR_SCALE;
    private static final Slice[] DECIMAL_ALMOST_HALF_UNSCALED_FOR_SCALE;

    static {
        DECIMAL_HALF_UNSCALED_FOR_SCALE = new Slice[Decimals.MAX_PRECISION];
        DECIMAL_ALMOST_HALF_UNSCALED_FOR_SCALE = new Slice[Decimals.MAX_PRECISION];
        DECIMAL_HALF_UNSCALED_FOR_SCALE[0] = UnscaledDecimal128Arithmetic.unscaledDecimal(0);
        DECIMAL_ALMOST_HALF_UNSCALED_FOR_SCALE[0] = UnscaledDecimal128Arithmetic.unscaledDecimal(0);
        for (int scale = 1; scale < Decimals.MAX_PRECISION; ++scale) {
            DECIMAL_HALF_UNSCALED_FOR_SCALE[scale] = UnscaledDecimal128Arithmetic.unscaledDecimal(
                    BigInteger.TEN
                            .pow(scale)
                            .divide(BigInteger.valueOf(2)));
            DECIMAL_ALMOST_HALF_UNSCALED_FOR_SCALE[scale] = UnscaledDecimal128Arithmetic.unscaledDecimal(
                    BigInteger.TEN
                            .pow(scale)
                            .divide(BigInteger.valueOf(2))
                            .subtract(BigInteger.ONE));
        }
    }

    private MathFunctions() {}

    @Description("absolute value")
    @ScalarFunction("abs")
    @SqlType(StandardTypes.TINYINT)
    public static long absTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        checkCondition(num != Byte.MIN_VALUE, NUMERIC_VALUE_OUT_OF_RANGE, "Value -128 is out of range for abs(tinyint)");
        return Math.abs(num);
    }

    @Description("absolute value")
    @ScalarFunction("abs")
    @SqlType(StandardTypes.SMALLINT)
    public static long absSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        checkCondition(num != Short.MIN_VALUE, NUMERIC_VALUE_OUT_OF_RANGE, "Value -32768 is out of range for abs(smallint)");
        return Math.abs(num);
    }

    @Description("absolute value")
    @ScalarFunction("abs")
    @SqlType(StandardTypes.INTEGER)
    public static long absInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        checkCondition(num != Integer.MIN_VALUE, NUMERIC_VALUE_OUT_OF_RANGE, "Value -2147483648 is out of range for abs(integer)");
        return Math.abs(num);
    }

    @Description("absolute value")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long abs(@SqlType(StandardTypes.BIGINT) long num)
    {
        checkCondition(num != Long.MIN_VALUE, NUMERIC_VALUE_OUT_OF_RANGE, "Value -9223372036854775808 is out of range for abs(bigint)");
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
        private Abs() {}

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
            if (isNegative(arg)) {
                Slice result = unscaledDecimal(arg);
                negate(result);
                return result;
            }
            else {
                return arg;
            }
        }
    }

    @ScalarFunction("log")
    @Description("logarithm to given base")
    public static final class LegacyLogFunction
    {
        private LegacyLogFunction() {}

        @SqlType(StandardTypes.DOUBLE)
        public static double log(@SqlType(StandardTypes.DOUBLE) double number, @SqlType(StandardTypes.DOUBLE) double base)
        {
            return Math.log(number) / Math.log(base);
        }
    }

    @Description("absolute value")
    @ScalarFunction("abs")
    @SqlType(StandardTypes.REAL)
    public static long absFloat(@SqlType(StandardTypes.REAL) long num)
    {
        return floatToRawIntBits(Math.abs(intBitsToFloat((int) num)));
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

    @Description("Binomial cdf given numberOfTrials, successProbability, and a value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double binomialCdf(
            @SqlType(StandardTypes.INTEGER) long numberOfTrials,
            @SqlType(StandardTypes.DOUBLE) double successProbability,
            @SqlType(StandardTypes.INTEGER) long value)
    {
        checkCondition(successProbability >= 0 && successProbability <= 1, INVALID_FUNCTION_ARGUMENT, "successProbability must be in the interval [0, 1]");
        checkCondition(numberOfTrials > 0, INVALID_FUNCTION_ARGUMENT, "numberOfTrials must be greater than 0");
        BinomialDistribution distribution = new BinomialDistribution(null, (int) numberOfTrials, successProbability);
        return distribution.cumulativeProbability((int) value);
    }

    @Description("cube root")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double cbrt(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.cbrt(num);
    }

    @Description("round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil")
    @SqlType(StandardTypes.TINYINT)
    public static long ceilingTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        return num;
    }

    @Description("round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil")
    @SqlType(StandardTypes.SMALLINT)
    public static long ceilingSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        return num;
    }

    @Description("round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil")
    @SqlType(StandardTypes.INTEGER)
    public static long ceilingInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        return num;
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

    @Description("round up to nearest integer")
    @ScalarFunction(value = "ceiling", alias = "ceil")
    @SqlType(StandardTypes.REAL)
    public static long ceilingFloat(@SqlType(StandardTypes.REAL) long num)
    {
        return floatToRawIntBits((float) ceiling(intBitsToFloat((int) num)));
    }

    @ScalarFunction(value = "ceiling", alias = "ceil")
    @Description("round up to nearest integer")
    public static final class Ceiling
    {
        private Ceiling() {}

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static long ceilingShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") long num)
        {
            long rescaleFactor = Decimals.longTenToNth((int) numScale);
            long increment = (num % rescaleFactor > 0) ? 1 : 0;
            return num / rescaleFactor + increment;
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static Slice ceilingLong(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Slice num)
        {
            Slice tmp;
            if (isNegative(num)) {
                tmp = add(num, DECIMAL_HALF_UNSCALED_FOR_SCALE[(int) numScale]);
            }
            else {
                tmp = add(num, DECIMAL_ALMOST_HALF_UNSCALED_FOR_SCALE[(int) numScale]);
            }
            return rescale(tmp, -(int) numScale);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static long ceilingLongShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Slice num)
        {
            return unscaledDecimalToUnscaledLong(ceilingLong(numScale, num));
        }
    }

    @Description("round to integer by dropping digits after decimal point")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double truncate(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.signum(num) * Math.floor(Math.abs(num));
    }

    @Description("round to integer by dropping digits after decimal point")
    @ScalarFunction
    @SqlType(StandardTypes.REAL)
    public static long truncate(@SqlType(StandardTypes.REAL) long num)
    {
        float numInFloat = intBitsToFloat((int) num);
        return floatToRawIntBits((float) (Math.signum(numInFloat) * Math.floor(Math.abs(numInFloat))));
    }

    @Description("truncate to double by dropping digits after decimal point")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double truncate(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        if (Double.isNaN(num) || Double.isInfinite(num)) {
            // compatible with truncate(DOUBLE)
            return num;
        }
        if (decimals == 0) {
            if (num >= 0) {
                return Math.floor(num);
            }
            else {
                return Math.ceil(num);
            }
        }

        return BigDecimal.valueOf(num).setScale((int) decimals, BigDecimal.ROUND_DOWN).doubleValue();
    }

    @Description("truncate to float by dropping digits after decimal point")
    @ScalarFunction
    @SqlType(StandardTypes.REAL)
    public static long truncate(@SqlType(StandardTypes.REAL) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        float numBitsToFloats = intBitsToFloat((int) num);
        if (Float.isNaN(numBitsToFloats) || Float.isInfinite(numBitsToFloats)) {
            // compatible with truncate(REAL)
            return num;
        }
        if (decimals == 0) {
            if (numBitsToFloats >= 0) {
                return floatToRawIntBits((float) Math.floor(numBitsToFloats));
            }
            else {
                return floatToRawIntBits((float) Math.ceil(numBitsToFloats));
            }
        }

        return floatToRawIntBits(new BigDecimal(Float.toString(numBitsToFloats)).setScale((int) decimals, BigDecimal.ROUND_DOWN).floatValue());
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
    @ScalarFunction("floor")
    @SqlType(StandardTypes.TINYINT)
    public static long floorTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        return num;
    }

    @Description("round down to nearest integer")
    @ScalarFunction("floor")
    @SqlType(StandardTypes.SMALLINT)
    public static long floorSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        return num;
    }

    @Description("round down to nearest integer")
    @ScalarFunction("floor")
    @SqlType(StandardTypes.INTEGER)
    public static long floorInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        return num;
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

    @ScalarFunction(value = "floor")
    @Description("round down to nearest integer")
    public static final class Floor
    {
        private Floor() {}

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static long floorShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") long num)
        {
            long rescaleFactor = Decimals.longTenToNth((int) numScale);
            long increment = (num % rescaleFactor) < 0 ? -1 : 0;
            return num / rescaleFactor + increment;
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static Slice floorLong(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Slice num)
        {
            Slice tmp;
            if (isZero(num)) {
                return num;
            }
            if (isNegative(num)) {
                tmp = subtract(num, DECIMAL_ALMOST_HALF_UNSCALED_FOR_SCALE[(int) numScale]);
            }
            else {
                tmp = subtract(num, DECIMAL_HALF_UNSCALED_FOR_SCALE[(int) numScale]);
            }
            return rescale(tmp, -(int) numScale);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "p - s + min(s, 1)")
        public static long floorLongShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Slice num)
        {
            return unscaledDecimalToUnscaledLong(floorLong(numScale, num));
        }
    }

    @Description("round down to nearest integer")
    @ScalarFunction("floor")
    @SqlType(StandardTypes.REAL)
    public static long floorFloat(@SqlType(StandardTypes.REAL) long num)
    {
        return floatToRawIntBits((float) floor(intBitsToFloat((int) num)));
    }

    @Description("inverse of Binomial cdf given numberOfTrials, successProbability parameters and p")
    @ScalarFunction
    @SqlType(StandardTypes.INTEGER)
    public static long inverseBinomialCdf(
            @SqlType(StandardTypes.INTEGER) long numberOfTrials,
            @SqlType(StandardTypes.DOUBLE) double successProbability,
            @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(p >= 0 && p <= 1, INVALID_FUNCTION_ARGUMENT, "p must be in the interval [0, 1]");
        checkCondition(successProbability >= 0 && successProbability <= 1, INVALID_FUNCTION_ARGUMENT, "successProbability must be in the interval [0, 1]");
        checkCondition(numberOfTrials > 0, INVALID_FUNCTION_ARGUMENT, "numberOfTrials must be greater than 0");
        BinomialDistribution distribution = new BinomialDistribution(null, (int) numberOfTrials, successProbability);
        return distribution.inverseCumulativeProbability(p);
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

    @Description("remainder of given quotient")
    @ScalarFunction("mod")
    @SqlType(StandardTypes.TINYINT)
    public static long modTinyint(@SqlType(StandardTypes.TINYINT) long num1, @SqlType(StandardTypes.TINYINT) long num2)
    {
        return num1 % num2;
    }

    @Description("remainder of given quotient")
    @ScalarFunction("mod")
    @SqlType(StandardTypes.SMALLINT)
    public static long modSmallint(@SqlType(StandardTypes.SMALLINT) long num1, @SqlType(StandardTypes.SMALLINT) long num2)
    {
        return num1 % num2;
    }

    @Description("remainder of given quotient")
    @ScalarFunction("mod")
    @SqlType(StandardTypes.INTEGER)
    public static long modInteger(@SqlType(StandardTypes.INTEGER) long num1, @SqlType(StandardTypes.INTEGER) long num2)
    {
        return num1 % num2;
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

    private static SqlScalarFunction decimalModFunction()
    {
        Signature signature = modulusSignatureBuilder()
                .kind(SCALAR)
                .name("mod")
                .build();
        return modulusScalarFunction(signature);
    }

    @Description("remainder of given quotient")
    @ScalarFunction("mod")
    @SqlType(StandardTypes.REAL)
    public static long modFloat(@SqlType(StandardTypes.REAL) long num1, @SqlType(StandardTypes.REAL) long num2)
    {
        return floatToRawIntBits(intBitsToFloat((int) num1) % intBitsToFloat((int) num2));
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
    @ScalarFunction(value = "random", alias = "rand", deterministic = false)
    @SqlType(StandardTypes.TINYINT)
    public static long randomTinyint(@SqlType(StandardTypes.TINYINT) long value)
    {
        checkCondition(value > 0, INVALID_FUNCTION_ARGUMENT, "bound must be positive");
        return ThreadLocalRandom.current().nextInt((int) value);
    }

    @Description("a pseudo-random number between 0 and value (exclusive)")
    @ScalarFunction(value = "random", alias = "rand", deterministic = false)
    @SqlType(StandardTypes.SMALLINT)
    public static long randomSmallint(@SqlType(StandardTypes.SMALLINT) long value)
    {
        checkCondition(value > 0, INVALID_FUNCTION_ARGUMENT, "bound must be positive");
        return ThreadLocalRandom.current().nextInt((int) value);
    }

    @Description("a pseudo-random number between 0 and value (exclusive)")
    @ScalarFunction(value = "random", alias = "rand", deterministic = false)
    @SqlType(StandardTypes.INTEGER)
    public static long randomInteger(@SqlType(StandardTypes.INTEGER) long value)
    {
        checkCondition(value > 0, INVALID_FUNCTION_ARGUMENT, "bound must be positive");
        return ThreadLocalRandom.current().nextInt((int) value);
    }

    @Description("a pseudo-random number between 0 and value (exclusive)")
    @ScalarFunction(alias = "rand", deterministic = false)
    @SqlType(StandardTypes.BIGINT)
    public static long random(@SqlType(StandardTypes.BIGINT) long value)
    {
        checkCondition(value > 0, INVALID_FUNCTION_ARGUMENT, "bound must be positive");
        return ThreadLocalRandom.current().nextLong(value);
    }

    @Description("a cryptographically secure random number between 0 and 1 (exclusive)")
    @ScalarFunction(value = "secure_random", alias = "secure_rand", deterministic = false)
    @SqlType(StandardTypes.DOUBLE)
    public static double secure_random()
    {
        SecureRandom random = SecureRandomGeneration.getNonBlocking();
        return random.nextDouble();
    }

    @Description("a cryptographically secure random number between lower and upper (exclusive)")
    @ScalarFunction(value = "secure_random", alias = "secure_rand", deterministic = false)
    @SqlType(StandardTypes.DOUBLE)
    public static double secure_random(@SqlType(StandardTypes.DOUBLE) double lower, @SqlType(StandardTypes.DOUBLE) double upper)
    {
        checkCondition(lower < upper, INVALID_FUNCTION_ARGUMENT, "upper bound must be greater than lower bound");
        SecureRandom random = SecureRandomGeneration.getNonBlocking();
        return random.doubles(lower, upper)
                .findFirst()
                .getAsDouble();
    }

    @Description("a cryptographically secure random number between lower and upper (exclusive)")
    @ScalarFunction(value = "secure_random", alias = "secure_rand", deterministic = false)
    @SqlType(StandardTypes.TINYINT)
    public static long secureRandomTinyint(@SqlType(StandardTypes.TINYINT) long lower, @SqlType(StandardTypes.TINYINT) long upper)
    {
        checkCondition(lower < upper, INVALID_FUNCTION_ARGUMENT, "upper bound must be greater than lower bound");
        SecureRandom random = SecureRandomGeneration.getNonBlocking();
        return random.ints((int) lower, (int) upper)
                .findFirst()
                .getAsInt();
    }

    @Description("a cryptographically secure random number between lower and upper (exclusive)")
    @ScalarFunction(value = "secure_random", alias = "secure_rand", deterministic = false)
    @SqlType(StandardTypes.SMALLINT)
    public static long secureRandomSmallint(@SqlType(StandardTypes.SMALLINT) long lower, @SqlType(StandardTypes.SMALLINT) long upper)
    {
        checkCondition(lower < upper, INVALID_FUNCTION_ARGUMENT, "upper bound must be greater than lower bound");
        SecureRandom random = SecureRandomGeneration.getNonBlocking();
        return random.ints((int) lower, (int) upper)
                .findFirst()
                .getAsInt();
    }

    @Description("a cryptographically secure random number between lower and upper (exclusive)")
    @ScalarFunction(value = "secure_random", alias = "secure_rand", deterministic = false)
    @SqlType(StandardTypes.INTEGER)
    public static long secureRandomInteger(@SqlType(StandardTypes.INTEGER) long lower, @SqlType(StandardTypes.INTEGER) long upper)
    {
        checkCondition(lower < upper, INVALID_FUNCTION_ARGUMENT, "upper bound must be greater than lower bound");
        SecureRandom random = SecureRandomGeneration.getNonBlocking();
        return random.ints((int) lower, (int) upper)
                .findFirst()
                .getAsInt();
    }

    @Description("a cryptographically secure random number between lower and upper (exclusive)")
    @ScalarFunction(value = "secure_random", alias = "secure_rand", deterministic = false)
    @SqlType(StandardTypes.BIGINT)
    public static long secureRandomBigint(@SqlType(StandardTypes.BIGINT) long lower, @SqlType(StandardTypes.BIGINT) long upper)
    {
        checkCondition(lower < upper, INVALID_FUNCTION_ARGUMENT, "upper bound must be greater than lower bound");
        SecureRandom random = SecureRandomGeneration.getNonBlocking();
        return random.longs(lower, upper)
                .findFirst()
                .getAsLong();
    }

    @Description("Inverse of normal cdf given a mean, std, and probability")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double inverseNormalCdf(@SqlType(StandardTypes.DOUBLE) double mean, @SqlType(StandardTypes.DOUBLE) double sd, @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(p > 0 && p < 1, INVALID_FUNCTION_ARGUMENT, "inverseNormalCdf Function: p must be 0 > p > 1");
        checkCondition(sd > 0, INVALID_FUNCTION_ARGUMENT, "inverseNormalCdf Function: sd must be > 0");

        return mean + sd * 1.4142135623730951 * Erf.erfInv(2 * p - 1);
    }

    @Description("Normal cdf given a mean, standard deviation, and value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double normalCdf(
            @SqlType(StandardTypes.DOUBLE) double mean,
            @SqlType(StandardTypes.DOUBLE) double standardDeviation,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        checkCondition(standardDeviation > 0, INVALID_FUNCTION_ARGUMENT, "normalCdf Function: standardDeviation must be > 0");
        return 0.5 * (1 + Erf.erf((value - mean) / (standardDeviation * Math.sqrt(2))));
    }

    @Description("Inverse of Beta cdf given a, b parameters and probability")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double inverseBetaCdf(
            @SqlType(StandardTypes.DOUBLE) double a,
            @SqlType(StandardTypes.DOUBLE) double b,
            @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(p >= 0 && p <= 1, INVALID_FUNCTION_ARGUMENT, "inverseBetaCdf Function: p must be in the interval [0, 1]");
        checkCondition(a > 0, INVALID_FUNCTION_ARGUMENT, "inverseBetaCdf Function: a must be > 0");
        checkCondition(b > 0, INVALID_FUNCTION_ARGUMENT, "inverseBetaCdf Function: b must be > 0");
        BetaDistribution distribution = new BetaDistribution(null, a, b, BetaDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.inverseCumulativeProbability(p);
    }

    @Description("Beta cdf given the a, b parameters and value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double betaCdf(
            @SqlType(StandardTypes.DOUBLE) double a,
            @SqlType(StandardTypes.DOUBLE) double b,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        checkCondition(value >= 0 && value <= 1, INVALID_FUNCTION_ARGUMENT, "betaCdf Function: value must be in the interval [0, 1]");
        checkCondition(a > 0, INVALID_FUNCTION_ARGUMENT, "betaCdf Function: a must be > 0");
        checkCondition(b > 0, INVALID_FUNCTION_ARGUMENT, "betaCdf Function: b must be > 0");
        BetaDistribution distribution = new BetaDistribution(null, a, b, BetaDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.cumulativeProbability(value);
    }

    @Description("Inverse of Cauchy cdf for a given probability, median, and scale (gamma)")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double inverseCauchyCdf(
            @SqlType(StandardTypes.DOUBLE) double median,
            @SqlType(StandardTypes.DOUBLE) double scale,
            @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(p >= 0 && p <= 1, INVALID_FUNCTION_ARGUMENT, "inverseCauchyCdf Function: p must be in the interval [0, 1]");
        checkCondition(scale > 0, INVALID_FUNCTION_ARGUMENT, "inverseCauchyCdf Function: scale must be greater than 0");
        CauchyDistribution distribution = new CauchyDistribution(null, median, scale, CauchyDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.inverseCumulativeProbability(p);
    }

    @Description("Cauchy cdf for a given value, median, and scale (gamma)")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double cauchyCdf(
            @SqlType(StandardTypes.DOUBLE) double median,
            @SqlType(StandardTypes.DOUBLE) double scale,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        checkCondition(scale > 0, INVALID_FUNCTION_ARGUMENT, "cauchyCdf Function: scale must be greater than 0");
        CauchyDistribution distribution = new CauchyDistribution(null, median, scale, CauchyDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.cumulativeProbability(value);
    }

    @Description("Inverse of ChiSquared cdf given df parameter and probability")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double inverseChiSquaredCdf(
            @SqlType(StandardTypes.DOUBLE) double df,
            @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(p >= 0 && p <= 1, INVALID_FUNCTION_ARGUMENT, "inverseChiSquaredCdf Function: p must be in the interval [0, 1]");
        checkCondition(df > 0, INVALID_FUNCTION_ARGUMENT, "inverseChiSquaredCdf Function: df must be greater than 0");
        ChiSquaredDistribution distribution = new ChiSquaredDistribution(null, df, ChiSquaredDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.inverseCumulativeProbability(p);
    }

    @Description("ChiSquared cdf given the df parameter and value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double chiSquaredCdf(
            @SqlType(StandardTypes.DOUBLE) double df,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        checkCondition(value >= 0, INVALID_FUNCTION_ARGUMENT, "chiSquaredCdf Function: value must non-negative");
        checkCondition(df > 0, INVALID_FUNCTION_ARGUMENT, "chiSquaredCdf Function: df must be greater than 0");
        ChiSquaredDistribution distribution = new ChiSquaredDistribution(null, df, ChiSquaredDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.cumulativeProbability(value);
    }

    @Description("Inverse of F cdf given numerator degrees of freedom (df1), denominator degrees of freedom (df2) parameters, and probability")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double inverseFCdf(
            @SqlType(StandardTypes.DOUBLE) double df1,
            @SqlType(StandardTypes.DOUBLE) double df2,
            @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(p >= 0 && p <= 1, INVALID_FUNCTION_ARGUMENT, "inverseFCdf Function: p must be in the interval [0, 1]");
        checkCondition(df1 > 0, INVALID_FUNCTION_ARGUMENT, "inverseFCdf Function: numerator df must be greater than 0");
        checkCondition(df2 > 0, INVALID_FUNCTION_ARGUMENT, "inverseFCdf Function: denominator df must be greater than 0");
        FDistribution distribution = new FDistribution(null, df1, df2, FDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.inverseCumulativeProbability(p);
    }

    @Description("F cdf given the numerator degrees of freedom (df1), denominator degrees of freedom (df2) parameters, and value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double fCdf(
            @SqlType(StandardTypes.DOUBLE) double df1,
            @SqlType(StandardTypes.DOUBLE) double df2,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        checkCondition(value >= 0, INVALID_FUNCTION_ARGUMENT, "fCdf Function: value must non-negative");
        checkCondition(df1 > 0, INVALID_FUNCTION_ARGUMENT, "fCdf Function: numerator df must be greater than 0");
        checkCondition(df2 > 0, INVALID_FUNCTION_ARGUMENT, "fCdf Function: denominator df must be greater than 0");
        FDistribution distribution = new FDistribution(null, df1, df2, FDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.cumulativeProbability(value);
    }

    @Description("Inverse of Gamma cdf given shape and scale parameter and probability")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double inverseGammaCdf(
            @SqlType(StandardTypes.DOUBLE) double shape,
            @SqlType(StandardTypes.DOUBLE) double scale,
            @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(p >= 0 && p <= 1, INVALID_FUNCTION_ARGUMENT, "inverseGammaCdf Function: p must be in the interval [0, 1]");
        checkCondition(shape > 0, INVALID_FUNCTION_ARGUMENT, "inverseGammaCdf Function: shape must be greater than 0");
        checkCondition(scale > 0, INVALID_FUNCTION_ARGUMENT, "inverseGammaCdf Function: scale must be greater than 0");
        GammaDistribution distribution = new GammaDistribution(null, shape, scale, GammaDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.inverseCumulativeProbability(p);
    }

    @Description("Gamma cdf given the shape and scale parameter and value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double gammaCdf(
            @SqlType(StandardTypes.DOUBLE) double shape,
            @SqlType(StandardTypes.DOUBLE) double scale,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        checkCondition(value >= 0, INVALID_FUNCTION_ARGUMENT, "gammaCdf Function: value must be greater than, or equal to, 0");
        checkCondition(shape > 0, INVALID_FUNCTION_ARGUMENT, "gammaCdf Function: shape must be greater than 0");
        checkCondition(scale > 0, INVALID_FUNCTION_ARGUMENT, "gammaCdf Function: scale must be greater than 0");
        GammaDistribution distribution = new GammaDistribution(null, shape, scale, GammaDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.cumulativeProbability(value);
    }

    @Description("Inverse of Laplace cdf given mean, scale parameters and probability")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double inverseLaplaceCdf(
            @SqlType(StandardTypes.DOUBLE) double mean,
            @SqlType(StandardTypes.DOUBLE) double scale,
            @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(scale > 0, INVALID_FUNCTION_ARGUMENT, "inverseLaplaceCdf Function: scale must be greater than 0");
        checkCondition(p >= 0 && p <= 1, INVALID_FUNCTION_ARGUMENT, "inverseLaplaceCdf Function: p must be in the interval [0, 1]");
        LaplaceDistribution distribution = new LaplaceDistribution(null, mean, scale);
        return distribution.inverseCumulativeProbability(p);
    }

    @Description("Laplace cdf given mean, scale parameters and value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double laplaceCdf(
            @SqlType(StandardTypes.DOUBLE) double mean,
            @SqlType(StandardTypes.DOUBLE) double scale,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        checkCondition(scale > 0, INVALID_FUNCTION_ARGUMENT, "laplaceCdf Function: scale must be greater than 0");
        LaplaceDistribution distribution = new LaplaceDistribution(null, mean, scale);
        return distribution.cumulativeProbability(value);
    }

    @Description("Inverse of Poisson cdf given lambda (mean) parameter and probability")
    @ScalarFunction
    @SqlType(StandardTypes.INTEGER)
    public static long inversePoissonCdf(
            @SqlType(StandardTypes.DOUBLE) double lambda,
            @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(p >= 0 && p < 1, INVALID_FUNCTION_ARGUMENT, "inversePoissonCdf Function: p must be in the interval [0, 1)");
        checkCondition(lambda > 0, INVALID_FUNCTION_ARGUMENT, "inversePoissonCdf Function: lambda must be greater than 0");
        PoissonDistribution distribution = new PoissonDistribution(lambda);
        return distribution.inverseCumulativeProbability(p);
    }

    @Description("Poisson cdf given the lambda (mean) parameter and value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double poissonCdf(
            @SqlType(StandardTypes.DOUBLE) double lambda,
            @SqlType(StandardTypes.INTEGER) long value)
    {
        checkCondition(value >= 0, INVALID_FUNCTION_ARGUMENT, "poissonCdf Function: value must be a non-negative integer");
        checkCondition(lambda > 0, INVALID_FUNCTION_ARGUMENT, "poissonCdf Function: lambda must be greater than 0");
        PoissonDistribution distribution = new PoissonDistribution(lambda);
        return distribution.cumulativeProbability((int) value);
    }

    @Description("Inverse of Weibull cdf given a, b parameters and probability")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double inverseWeibullCdf(
            @SqlType(StandardTypes.DOUBLE) double a,
            @SqlType(StandardTypes.DOUBLE) double b,
            @SqlType(StandardTypes.DOUBLE) double p)
    {
        checkCondition(p >= 0 && p <= 1, INVALID_FUNCTION_ARGUMENT, "inverseWeibullCdf Function: p must be in the interval [0, 1]");
        checkCondition(a > 0, INVALID_FUNCTION_ARGUMENT, "inverseWeibullCdf Function: a must be greater than 0");
        checkCondition(b > 0, INVALID_FUNCTION_ARGUMENT, "inverseWeibullCdf Function: b must be greater than 0");
        WeibullDistribution distribution = new WeibullDistribution(null, a, b, WeibullDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.inverseCumulativeProbability(p);
    }

    @Description("Weibull cdf given the a, b parameters and value")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double weibullCdf(
            @SqlType(StandardTypes.DOUBLE) double a,
            @SqlType(StandardTypes.DOUBLE) double b,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        checkCondition(a > 0, INVALID_FUNCTION_ARGUMENT, "weibullCdf Function: a must be greater than 0");
        checkCondition(b > 0, INVALID_FUNCTION_ARGUMENT, "weibullCdf Function: b must be greater than 0");
        WeibullDistribution distribution = new WeibullDistribution(null, a, b, WeibullDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
        return distribution.cumulativeProbability(value);
    }

    @Description("round to nearest integer")
    @ScalarFunction("round")
    @SqlType(StandardTypes.TINYINT)
    public static long roundTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction("round")
    @SqlType(StandardTypes.SMALLINT)
    public static long roundSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction("round")
    @SqlType(StandardTypes.INTEGER)
    public static long roundInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long round(@SqlType(StandardTypes.BIGINT) long num)
    {
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction("round")
    @SqlType(StandardTypes.TINYINT)
    public static long roundTinyint(@SqlType(StandardTypes.TINYINT) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        // TODO implement support for `decimals < 0`
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction("round")
    @SqlType(StandardTypes.SMALLINT)
    public static long roundSmallint(@SqlType(StandardTypes.SMALLINT) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        // TODO implement support for `decimals < 0`
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction("round")
    @SqlType(StandardTypes.INTEGER)
    public static long roundInteger(@SqlType(StandardTypes.INTEGER) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        // TODO implement support for `decimals < 0`
        return num;
    }

    @Description("round to nearest integer")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long round(@SqlType(StandardTypes.BIGINT) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        // TODO implement support for `decimals < 0`
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
    @ScalarFunction("round")
    @SqlType(StandardTypes.REAL)
    public static long roundFloat(@SqlType(StandardTypes.REAL) long num)
    {
        return roundFloat(num, 0);
    }

    @Description("round to given number of decimal places")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double round(@SqlType(StandardTypes.DOUBLE) double num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        if (Double.isNaN(num) || Double.isInfinite(num)) {
            return num;
        }

        double factor = Math.pow(10, decimals);
        try {
            if (num < 0) {
                return -(roundToLong(-num * factor, HALF_UP) / factor);
            }
            return roundToLong(num * factor, HALF_UP) / factor;
        }
        catch (ArithmeticException e) {
            // Use BigDecimal if the value is out of the range of long.
            BigDecimal bigDecimal = new BigDecimal(num);
            return bigDecimal.setScale((int) decimals, HALF_UP).doubleValue();
        }
    }

    @Description("round to given number of decimal places")
    @ScalarFunction("round")
    @SqlType(StandardTypes.REAL)
    public static long roundFloat(@SqlType(StandardTypes.REAL) long num, @SqlType(StandardTypes.INTEGER) long decimals)
    {
        float numInFloat = intBitsToFloat((int) num);
        if (Float.isNaN(numInFloat) || Float.isInfinite(numInFloat)) {
            return num;
        }

        double factor = Math.pow(10, decimals);
        try {
            if (numInFloat < 0) {
                return floatToRawIntBits((float) -(roundToLong(-numInFloat * factor, HALF_UP) / factor));
            }
            return floatToRawIntBits((float) (roundToLong(numInFloat * factor, HALF_UP) / factor));
        }
        catch (ArithmeticException e) {
            // Use BigDecimal if the value is out of the range of long.
            BigDecimal bigDecimal = new BigDecimal(numInFloat);
            return floatToRawIntBits(bigDecimal.setScale((int) decimals, HALF_UP).floatValue());
        }
    }

    @ScalarFunction("round")
    @Description("round to nearest integer")
    public static final class Round
    {
        private Round() {}

        @LiteralParameters({"p", "s", "rp", "rs"})
        @SqlType("decimal(rp, rs)")
        @Constraint(variable = "rp", expression = "min(38, p - s + min(1, s))")
        @Constraint(variable = "rs", expression = "0")
        public static long roundShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") long num)
        {
            if (num == 0) {
                return 0;
            }
            if (numScale == 0) {
                return num;
            }
            if (num < 0) {
                return -roundShort(numScale, -num);
            }

            long rescaleFactor = Decimals.longTenToNth((int) numScale);
            long remainder = num % rescaleFactor;
            long remainderBoundary = rescaleFactor / 2;
            int roundUp = remainder >= remainderBoundary ? 1 : 0;
            return num / rescaleFactor + roundUp;
        }

        @LiteralParameters({"p", "s", "rp", "rs"})
        @SqlType("decimal(rp, rs)")
        @Constraint(variable = "rp", expression = "min(38, p - s + min(1, s))")
        @Constraint(variable = "rs", expression = "0")
        public static Slice roundLongLong(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Slice num)
        {
            if (numScale == 0) {
                return num;
            }
            return rescale(num, -(int) numScale);
        }

        @LiteralParameters({"p", "s", "rp", "rs"})
        @SqlType("decimal(rp, rs)")
        @Constraint(variable = "rp", expression = "min(38, p - s + min(1, s))")
        @Constraint(variable = "rs", expression = "0")
        public static long roundLongShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Slice num)
        {
            return unscaledDecimalToUnscaledLong(rescale(num, -(int) numScale));
        }
    }

    @ScalarFunction("round")
    @Description("round to given number of decimal places")
    public static final class RoundN
    {
        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp, s)")
        @Constraint(variable = "rp", expression = "min(38, p + 1)")
        public static long roundNShort(
                @LiteralParameter("p") long numPrecision,
                @LiteralParameter("s") long numScale,
                @SqlType("decimal(p, s)") long num,
                @SqlType(StandardTypes.INTEGER) long decimals)
        {
            if (num == 0 || numPrecision - numScale + decimals <= 0) {
                return 0;
            }
            if (decimals >= numScale) {
                return num;
            }
            if (num < 0) {
                return -roundNShort(numPrecision, numScale, -num, decimals);
            }

            long rescaleFactor = longTenToNth((int) (numScale - decimals));
            long remainder = num % rescaleFactor;
            int roundUp = (remainder >= rescaleFactor / 2) ? 1 : 0;
            return (num / rescaleFactor + roundUp) * rescaleFactor;
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp, s)")
        @Constraint(variable = "rp", expression = "min(38, p + 1)")
        public static Slice roundNLong(
                @LiteralParameter("s") long numScale,
                @LiteralParameter("rp") long resultPrecision,
                @SqlType("decimal(p, s)") Slice num,
                @SqlType(StandardTypes.INTEGER) long decimals)
        {
            if (decimals >= numScale) {
                return num;
            }
            int rescaleFactor = ((int) numScale) - (int) decimals;
            try {
                Slice result = rescale(rescale(num, -rescaleFactor), rescaleFactor);
                throwIfOverflows(result, ((int) resultPrecision));
                return result;
            }
            catch (ArithmeticException e) {
                throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "decimal overflow: " + num, e);
            }
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp, s)")
        @Constraint(variable = "rp", expression = "min(38, p + 1)")
        public static Slice roundNShortLong(
                @LiteralParameter("s") long numScale,
                @LiteralParameter("rp") long resultPrecision,
                @SqlType("decimal(p, s)") long num,
                @SqlType(StandardTypes.INTEGER) long decimals)
        {
            return roundNLong(numScale, resultPrecision, unscaledDecimal(num), decimals);
        }
    }

    @ScalarFunction("truncate")
    @Description("round to integer by dropping digits after decimal point")
    public static final class Truncate
    {
        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "max(1, p - s)")
        public static long truncateShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") long num)
        {
            if (num == 0) {
                return 0;
            }
            if (numScale == 0) {
                return num;
            }

            long rescaleFactor = Decimals.longTenToNth((int) numScale);
            return num / rescaleFactor;
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "max(1, p - s)")
        public static Slice truncateLong(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Slice num)
        {
            if (numScale == 0) {
                return num;
            }
            return rescaleTruncate(num, -(int) numScale);
        }

        @LiteralParameters({"p", "s", "rp"})
        @SqlType("decimal(rp,0)")
        @Constraint(variable = "rp", expression = "max(1, p - s)")
        public static long truncateLongShort(@LiteralParameter("s") long numScale, @SqlType("decimal(p, s)") Slice num)
        {
            return unscaledDecimalToUnscaledLong(rescaleTruncate(num, -(int) numScale));
        }
    }

    @ScalarFunction("truncate")
    @Description("round to integer by dropping given number of digits after decimal point")
    public static final class TruncateN
    {
        private TruncateN() {}

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static long truncateShort(
                @LiteralParameter("p") long numPrecision,
                @LiteralParameter("s") long numScale,
                @SqlType("decimal(p, s)") long num,
                @SqlType(StandardTypes.INTEGER) long roundScale)
        {
            if (num == 0 || numPrecision - numScale + roundScale <= 0) {
                return 0;
            }
            if (roundScale >= numScale) {
                return num;
            }

            long rescaleFactor = longTenToNth((int) (numScale - roundScale));
            long remainder = num % rescaleFactor;
            return num - remainder;
        }

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static Slice truncateLong(
                @LiteralParameter("p") long numPrecision,
                @LiteralParameter("s") long numScale,
                @SqlType("decimal(p, s)") Slice num,
                @SqlType(StandardTypes.INTEGER) long roundScale)
        {
            if (numPrecision - numScale + roundScale <= 0) {
                return unscaledDecimal(0);
            }
            if (roundScale >= numScale) {
                return num;
            }
            int rescaleFactor = (int) (numScale - roundScale);
            return rescaleTruncate(rescaleTruncate(num, -rescaleFactor), rescaleFactor);
        }
    }

    @Description("signum")
    @ScalarFunction("sign")
    public static final class Sign
    {
        private Sign() {}

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(1,0)")
        public static long signDecimalShort(@SqlType("decimal(p, s)") long num)
        {
            return (long) Math.signum(num);
        }

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(1,0)")
        public static long signDecimalLong(@SqlType("decimal(p, s)") Slice num)
        {
            if (isZero(num)) {
                return 0;
            }
            else if (isNegative(num)) {
                return -1;
            }
            else {
                return 1;
            }
        }
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long sign(@SqlType(StandardTypes.BIGINT) long num)
    {
        return (long) Math.signum(num);
    }

    @Description("signum")
    @ScalarFunction("sign")
    @SqlType(StandardTypes.INTEGER)
    public static long signInteger(@SqlType(StandardTypes.INTEGER) long num)
    {
        return (long) Math.signum(num);
    }

    @Description("signum")
    @ScalarFunction("sign")
    @SqlType(StandardTypes.SMALLINT)
    public static long signSmallint(@SqlType(StandardTypes.SMALLINT) long num)
    {
        return (long) Math.signum(num);
    }

    @Description("signum")
    @ScalarFunction("sign")
    @SqlType(StandardTypes.TINYINT)
    public static long signTinyint(@SqlType(StandardTypes.TINYINT) long num)
    {
        return (long) Math.signum(num);
    }

    @Description("signum")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double sign(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.signum(num);
    }

    @Description("signum")
    @ScalarFunction("sign")
    @SqlType(StandardTypes.REAL)
    public static long signFloat(@SqlType(StandardTypes.REAL) long num)
    {
        return floatToRawIntBits((Math.signum(intBitsToFloat((int) num))));
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
    @SqlType("varchar(64)")
    public static Slice toBase(@SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long radix)
    {
        checkRadix(radix);
        return utf8Slice(Long.toString(value, (int) radix));
    }

    @Description("convert a string in the given base to a number")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long fromBase(@SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.BIGINT) long radix)
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

    @Description("The bucket number of a value given a lower and upper bound and the number of buckets")
    @ScalarFunction("width_bucket")
    @SqlType(StandardTypes.BIGINT)
    public static long widthBucket(@SqlType(StandardTypes.DOUBLE) double operand, @SqlType(StandardTypes.DOUBLE) double bound1, @SqlType(StandardTypes.DOUBLE) double bound2, @SqlType(StandardTypes.BIGINT) long bucketCount)
    {
        checkCondition(bucketCount > 0, INVALID_FUNCTION_ARGUMENT, "bucketCount must be greater than 0");
        checkCondition(!isNaN(operand), INVALID_FUNCTION_ARGUMENT, "operand must not be NaN");
        checkCondition(isFinite(bound1), INVALID_FUNCTION_ARGUMENT, "first bound must be finite");
        checkCondition(isFinite(bound2), INVALID_FUNCTION_ARGUMENT, "second bound must be finite");
        checkCondition(bound1 != bound2, INVALID_FUNCTION_ARGUMENT, "bounds cannot equal each other");

        long result = 0;

        double lower = Math.min(bound1, bound2);
        double upper = Math.max(bound1, bound2);

        if (operand < lower) {
            result = 0;
        }
        else if (operand >= upper) {
            try {
                result = Math.addExact(bucketCount, 1);
            }
            catch (ArithmeticException e) {
                throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Bucket for value %s is out of range", operand));
            }
        }
        else {
            result = (long) ((double) bucketCount * (operand - lower) / (upper - lower) + 1);
        }

        if (bound1 > bound2) {
            result = (bucketCount - result) + 1;
        }

        return result;
    }

    @Description("The bucket number of a value given an array of bins")
    @ScalarFunction("width_bucket")
    @SqlType(StandardTypes.BIGINT)
    public static long widthBucket(@SqlType(StandardTypes.DOUBLE) double operand, @SqlType("array(double)") Block bins)
    {
        int numberOfBins = bins.getPositionCount();

        checkCondition(numberOfBins > 0, INVALID_FUNCTION_ARGUMENT, "Bins cannot be an empty array");
        checkCondition(!isNaN(operand), INVALID_FUNCTION_ARGUMENT, "Operand cannot be NaN");

        int lower = 0;
        int upper = numberOfBins;

        int index;
        double bin;
        double lowerBin;
        double upperBin;

        while (lower < upper) {
            index = (lower + upper) / 2;
            if (bins.isNull(lower) || bins.isNull(index) || bins.isNull(upper - 1)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Bin values cannot be NULL");
            }

            bin = DOUBLE.getDouble(bins, index);
            lowerBin = DOUBLE.getDouble(bins, lower);
            upperBin = DOUBLE.getDouble(bins, upper - 1);
            if (lowerBin > upperBin || lowerBin > bin || bin > upperBin) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Bin values are not sorted in ascending order");
            }
            if (!isFinite(bin) || !isFinite(lowerBin) || !isFinite(upperBin)) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Bin values must be finite");
            }

            if (operand < bin) {
                upper = index;
            }
            else {
                lower = index + 1;
            }
        }

        return lower;
    }

    @Description("cosine similarity between the given sparse vectors represented as maps")
    @ScalarFunction("cosine_similarity")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double mapCosineSimilarity(@SqlType("map(varchar,double)") Block leftMap, @SqlType("map(varchar,double)") Block rightMap)
    {
        Double normLeftMap = mapL2Norm(leftMap);
        Double normRightMap = mapL2Norm(rightMap);

        if (normLeftMap == null || normRightMap == null) {
            return null;
        }

        double dotProduct = mapDotProduct(leftMap, rightMap);

        return dotProduct / (normLeftMap * normRightMap);
    }

    @Description("cosine similarity between the given identical sized vectors represented as arrays")
    @ScalarFunction("cosine_similarity")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double arrayCosineSimilarity(@SqlType("array(double)") Block leftArray, @SqlType("array(double)") Block rightArray)
    {
        checkCondition(
                leftArray.getPositionCount() == rightArray.getPositionCount(),
                INVALID_FUNCTION_ARGUMENT,
                "Both array arguments need to have identical size");

        checkCondition(
                !(leftArray.mayHaveNull() || rightArray.mayHaveNull()),
                INVALID_FUNCTION_ARGUMENT,
                "Both arrays must not have nulls");

        Double normLeftArray = array2Norm(leftArray);
        Double normRightArray = array2Norm(rightArray);

        if (normLeftArray == null || normRightArray == null) {
            return null;
        }

        double dotProduct = arrayDotProduct(leftArray, rightArray);

        return dotProduct / (normLeftArray * normRightArray);
    }

    @Description("squared Euclidean distance between the given identical sized vectors represented as arrays")
    @ScalarFunction("l2_squared")
    @SqlType(StandardTypes.REAL)
    public static long arrayL2Squared(@SqlType("array(real)") Block leftArray, @SqlType("array(real)") Block rightArray)
    {
        checkCondition(
                leftArray.getPositionCount() == rightArray.getPositionCount(),
                INVALID_FUNCTION_ARGUMENT,
                "Both array arguments need to have identical size");

        checkCondition(
                !(leftArray.mayHaveNull() || rightArray.mayHaveNull()),
                INVALID_FUNCTION_ARGUMENT,
                "Both arrays must not have nulls");

        float sum = 0.0f;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            float left = intBitsToFloat((int) leftArray.getInt(i));
            float right = intBitsToFloat((int) rightArray.getInt(i));
            float diff = left - right;
            sum += diff * diff;
        }

        return floatToRawIntBits(sum);
    }

    @Description("squared Euclidean distance between the given identical sized vectors represented as arrays")
    @ScalarFunction("l2_squared")
    @SqlType(StandardTypes.DOUBLE)
    public static double arrayL2SquaredDouble(
            @SqlType("array(double)") Block leftArray,
            @SqlType("array(double)") Block rightArray)
    {
        checkCondition(
                leftArray.getPositionCount() == rightArray.getPositionCount(),
                INVALID_FUNCTION_ARGUMENT,
                "Both array arguments need to have identical size");

        checkCondition(
                !(leftArray.mayHaveNull() || rightArray.mayHaveNull()),
                INVALID_FUNCTION_ARGUMENT,
                "Both arrays must not have nulls");

        double sum = 0.0;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            double left = DOUBLE.getDouble(leftArray, i);
            double right = DOUBLE.getDouble(rightArray, i);
            double diff = left - right;
            sum += diff * diff;
        }
        return sum;
    }

    @Description("Dot Product distance between the given identical sized vectors represented as DOUBLE arrays")
    @ScalarFunction("dot_product")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double arrayDotProduct(
            @SqlType("array(double)") Block leftArray,
            @SqlType("array(double)") Block rightArray)
    {
        checkCondition(
                leftArray.getPositionCount() == rightArray.getPositionCount(),
                INVALID_FUNCTION_ARGUMENT,
                "Both array arguments must have identical sizes");

        checkCondition(
                !(leftArray.mayHaveNull() || rightArray.mayHaveNull()),
                INVALID_FUNCTION_ARGUMENT,
                "Both array arguments must not have nulls");

        double result = 0.0;

        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            result += DOUBLE.getDouble(leftArray, i) * DOUBLE.getDouble(rightArray, i);
        }

        return result;
    }

    @Description("Dot Product distance between the given identical sized vectors represented as REAL arrays")
    @ScalarFunction("dot_product")
    @SqlNullable
    @SqlType(StandardTypes.REAL)
    public static Long arrayDotProductReal(
            @SqlType("array(real)") Block leftArray,
            @SqlType("array(real)") Block rightArray)
    {
        checkCondition(
                leftArray.getPositionCount() == rightArray.getPositionCount(),
                INVALID_FUNCTION_ARGUMENT,
                "Both array arguments must have identical sizes");

        checkCondition(
                !(leftArray.mayHaveNull() || rightArray.mayHaveNull()),
                INVALID_FUNCTION_ARGUMENT,
                "Both array arguments must not have nulls");

        float dotProduct = 0.0F;

        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            dotProduct += intBitsToFloat((int) REAL.getLong(leftArray, i)) * Float.intBitsToFloat((int) REAL.getLong(rightArray, i));
        }

        return ((long) floatToRawIntBits(dotProduct));
    }

    private static double mapDotProduct(Block leftMap, Block rightMap)
    {
        TypedSet rightMapKeys = new TypedSet(VARCHAR, rightMap.getPositionCount(), "cosine_similarity");

        for (int i = 0; i < rightMap.getPositionCount(); i += 2) {
            rightMapKeys.add(rightMap, i);
        }

        double result = 0.0;

        for (int i = 0; i < leftMap.getPositionCount(); i += 2) {
            int position = rightMapKeys.positionOf(leftMap, i);

            if (position != -1) {
                result += DOUBLE.getDouble(leftMap, i + 1) *
                        DOUBLE.getDouble(rightMap, 2 * position + 1);
            }
        }

        return result;
    }

    private static Double mapL2Norm(Block map)
    {
        double norm = 0.0;

        for (int i = 1; i < map.getPositionCount(); i += 2) {
            if (map.isNull(i)) {
                return null;
            }
            norm += DOUBLE.getDouble(map, i) * DOUBLE.getDouble(map, i);
        }

        return Math.sqrt(norm);
    }

    private static Double array2Norm(Block array)
    {
        double norm = 0.0;
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                return null;
            }
            norm += DOUBLE.getDouble(array, i) * DOUBLE.getDouble(array, i);
        }

        return Math.sqrt(norm);
    }

    @Description("factorial of a given integer in the range of 0 to 20")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long factorial(@SqlType(StandardTypes.INTEGER) long x)
    {
        checkCondition(
                x >= 0,
                INVALID_FUNCTION_ARGUMENT,
                "The factorial function is only defined for non-negative integers");

        checkCondition(
                x <= 20,
                INVALID_FUNCTION_ARGUMENT,
                "The output of the factorial function would overflow for any input over 20");

        long result = 1;
        for (int i = 2; i <= x; i++) {
            result *= i;
        }

        return result;
    }
}
