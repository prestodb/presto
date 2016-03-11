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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.SqlScalarFunctionBuilder;
import com.facebook.presto.metadata.SqlScalarFunctionBuilder.SpecializeContext;
import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.DIVIDE;
import static com.facebook.presto.metadata.OperatorType.MODULUS;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorType.NEGATION;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static com.facebook.presto.metadata.Signature.longVariableExpression;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.type.Decimals.bigIntegerTenToNth;
import static com.facebook.presto.spi.type.Decimals.checkOverflow;
import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static java.lang.Integer.max;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.TEN;
import static java.math.BigInteger.ZERO;

public final class DecimalOperators
{
    public static final SqlScalarFunction DECIMAL_MULTIPLY_OPERATOR = decimalMultiplyOperator();
    public static final SqlScalarFunction DECIMAL_DIVIDE_OPERATOR = decimalDivideOperator();
    public static final SqlScalarFunction DECIMAL_MODULUS_OPERATOR = decimalModulusOperator();

    private DecimalOperators()
    {
    }

    @ScalarOperator(ADD)
    public static final class Add
    {
        public Add() {}

        @UsedByGeneratedCode
        @LiteralParameters({"p", "s", "p2"})
        @Constraint(variable = "p2", expression = "min(38, p + 1)")
        @SqlType("decimal(p2, s)")
        public long addShortShortShort(@SqlType("decimal(p, s)") long a, @SqlType("decimal(p, s)") long b)
        {
            return a + b;
        }

        @UsedByGeneratedCode
        @LiteralParameters({"p", "s", "p2"})
        @Constraint(variable = "p2", expression = "min(38, p + 1)")
        @SqlType("decimal(p2, s)")
        public Slice addShortShortLong(@SqlType("decimal(p, s)") long a, @SqlType("decimal(p, s)") long b)
        {
            BigInteger result = BigInteger.valueOf(a).add(BigInteger.valueOf(b));
            return Decimals.encodeUnscaledValue(result);
        }

        @UsedByGeneratedCode
        @LiteralParameters({"p", "s", "p2"})
        @Constraint(variable = "p2", expression = "min(38, p + 1)")
        @SqlType("decimal(p2, s)")
        public Slice addLongLongLong(@SqlType("decimal(p, s)") Slice a, @SqlType("decimal(p, s)") Slice b)
        {
            BigInteger aBigInteger = Decimals.decodeUnscaledValue(a);
            BigInteger bBigInteger = Decimals.decodeUnscaledValue(b);
            BigInteger result = aBigInteger.add(bBigInteger);
            checkOverflow(result);
            return Decimals.encodeUnscaledValue(result);
        }
    }

    @ScalarOperator(SUBTRACT)
    public static final class Subtract
    {
        @UsedByGeneratedCode
        @LiteralParameters({"p", "s", "p2"})
        @Constraint(variable = "p2", expression = "min(38, p + 1)")
        @SqlType("decimal(p2, s)")
        public static long subtractShortShortShort(@SqlType("decimal(p, s)") long a, @SqlType("decimal(p, s)") long b)
        {
            return a - b;
        }

        @UsedByGeneratedCode
        @LiteralParameters({"p", "s", "p2"})
        @Constraint(variable = "p2", expression = "min(38, p + 1)")
        @SqlType("decimal(p2, s)")
        public static Slice subtractShortShortLong(@SqlType("decimal(p, s)") long a, @SqlType("decimal(p, s)") long b)
        {
            BigInteger result = BigInteger.valueOf(a).subtract(BigInteger.valueOf(b));
            return Decimals.encodeUnscaledValue(result);
        }

        @UsedByGeneratedCode
        @LiteralParameters({"p", "s", "p2"})
        @Constraint(variable = "p2", expression = "min(38, p + 1)")
        @SqlType("decimal(p2, s)")
        public static Slice subtractLongLongLong(@SqlType("decimal(p, s)") Slice a, @SqlType("decimal(p, s)") Slice b)
        {
            BigInteger aBigInteger = Decimals.decodeUnscaledValue(a);
            BigInteger bBigInteger = Decimals.decodeUnscaledValue(b);
            BigInteger result = aBigInteger.subtract(bBigInteger);
            checkOverflow(result);
            return Decimals.encodeUnscaledValue(result);
        }
    }

    private static SqlScalarFunction decimalMultiplyOperator()
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(MULTIPLY)
                .literalParameters("a_precision", "a_scale", "b_precision", "b_scale", "r_precision", "r_scale")
                .longVariableConstraints(
                        longVariableExpression("r_precision", "min(38, a_precision + b_precision)"),
                        longVariableExpression("r_scale", "a_scale + b_scale")
                )
                .argumentTypes("decimal(a_precision, a_scale)", "decimal(b_precision, b_scale)")
                .returnType("decimal(r_precision, r_scale)")
                .build();
        return SqlScalarFunction.builder(DecimalOperators.class)
                .signature(signature)
                .implementation(b -> b.methods("multiplyShortShortShort", "multiplyShortShortLong", "multiplyLongLongLong", "multiplyShortLongLong", "multiplyLongShortLong"))
                .build();
    }

    @UsedByGeneratedCode
    public static long multiplyShortShortShort(long a, long b)
    {
        return a * b;
    }

    @UsedByGeneratedCode
    public static Slice multiplyShortShortLong(long a, long b)
    {
        BigInteger aBigInteger = BigInteger.valueOf(a);
        BigInteger bBigInteger = BigInteger.valueOf(b);
        return internalMultiplyLongLongLong(aBigInteger, bBigInteger);
    }

    @UsedByGeneratedCode
    public static Slice multiplyLongLongLong(Slice a, Slice b)
    {
        BigInteger aBigInteger = Decimals.decodeUnscaledValue(a);
        BigInteger bBigInteger = Decimals.decodeUnscaledValue(b);
        return internalMultiplyLongLongLong(aBigInteger, bBigInteger);
    }

    @UsedByGeneratedCode
    public static Slice multiplyShortLongLong(long a, Slice b)
    {
        BigInteger aBigInteger = BigInteger.valueOf(a);
        BigInteger bBigInteger = Decimals.decodeUnscaledValue(b);
        return internalMultiplyLongLongLong(aBigInteger, bBigInteger);
    }

    @UsedByGeneratedCode
    public static Slice multiplyLongShortLong(Slice a, long b)
    {
        BigInteger aBigInteger = Decimals.decodeUnscaledValue(a);
        BigInteger bBigInteger = BigInteger.valueOf(b);
        return internalMultiplyLongLongLong(aBigInteger, bBigInteger);
    }

    private static Slice internalMultiplyLongLongLong(BigInteger aBigInteger, BigInteger bBigInteger)
    {
        BigInteger result = aBigInteger.multiply(bBigInteger);
        checkOverflow(result);
        return Decimals.encodeUnscaledValue(result);
    }

    private static SqlScalarFunction decimalDivideOperator()
    {
        // we extend target precision by b_scale. This is upper bound on how much division result will grow.
        // pessimistic case is a / 0.0000001
        // if scale of divisor is greater than scale of dividend we extend scale further as we
        // want result scale to be maximum of scales of divisor and dividend.
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(DIVIDE)
                .literalParameters("a_precision", "a_scale", "b_precision", "b_scale", "r_precision", "r_scale")
                .longVariableConstraints(
                        longVariableExpression("r_precision", "min(38, a_precision + b_scale + max(b_scale - a_scale, 0))"),
                        longVariableExpression("r_scale", "max(a_scale, b_scale)")
                )
                .argumentTypes("decimal(a_precision, a_scale)", "decimal(b_precision, b_scale)")
                .returnType("decimal(r_precision, r_scale)")
                .build();
        return SqlScalarFunction.builder(DecimalOperators.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("divideShortShortShort")
                        .withExtraParameters(DecimalOperators::shortDivideRescaleExtraParameter)
                )
                .implementation(b -> b
                        .methods("divideShortShortLong", "divideLongLongLong", "divideShortLongLong", "divideLongShortLong")
                        .withExtraParameters(DecimalOperators::longDivideRescaleExtraParameter)
                )
                .build();
    }

    private static List<Object> shortDivideRescaleExtraParameter(SpecializeContext context)
    {
        int rescaleFactor = divideRescaleFactor(context);
        return ImmutableList.of(longTenToNth(rescaleFactor));
    }

    private static List<Object> longDivideRescaleExtraParameter(SpecializeContext context)
    {
        int rescaleFactor = divideRescaleFactor(context);
        return ImmutableList.of(bigIntegerTenToNth(rescaleFactor));
    }

    private static int divideRescaleFactor(SqlScalarFunctionBuilder.SpecializeContext context)
    {
        DecimalType returnType = (DecimalType) context.getReturnType();
        // +1 because we want to do computations with one extra decimal field to be able to handle rounding of the result.
        return (int) (returnType.getScale() - context.getLiteral("a_scale") + context.getLiteral("b_scale") + 1);
    }

    @UsedByGeneratedCode
    public static long divideShortShortShort(long a, long b, long aRescale)
    {
        try {
            long result = a * aRescale / b;
            if (result > 0) {
                if (result % 10 >= 5) {
                    return result / 10 + 1;
                }
                else {
                    return result / 10;
                }
            }
            else {
                if (result % 10 <= -5) {
                    return result / 10 - 1;
                }
                else {
                    return result / 10;
                }
            }
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    @UsedByGeneratedCode
    public static Slice divideShortShortLong(long a, long b, BigInteger aRescale)
    {
        BigInteger aBigInteger = BigInteger.valueOf(a).multiply(aRescale);
        BigInteger bBigInteger = BigInteger.valueOf(b);
        return internalDivideLongLongLong(aBigInteger, bBigInteger);
    }

    @UsedByGeneratedCode
    public static Slice divideLongLongLong(Slice a, Slice b, BigInteger aRescale)
    {
        BigInteger aBigInteger = Decimals.decodeUnscaledValue(a).multiply(aRescale);
        BigInteger bBigInteger = Decimals.decodeUnscaledValue(b);
        return internalDivideLongLongLong(aBigInteger, bBigInteger);
    }

    @UsedByGeneratedCode
    public static Slice divideShortLongLong(long a, Slice b, BigInteger aRescale)
    {
        BigInteger aBigInteger = BigInteger.valueOf(a).multiply(aRescale);
        BigInteger bBigInteger = Decimals.decodeUnscaledValue(b);
        return internalDivideLongLongLong(aBigInteger, bBigInteger);
    }

    @UsedByGeneratedCode
    public static Slice divideLongShortLong(Slice a, long b, BigInteger aRescale)
    {
        BigInteger aBigInteger = Decimals.decodeUnscaledValue(a).multiply(aRescale);
        BigInteger bBigInteger = BigInteger.valueOf(b);
        return internalDivideLongLongLong(aBigInteger, bBigInteger);
    }

    private static Slice internalDivideLongLongLong(BigInteger aBigInteger, BigInteger bBigInteger)
    {
        try {
            BigInteger result = aBigInteger.divide(bBigInteger);
            BigInteger resultModTen = result.mod(TEN);
            if (result.signum() > 0) {
                if (resultModTen.compareTo(BigInteger.valueOf(5)) >= 0) {
                    result = result.divide(TEN).add(ONE);
                }
                else {
                    result = result.divide(TEN);
                }
            }
            else {
                if (resultModTen.compareTo(BigInteger.valueOf(5)) < 0 && !resultModTen.equals(ZERO)) {
                    result = result.divide(TEN).subtract(ONE);
                }
                else {
                    result = result.divide(TEN);
                }
            }
            checkOverflow(result);
            return Decimals.encodeUnscaledValue(result);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    private static SqlScalarFunction decimalModulusOperator()
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(MODULUS)
                .literalParameters("a_precision", "a_scale", "b_precision", "b_scale", "r_precision", "r_scale")
                .longVariableConstraints(
                        longVariableExpression("r_precision", "min(b_precision - b_scale, a_precision - a_scale) + max(a_scale, b_scale)"),
                        longVariableExpression("r_scale", "max(a_scale, b_scale)")
                )
                .argumentTypes("decimal(a_precision, a_scale)", "decimal(b_precision, b_scale)")
                .returnType("decimal(r_precision, r_scale)")
                .build();
        return SqlScalarFunction.builder(DecimalOperators.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("modulusShortShortShort", "modulusLongLongLong", "modulusShortLongLong", "modulusShortLongShort", "modulusLongShortShort", "modulusLongShortLong")
                        .withExtraParameters(DecimalOperators::longRescaleExtraParameters)
                )
                .build();
    }

    private static List<Object> longRescaleExtraParameters(SpecializeContext context)
    {
        BigInteger aRescale = bigIntegerTenToNth(rescaleFactor(context.getLiteral("a_scale"), context.getLiteral("b_scale")));
        BigInteger bRescale = bigIntegerTenToNth(rescaleFactor(context.getLiteral("b_scale"), context.getLiteral("a_scale")));
        return ImmutableList.of(aRescale, bRescale);
    }

    private static int rescaleFactor(long fromScale, long toScale)
    {
        return max(0, (int) toScale - (int) fromScale);
    }

    @UsedByGeneratedCode
    public static long modulusShortShortShort(long a, long b, BigInteger aRescale, BigInteger bRescale)
    {
        BigInteger aBigInteger = BigInteger.valueOf(a);
        BigInteger bBigInteger = BigInteger.valueOf(b);
        return internalModulusShortResult(aBigInteger, bBigInteger, aRescale, bRescale);
    }

    @UsedByGeneratedCode
    public static Slice modulusLongLongLong(Slice a, Slice b, BigInteger aRescale, BigInteger bRescale)
    {
        BigInteger aBigInteger = Decimals.decodeUnscaledValue(a);
        BigInteger bBigInteger = Decimals.decodeUnscaledValue(b);
        return internalModulusLongResult(aBigInteger, bBigInteger, aRescale, bRescale);
    }

    @UsedByGeneratedCode
    public static Slice modulusShortLongLong(long a, Slice b, BigInteger aRescale, BigInteger bRescale)
    {
        BigInteger aBigInteger = BigInteger.valueOf(a);
        BigInteger bBigInteger = Decimals.decodeUnscaledValue(b);
        return internalModulusLongResult(aBigInteger, bBigInteger, aRescale, bRescale);
    }

    @UsedByGeneratedCode
    public static long modulusShortLongShort(long a, Slice b, BigInteger aRescale, BigInteger bRescale)
    {
        BigInteger aBigInteger = BigInteger.valueOf(a).multiply(aRescale);
        BigInteger bBigInteger = Decimals.decodeUnscaledValue(b).multiply(bRescale);
        return internalModulusShortResult(aBigInteger, bBigInteger, aRescale, bRescale);
    }

    @UsedByGeneratedCode
    public static long modulusLongShortShort(Slice a, long b, BigInteger aRescale, BigInteger bRescale)
    {
        BigInteger aBigInteger = Decimals.decodeUnscaledValue(a);
        BigInteger bBigInteger = BigInteger.valueOf(b);
        return internalModulusShortResult(aBigInteger, bBigInteger, aRescale, bRescale);
    }

    @UsedByGeneratedCode
    public static Slice modulusLongShortLong(Slice a, long b, BigInteger aRescale, BigInteger bRescale)
    {
        BigInteger aBigInteger = Decimals.decodeUnscaledValue(a);
        BigInteger bBigInteger = BigInteger.valueOf(b);
        return internalModulusLongResult(aBigInteger, bBigInteger, aRescale, bRescale);
    }

    private static long internalModulusShortResult(BigInteger aBigInteger, BigInteger bBigInteger, BigInteger aRescale, BigInteger bRescale)
    {
        try {
            return aBigInteger.multiply(aRescale).remainder(bBigInteger.multiply(bRescale)).longValue();
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    private static Slice internalModulusLongResult(BigInteger aBigInteger, BigInteger bBigInteger, BigInteger aRescale, BigInteger bRescale)
    {
        try {
            BigInteger result = aBigInteger.multiply(aRescale).remainder(bBigInteger.multiply(bRescale));
            return Decimals.encodeUnscaledValue(result);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(DIVISION_BY_ZERO, e);
        }
    }

    @ScalarOperator(NEGATION)
    public static final class Negation
    {
        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static long negate(@SqlType("decimal(p, s)") long arg)
        {
            return -arg;
        }

        @LiteralParameters({"p", "s"})
        @SqlType("decimal(p, s)")
        public static Slice negate(@SqlType("decimal(p, s)") Slice arg)
        {
            BigInteger argBigInteger = Decimals.decodeUnscaledValue(arg);
            return Decimals.encodeUnscaledValue(argBigInteger.negate());
        }
    }
}
