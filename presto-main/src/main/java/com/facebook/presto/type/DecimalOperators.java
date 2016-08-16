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
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.SqlScalarFunctionBuilder;
import com.facebook.presto.metadata.SqlScalarFunctionBuilder.SpecializeContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.longVariableExpression;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.DIVIDE;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.MODULUS;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.NEGATION;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.divideRoundUp;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.isZero;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.remainder;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.throwIfOverflows;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLong;
import static java.lang.Integer.max;
import static java.lang.Long.signum;
import static java.lang.Math.abs;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class DecimalOperators
{
    public static final SqlScalarFunction DECIMAL_ADD_OPERATOR = decimalAddOperator();
    public static final SqlScalarFunction DECIMAL_SUBTRACT_OPERATOR = decimalSubtractOperator();
    public static final SqlScalarFunction DECIMAL_MULTIPLY_OPERATOR = decimalMultiplyOperator();
    public static final SqlScalarFunction DECIMAL_DIVIDE_OPERATOR = decimalDivideOperator();
    public static final SqlScalarFunction DECIMAL_MODULUS_OPERATOR = decimalModulusOperator();

    private DecimalOperators()
    {
    }

    private static SqlScalarFunction decimalAddOperator()
    {
        TypeSignature decimalLeftSignature = parseTypeSignature("decimal(a_precision, a_scale)", ImmutableSet.of("a_precision", "a_scale"));
        TypeSignature decimalRightSignature = parseTypeSignature("decimal(b_precision, b_scale)", ImmutableSet.of("b_precision", "b_scale"));
        TypeSignature decimalResultSignature = parseTypeSignature("decimal(r_precision, r_scale)", ImmutableSet.of("r_precision", "r_scale"));

        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(ADD)
                .longVariableConstraints(
                        longVariableExpression("r_precision", "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)"),
                        longVariableExpression("r_scale", "max(a_scale, b_scale)"))
                .argumentTypes(decimalLeftSignature, decimalRightSignature)
                .returnType(decimalResultSignature)
                .build();
        return SqlScalarFunction.builder(DecimalOperators.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("addShortShortShort")
                        .withExtraParameters(DecimalOperators::calculateShortRescaleParameters)
                )
                .implementation(b -> b
                        .methods("addShortShortLong", "addLongLongLong", "addShortLongLong", "addLongShortLong")
                        .withExtraParameters(DecimalOperators::calculateLongRescaleParameters)
                )
                .build();
    }

    @UsedByGeneratedCode
    public static long addShortShortShort(long a, long b, long aRescale, long bRescale)
    {
        return a * aRescale + b * bRescale;
    }

    @UsedByGeneratedCode
    public static Slice addShortShortLong(long a, long b, int rescale, boolean left)
    {
        return internalAddLongLongLong(unscaledDecimal(a), unscaledDecimal(b), rescale, left);
    }

    @UsedByGeneratedCode
    public static Slice addLongLongLong(Slice a, Slice b, int rescale, boolean left)
    {
        return internalAddLongLongLong(a, b, rescale, left);
    }

    @UsedByGeneratedCode
    public static Slice addShortLongLong(long a, Slice b, int rescale, boolean left)
    {
        return internalAddLongLongLong(unscaledDecimal(a), b, rescale, left);
    }

    @UsedByGeneratedCode
    public static Slice addLongShortLong(Slice a, long b, int rescale, boolean left)
    {
        return internalAddLongLongLong(a, unscaledDecimal(b), rescale, left);
    }

    private static Slice internalAddLongLongLong(Slice a, Slice b, int rescale, boolean rescaleLeft)
    {
        try {
            Slice left = unscaledDecimal();
            Slice right;

            if (rescaleLeft) {
                rescale(a, rescale, left);
                right = b;
            }
            else {
                rescale(b, rescale, left);
                right = a;
            }

            UnscaledDecimal128Arithmetic.add(left, right, left);
            throwIfOverflows(left);
            return left;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    private static SqlScalarFunction decimalSubtractOperator()
    {
        TypeSignature decimalLeftSignature = parseTypeSignature("decimal(a_precision, a_scale)", ImmutableSet.of("a_precision", "a_scale"));
        TypeSignature decimalRightSignature = parseTypeSignature("decimal(b_precision, b_scale)", ImmutableSet.of("b_precision", "b_scale"));
        TypeSignature decimalResultSignature = parseTypeSignature("decimal(r_precision, r_scale)", ImmutableSet.of("r_precision", "r_scale"));

        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(SUBTRACT)
                .longVariableConstraints(
                        longVariableExpression("r_precision", "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)"),
                        longVariableExpression("r_scale", "max(a_scale, b_scale)"))
                .argumentTypes(decimalLeftSignature, decimalRightSignature)
                .returnType(decimalResultSignature)
                .build();
        return SqlScalarFunction.builder(DecimalOperators.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("subtractShortShortShort")
                        .withExtraParameters(DecimalOperators::calculateShortRescaleParameters)
                )
                .implementation(b -> b
                        .methods("subtractShortShortLong", "subtractLongLongLong", "subtractShortLongLong", "subtractLongShortLong")
                        .withExtraParameters(DecimalOperators::calculateLongRescaleParameters)
                )
                .build();
    }

    @UsedByGeneratedCode
    public static long subtractShortShortShort(long a, long b, long aRescale, long bRescale)
    {
        return a * aRescale - b * bRescale;
    }

    @UsedByGeneratedCode
    public static Slice subtractShortShortLong(long a, long b, int rescale, boolean left)
    {
        return internalSubtractLongLongLong(unscaledDecimal(a), unscaledDecimal(b), rescale, left);
    }

    @UsedByGeneratedCode
    public static Slice subtractLongLongLong(Slice a, Slice b, int rescale, boolean left)
    {
        return internalSubtractLongLongLong(a, b, rescale, left);
    }

    @UsedByGeneratedCode
    public static Slice subtractShortLongLong(long a, Slice b, int rescale, boolean left)
    {
        return internalSubtractLongLongLong(unscaledDecimal(a), b, rescale, left);
    }

    @UsedByGeneratedCode
    public static Slice subtractLongShortLong(Slice a, long b, int rescale, boolean left)
    {
        return internalSubtractLongLongLong(a, unscaledDecimal(b), rescale, left);
    }

    private static Slice internalSubtractLongLongLong(Slice a, Slice b, int rescale, boolean rescaleLeft)
    {
        try {
            Slice tmp = unscaledDecimal();
            if (rescaleLeft) {
                rescale(a, rescale, tmp);
                UnscaledDecimal128Arithmetic.subtract(tmp, b, tmp);
            }
            else {
                rescale(b, rescale, tmp);
                UnscaledDecimal128Arithmetic.subtract(a, tmp, tmp);
            }
            throwIfOverflows(tmp);
            return tmp;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    private static SqlScalarFunction decimalMultiplyOperator()
    {
        TypeSignature decimalLeftSignature = parseTypeSignature("decimal(a_precision, a_scale)", ImmutableSet.of("a_precision", "a_scale"));
        TypeSignature decimalRightSignature = parseTypeSignature("decimal(b_precision, b_scale)", ImmutableSet.of("b_precision", "b_scale"));
        TypeSignature decimalResultSignature = parseTypeSignature("decimal(r_precision, r_scale)", ImmutableSet.of("r_precision", "r_scale"));

        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(MULTIPLY)
                .longVariableConstraints(
                        longVariableExpression("r_precision", "min(38, a_precision + b_precision)"),
                        longVariableExpression("r_scale", "a_scale + b_scale")
                )
                .argumentTypes(decimalLeftSignature, decimalRightSignature)
                .returnType(decimalResultSignature)
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
        return multiplyLongLongLong(encodeUnscaledValue(a), encodeUnscaledValue(b));
    }

    @UsedByGeneratedCode
    public static Slice multiplyLongLongLong(Slice a, Slice b)
    {
        try {
            Slice result = UnscaledDecimal128Arithmetic.multiply(a, b);
            throwIfOverflows(result);
            return result;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Slice multiplyShortLongLong(long a, Slice b)
    {
        return multiplyLongLongLong(encodeUnscaledValue(a), b);
    }

    @UsedByGeneratedCode
    public static Slice multiplyLongShortLong(Slice a, long b)
    {
        return multiplyLongLongLong(a, encodeUnscaledValue(b));
    }

    private static SqlScalarFunction decimalDivideOperator()
    {
        TypeSignature decimalLeftSignature = parseTypeSignature("decimal(a_precision, a_scale)", ImmutableSet.of("a_precision", "a_scale"));
        TypeSignature decimalRightSignature = parseTypeSignature("decimal(b_precision, b_scale)", ImmutableSet.of("b_precision", "b_scale"));
        TypeSignature decimalResultSignature = parseTypeSignature("decimal(r_precision, r_scale)", ImmutableSet.of("r_precision", "r_scale"));

        // we extend target precision by b_scale. This is upper bound on how much division result will grow.
        // pessimistic case is a / 0.0000001
        // if scale of divisor is greater than scale of dividend we extend scale further as we
        // want result scale to be maximum of scales of divisor and dividend.
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(DIVIDE)
                .longVariableConstraints(
                        longVariableExpression("r_precision", "min(38, a_precision + b_scale + max(b_scale - a_scale, 0))"),
                        longVariableExpression("r_scale", "max(a_scale, b_scale)")
                )
                .argumentTypes(decimalLeftSignature, decimalRightSignature)
                .returnType(decimalResultSignature)
                .build();
        return SqlScalarFunction.builder(DecimalOperators.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("divideShortShortShort", "divideShortLongShort", "divideLongShortShort", "divideShortShortLong", "divideLongLongLong", "divideShortLongLong", "divideLongShortLong")
                        .withExtraParameters(DecimalOperators::divideRescaleFactor)
                )
                .build();
    }

    private static List<Object> divideRescaleFactor(SqlScalarFunctionBuilder.SpecializeContext context)
    {
        DecimalType returnType = (DecimalType) context.getReturnType();
        int dividendScale = toIntExact(requireNonNull(context.getLiteral("a_scale"), "a_scale is null"));
        int divisorScale = toIntExact(requireNonNull(context.getLiteral("b_scale"), "b_scale is null"));
        int resultScale = returnType.getScale();
        int rescaleFactor = resultScale - dividendScale + divisorScale;
        return ImmutableList.of(rescaleFactor);
    }

    @UsedByGeneratedCode
    public static long divideShortShortShort(long dividend, long divisor, int rescaleFactor)
    {
        if (divisor == 0) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }

        if (dividend == 0) {
            return 0;
        }

        int resultSignum = signum(dividend) * signum(divisor);

        long unsignedDividend = abs(dividend);
        long unsignedDivisor = abs(divisor);

        long rescaledUnsignedDividend = unsignedDividend * longTenToNth(rescaleFactor);
        long quotient = rescaledUnsignedDividend / unsignedDivisor;
        long remainder = rescaledUnsignedDividend - (quotient * unsignedDivisor);

        if (Long.compareUnsigned(remainder * 2, unsignedDivisor) >= 0) {
            quotient++;
        }

        return resultSignum * quotient;
    }

    @UsedByGeneratedCode
    public static long divideShortLongShort(long dividend, Slice divisor, int rescaleFactor)
    {
        if (isZero(divisor)) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return unscaledDecimalToUnscaledLong(divideRoundUp(dividend, rescaleFactor, divisor));
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static long divideLongShortShort(Slice dividend, long divisor, int rescaleFactor)
    {
        if (divisor == 0) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return unscaledDecimalToUnscaledLong(divideRoundUp(dividend, rescaleFactor, divisor));
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Slice divideShortShortLong(long dividend, long divisor, int rescaleFactor)
    {
        if (divisor == 0) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return divideRoundUp(dividend, rescaleFactor, divisor);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Slice divideLongLongLong(Slice dividend, Slice divisor, int rescaleFactor)
    {
        if (isZero(divisor)) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return divideRoundUp(dividend, rescaleFactor, divisor);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Slice divideShortLongLong(long dividend, Slice divisor, int rescaleFactor)
    {
        if (isZero(divisor)) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return divideRoundUp(dividend, rescaleFactor, divisor);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Slice divideLongShortLong(Slice dividend, long divisor, int rescaleFactor)
    {
        if (divisor == 0) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return divideRoundUp(dividend, rescaleFactor, divisor);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    private static SqlScalarFunction decimalModulusOperator()
    {
        Signature signature = modulusSignatureBuilder()
                .operatorType(MODULUS)
                .build();
        return modulusScalarFunction(signature);
    }

    public static SqlScalarFunction modulusScalarFunction(Signature signature)
    {
        return SqlScalarFunction.builder(DecimalOperators.class)
                .signature(signature)
                .implementation(b -> b
                    .methods("modulusShortShortShort", "modulusLongLongLong", "modulusShortLongLong", "modulusShortLongShort", "modulusLongShortShort", "modulusLongShortLong")
                    .withExtraParameters(DecimalOperators::modulusRescaleParameters)
                )
                .build();
    }

    public static SignatureBuilder modulusSignatureBuilder()
    {
        TypeSignature decimalLeftSignature = parseTypeSignature("decimal(a_precision, a_scale)", ImmutableSet.of("a_precision", "a_scale"));
        TypeSignature decimalRightSignature = parseTypeSignature("decimal(b_precision, b_scale)", ImmutableSet.of("b_precision", "b_scale"));
        TypeSignature decimalResultSignature = parseTypeSignature("decimal(r_precision, r_scale)", ImmutableSet.of("r_precision", "r_scale"));

        return Signature.builder()
                .longVariableConstraints(
                        longVariableExpression("r_precision", "min(b_precision - b_scale, a_precision - a_scale) + max(a_scale, b_scale)"),
                        longVariableExpression("r_scale", "max(a_scale, b_scale)")
                )
                .argumentTypes(decimalLeftSignature, decimalRightSignature)
                .returnType(decimalResultSignature);
    }

    private static List<Object> calculateShortRescaleParameters(SpecializeContext context)
    {
        long aRescale = longTenToNth(rescaleFactor(context.getLiteral("a_scale"), context.getLiteral("b_scale")));
        long bRescale = longTenToNth(rescaleFactor(context.getLiteral("b_scale"), context.getLiteral("a_scale")));
        return ImmutableList.of(aRescale, bRescale);
    }

    private static List<Object> calculateLongRescaleParameters(SpecializeContext context)
    {
        long aScale = context.getLiteral("a_scale");
        long bScale = context.getLiteral("b_scale");
        int aRescale = rescaleFactor(aScale, bScale);
        int bRescale = rescaleFactor(bScale, aScale);

        int rescale;
        boolean left;
        if (aRescale == 0) {
            rescale = bRescale;
            left = false;
        }
        else if (bRescale == 0) {
            rescale = aRescale;
            left = true;
        }
        else {
            throw new IllegalStateException();
        }
        return ImmutableList.of(rescale, left);
    }

    private static List<Object> modulusRescaleParameters(SqlScalarFunctionBuilder.SpecializeContext context)
    {
        int dividendScale = toIntExact(requireNonNull(context.getLiteral("a_scale"), "a_scale is null"));
        int divisorScale = toIntExact(requireNonNull(context.getLiteral("b_scale"), "b_scale is null"));
        int dividendRescaleFactor = rescaleFactor(dividendScale, divisorScale);
        int divisorRescaleFactor = rescaleFactor(divisorScale, dividendScale);
        return ImmutableList.of(dividendRescaleFactor, divisorRescaleFactor);
    }

    private static int rescaleFactor(long fromScale, long toScale)
    {
        return max(0, (int) toScale - (int) fromScale);
    }

    @UsedByGeneratedCode
    public static long modulusShortShortShort(long dividend, long divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (divisor == 0) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return unscaledDecimalToUnscaledLong(remainder(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor));
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static long modulusShortLongShort(long dividend, Slice divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (isZero(divisor)) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return unscaledDecimalToUnscaledLong(remainder(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor));
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static long modulusLongShortShort(Slice dividend, long divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (divisor == 0) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return unscaledDecimalToUnscaledLong(remainder(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor));
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Slice modulusShortLongLong(long dividend, Slice divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (isZero(divisor)) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return remainder(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Slice modulusLongShortLong(Slice dividend, long divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (divisor == 0) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return remainder(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
        }
    }

    @UsedByGeneratedCode
    public static Slice modulusLongLongLong(Slice dividend, Slice divisor, int dividendRescaleFactor, int divisorRescaleFactor)
    {
        if (isZero(divisor)) {
            throw new PrestoException(DIVISION_BY_ZERO, "Division by zero");
        }
        try {
            return remainder(dividend, dividendRescaleFactor, divisor, divisorRescaleFactor);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow", e);
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
            return encodeUnscaledValue(argBigInteger.negate());
        }
    }

    @ScalarOperator(HASH_CODE)
    public static final class HashCode
    {
        @LiteralParameters({"p", "s"})
        @SqlType("bigint")
        public static long hashCode(@SqlType("decimal(p, s)") long value)
        {
            return value;
        }

        @LiteralParameters({"p", "s"})
        @SqlType("bigint")
        public static long hashCode(@SqlType("decimal(p, s)") Slice value)
        {
            return UnscaledDecimal128Arithmetic.hash(value);
        }
    }
}
