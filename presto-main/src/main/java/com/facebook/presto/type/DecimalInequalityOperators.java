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
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.SqlScalarFunctionBuilder.SpecializeContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.SqlScalarFunctionBuilder.concat;
import static com.facebook.presto.metadata.SqlScalarFunctionBuilder.constant;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.Decimals.bigIntegerTenToNth;
import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.Integer.max;

public class DecimalInequalityOperators
{
    private static final MethodHandle IS_RESULT_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultEqual", int.class);
    private static final MethodHandle IS_RESULT_NOT_EQUAL = methodHandle(DecimalInequalityOperators.class, "isResultNotEqual", int.class);
    private static final MethodHandle IS_RESULT_LESS_THAN = methodHandle(DecimalInequalityOperators.class, "isResultLessThan", int.class);
    private static final MethodHandle IS_RESULT_LESS_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "isResultLessThanOrEqual", int.class);
    private static final MethodHandle IS_RESULT_GREATER_THAN = methodHandle(DecimalInequalityOperators.class, "isResultGreaterThan", int.class);
    private static final MethodHandle IS_RESULT_GREATER_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "isResultGreaterThanOrEqual", int.class);

    public static final SqlScalarFunction DECIMAL_EQUAL_OPERATOR = binaryOperator(EQUAL, IS_RESULT_EQUAL);
    public static final SqlScalarFunction DECIMAL_NOT_EQUAL_OPERATOR = binaryOperator(NOT_EQUAL, IS_RESULT_NOT_EQUAL);
    public static final SqlScalarFunction DECIMAL_LESS_THAN_OPERATOR = binaryOperator(LESS_THAN, IS_RESULT_LESS_THAN);
    public static final SqlScalarFunction DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR = binaryOperator(LESS_THAN_OR_EQUAL, IS_RESULT_LESS_THAN_OR_EQUAL);
    public static final SqlScalarFunction DECIMAL_GREATER_THAN_OPERATOR = binaryOperator(GREATER_THAN, IS_RESULT_GREATER_THAN);
    public static final SqlScalarFunction DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR = binaryOperator(GREATER_THAN_OR_EQUAL, IS_RESULT_GREATER_THAN_OR_EQUAL);
    public static final SqlScalarFunction DECIMAL_BETWEEN_OPERATOR = betweenOperator();

    private static final int MAX_PRECISION_OF_JAVA_LONG = 18;

    private DecimalInequalityOperators() {}

    @UsedByGeneratedCode
    public static boolean getResultEqual(int comparisonResult)
    {
        return comparisonResult == 0;
    }

    @UsedByGeneratedCode
    public static boolean isResultNotEqual(int comparisonResult)
    {
        return comparisonResult != 0;
    }

    @UsedByGeneratedCode
    public static boolean isResultLessThan(int comparisonResult)
    {
        return comparisonResult < 0;
    }

    @UsedByGeneratedCode
    public static boolean isResultLessThanOrEqual(int comparisonResult)
    {
        return comparisonResult <= 0;
    }

    @UsedByGeneratedCode
    public static boolean isResultGreaterThan(int comparisonResult)
    {
        return comparisonResult > 0;
    }

    @UsedByGeneratedCode
    public static boolean isResultGreaterThanOrEqual(int comparisonResult)
    {
        return comparisonResult >= 0;
    }

    private static SqlScalarFunction binaryOperator(OperatorType operatorType, MethodHandle getResultMethodHandle)
    {
        TypeSignature decimalASignature = parseTypeSignature("decimal(a_precision, a_scale)", ImmutableSet.of("a_precision", "a_scale"));
        TypeSignature decimalBSignature = parseTypeSignature("decimal(b_precision, b_scale)", ImmutableSet.of("b_precision", "b_scale"));
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(operatorType)
                .argumentTypes(decimalASignature, decimalBSignature)
                .returnType(parseTypeSignature(BOOLEAN))
                .build();
        return SqlScalarFunction.builder(DecimalInequalityOperators.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("opShortShortShortRescale")
                        .withPredicate(DecimalInequalityOperators::rescaledValuesFitJavaLong)
                        .withExtraParameters(concat(DecimalInequalityOperators::shortRescaleExtraParameters, constant(getResultMethodHandle)))
                )
                .implementation(b -> b
                        .methods("opShortShortLongRescale", "opShortLong", "opLongShort", "opLongLong")
                        .withExtraParameters(concat(DecimalInequalityOperators::longRescaleExtraParameters, constant(getResultMethodHandle)))
                )
                .build();
    }

    private static boolean rescaledValuesFitJavaLong(SpecializeContext context)
    {
        long aPrecision = context.getLiteral("a_precision");
        long aScale = context.getLiteral("a_scale");
        long bPrecision = context.getLiteral("b_precision");
        long bScale = context.getLiteral("b_scale");
        long aRescaleFactor = rescaleFactor(aScale, bScale);
        long bRescaleFactor = rescaleFactor(bScale, aScale);
        return aPrecision + aRescaleFactor <= MAX_PRECISION_OF_JAVA_LONG &&
                bPrecision + bRescaleFactor <= MAX_PRECISION_OF_JAVA_LONG;
    }

    private static List<Object> shortRescaleExtraParameters(SpecializeContext context)
    {
        long aScale = context.getLiteral("a_scale");
        long bScale = context.getLiteral("b_scale");
        long aRescale = longTenToNth(rescaleFactor(aScale, bScale));
        long bRescale = longTenToNth(rescaleFactor(bScale, aScale));
        return ImmutableList.of(aRescale, bRescale);
    }

    private static List<Object> longRescaleExtraParameters(SpecializeContext context)
    {
        long aScale = context.getLiteral("a_scale");
        long bScale = context.getLiteral("b_scale");
        BigInteger aRescale = bigIntegerTenToNth(rescaleFactor(aScale, bScale));
        BigInteger bRescale = bigIntegerTenToNth(rescaleFactor(bScale, aScale));
        return ImmutableList.of(aRescale, bRescale);
    }

    private static int rescaleFactor(long fromScale, long toScale)
    {
        return max(0, (int) (toScale - fromScale));
    }

    @UsedByGeneratedCode
    public static boolean opShortShortShortRescale(long a, long b, long aRescale, long bRescale, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, Long.compare(a * aRescale, b * bRescale));
    }

    @UsedByGeneratedCode
    public static boolean opShortShortLongRescale(long a, long b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
    {
        BigInteger left = BigInteger.valueOf(a).multiply(aRescale);
        BigInteger right = BigInteger.valueOf(b).multiply(bRescale);
        return invokeGetResult(getResultMethodHandle, left.compareTo(right));
    }

    @UsedByGeneratedCode
    public static boolean opShortLong(long a, Slice b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
    {
        BigInteger left = BigInteger.valueOf(a).multiply(aRescale);
        BigInteger right = Decimals.decodeUnscaledValue(b).multiply(bRescale);
        return invokeGetResult(getResultMethodHandle, left.compareTo(right));
    }

    @UsedByGeneratedCode
    public static boolean opLongShort(Slice a, long b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
    {
        BigInteger left = Decimals.decodeUnscaledValue(a).multiply(aRescale);
        BigInteger right = BigInteger.valueOf(b).multiply(bRescale);
        return invokeGetResult(getResultMethodHandle, left.compareTo(right));
    }

    @UsedByGeneratedCode
    public static boolean opLongLong(Slice a, Slice b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
    {
        BigInteger left = Decimals.decodeUnscaledValue(a).multiply(aRescale);
        BigInteger right = Decimals.decodeUnscaledValue(b).multiply(bRescale);
        return invokeGetResult(getResultMethodHandle, left.compareTo(right));
    }

    private static boolean invokeGetResult(MethodHandle getResultMethodHandle, int comparisonResult)
    {
        try {
            return (boolean) getResultMethodHandle.invokeExact(comparisonResult);
        }
        catch (Throwable t) {
            Throwables.propagateIfInstanceOf(t, Error.class);
            Throwables.propagateIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    private static SqlScalarFunction betweenOperator()
    {
        TypeSignature valueSignature = parseTypeSignature("decimal(value_precision, value_scale)", ImmutableSet.of("value_precision", "value_scale"));
        TypeSignature lowSignature = parseTypeSignature("decimal(low_precision, low_scale)", ImmutableSet.of("low_precision", "low_scale"));
        TypeSignature highSignature = parseTypeSignature("decimal(high_precision, high_scale)", ImmutableSet.of("high_precision", "high_scale"));
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(BETWEEN)
                .argumentTypes(valueSignature, lowSignature, highSignature)
                .returnType(parseTypeSignature(BOOLEAN))
                .build();
        return SqlScalarFunction.builder(DecimalInequalityOperators.class)
                .signature(signature)
                .implementation(b -> b
                        .methods("betweenShortShortShort", "betweenShortShortLong",
                                "betweenShortLongShort", "betweenShortLongLong",
                                "betweenLongShortShort", "betweenLongShortLong",
                                "betweenLongLongShort", "betweenLongLongLong")
                        .withExtraParameters(DecimalInequalityOperators::bindMethodsExtraParameters)
                )
                .build();
    }

    private static List<Object> bindMethodsExtraParameters(SpecializeContext context)
    {
        long valuePrecision = context.getLiteral("value_precision");
        long valueScale = context.getLiteral("value_scale");
        long lowPrecision = context.getLiteral("low_precision");
        long lowScale = context.getLiteral("low_scale");
        long highPrecision = context.getLiteral("high_precision");
        long highScale = context.getLiteral("high_scale");

        MethodHandle lowerBoundTestMethodHandle = DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR.specialize(
                new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "a_precision", lowPrecision,
                                "a_scale", lowScale,
                                "b_precision", valuePrecision,
                                "b_scale", valueScale
                        )
                ),
                2,
                context.getTypeManager(),
                context.getFunctionRegistry()
        ).getMethodHandle();
        MethodHandle upperBoundTestMethodHandle = DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR.specialize(
                new BoundVariables(
                        ImmutableMap.of(),
                        ImmutableMap.of(
                                "a_precision", highPrecision,
                                "a_scale", highScale,
                                "b_precision", valuePrecision,
                                "b_scale", valueScale
                        )
                ),
                2,
                context.getTypeManager(),
                context.getFunctionRegistry()
        ).getMethodHandle();
        return ImmutableList.of(lowerBoundTestMethodHandle, upperBoundTestMethodHandle);
    }

    @UsedByGeneratedCode
    public static boolean betweenShortShortShort(long value, long low, long high, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(low, value) && (boolean) upperBoundTestMethodHandle.invokeExact(high, value);
    }

    @UsedByGeneratedCode
    public static boolean betweenShortShortLong(long value, long low, Slice high, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(low, value) && (boolean) upperBoundTestMethodHandle.invokeExact(high, value);
    }

    @UsedByGeneratedCode
    public static boolean betweenShortLongShort(long value, Slice low, long high, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(low, value) && (boolean) upperBoundTestMethodHandle.invokeExact(high, value);
    }

    @UsedByGeneratedCode
    public static boolean betweenShortLongLong(long value, Slice low, Slice high, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(low, value) && (boolean) upperBoundTestMethodHandle.invokeExact(high, value);
    }

    @UsedByGeneratedCode
    public static boolean betweenLongShortShort(Slice value, long low, long high, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(low, value) && (boolean) upperBoundTestMethodHandle.invokeExact(high, value);
    }

    @UsedByGeneratedCode
    public static boolean betweenLongShortLong(Slice value, long low, Slice high, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(low, value) && (boolean) upperBoundTestMethodHandle.invokeExact(high, value);
    }

    @UsedByGeneratedCode
    public static boolean betweenLongLongShort(Slice value, Slice low, long high, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(low, value) && (boolean) upperBoundTestMethodHandle.invokeExact(high, value);
    }

    @UsedByGeneratedCode
    public static boolean betweenLongLongLong(Slice value, Slice low, Slice high, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(low, value) && (boolean) upperBoundTestMethodHandle.invokeExact(high, value);
    }
}
