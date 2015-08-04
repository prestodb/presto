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

import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.ParametricFunctionBuilder.SpecializeContext;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.ParametricFunctionBuilder.concat;
import static com.facebook.presto.metadata.ParametricFunctionBuilder.constant;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.LongDecimalType.unscaledValueToBigInteger;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.Integer.max;

public class DecimalInequalityOperators
{
    private static final MethodHandle GET_RESULT_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultEqual", int.class);
    private static final MethodHandle GET_RESULT_NOT_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultNotEqual", int.class);
    private static final MethodHandle GET_RESULT_LESS_THAN = methodHandle(DecimalInequalityOperators.class, "getResultLessThan", int.class);
    private static final MethodHandle GET_RESULT_LESS_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultLessThanOrEqual", int.class);
    private static final MethodHandle GET_RESULT_GREATER_THAN = methodHandle(DecimalInequalityOperators.class, "getResultGreaterThan", int.class);
    private static final MethodHandle GET_RESULT_GREATER_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultGreaterThanOrEqual", int.class);

    public static final ParametricFunction DECIMAL_EQUAL_OPERATOR = binaryOperator(EQUAL, GET_RESULT_EQUAL);
    public static final ParametricFunction DECIMAL_NOT_EQUAL_OPERATOR = binaryOperator(NOT_EQUAL, GET_RESULT_NOT_EQUAL);
    public static final ParametricFunction DECIMAL_LESS_THAN_OPERATOR = binaryOperator(LESS_THAN, GET_RESULT_LESS_THAN);
    public static final ParametricFunction DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR = binaryOperator(LESS_THAN_OR_EQUAL, GET_RESULT_LESS_THAN_OR_EQUAL);
    public static final ParametricFunction DECIMAL_GREATER_THAN_OPERATOR = binaryOperator(GREATER_THAN, GET_RESULT_GREATER_THAN);
    public static final ParametricFunction DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR = binaryOperator(GREATER_THAN_OR_EQUAL, GET_RESULT_GREATER_THAN_OR_EQUAL);
    public static final ParametricFunction DECIMAL_BETWEEN_OPERATOR = betweenOperator();

    private static final int MAX_PRECISION_OF_LONG = 18;

    private DecimalInequalityOperators() {}

    public static boolean getResultEqual(int comparisonResult)
    {
        return comparisonResult == 0;
    }

    public static boolean getResultNotEqual(int comparisonResult)
    {
        return comparisonResult != 0;
    }

    public static boolean getResultLessThan(int comparisonResult)
    {
        return comparisonResult < 0;
    }

    public static boolean getResultLessThanOrEqual(int comparisonResult)
    {
        return comparisonResult <= 0;
    }

    public static boolean getResultGreaterThan(int comparisonResult)
    {
        return comparisonResult > 0;
    }

    public static boolean getResultGreaterThanOrEqual(int comparisonResult)
    {
        return comparisonResult >= 0;
    }

    private static ParametricFunction binaryOperator(OperatorType operatorType, MethodHandle getResultMethodHandle)
    {
        Signature signature = Signature.builder()
                .operatorType(operatorType)
                .typeParameters(comparableWithVariadicBound("A", DECIMAL), comparableWithVariadicBound("B", DECIMAL))
                .returnType(BOOLEAN)
                .argumentTypes("A", "B")
                .build();
        return ParametricFunction.builder(DecimalInequalityOperators.class)
                .signature(signature)
                .methods("opShortShortShortRescale")
                .predicate(DecimalInequalityOperators::rescaledDecimalsFitLong)
                .extraParameters(concat(DecimalInequalityOperators::shortRescaleExtraParameters, constant(getResultMethodHandle)))
                .methods("opShortShortLongRescale", "opShortLong", "opLongShort", "opLongLong")
                .extraParameters(concat(DecimalInequalityOperators::longRescaleExtraParameters, constant(getResultMethodHandle)))
                .build();
    }

    private static boolean rescaledDecimalsFitLong(SpecializeContext context)
    {
        DecimalType aType = (DecimalType) context.getType("A");
        DecimalType bType = (DecimalType) context.getType("B");
        int aRescaleFactor = rescaleFactor(aType.getScale(), bType.getScale());
        int bRescaleFactor = rescaleFactor(bType.getScale(), aType.getScale());
        return aType.getPrecision() + aRescaleFactor <= MAX_PRECISION_OF_LONG &&
                bType.getPrecision() + bRescaleFactor <= MAX_PRECISION_OF_LONG;
    }

    private static List<Object> shortRescaleExtraParameters(SpecializeContext context)
    {
        DecimalType aType = (DecimalType) context.getType("A");
        DecimalType bType = (DecimalType) context.getType("B");
        long aRescale = ShortDecimalType.tenToNth(rescaleFactor(aType.getScale(), bType.getScale()));
        long bRescale = ShortDecimalType.tenToNth(rescaleFactor(bType.getScale(), aType.getScale()));
        return ImmutableList.of(aRescale, bRescale);
    }

    private static List<Object> longRescaleExtraParameters(SpecializeContext context)
    {
        DecimalType aType = (DecimalType) context.getType("A");
        DecimalType bType = (DecimalType) context.getType("B");
        BigInteger aRescale = LongDecimalType.tenToNth(rescaleFactor(aType.getScale(), bType.getScale()));
        BigInteger bRescale = LongDecimalType.tenToNth(rescaleFactor(bType.getScale(), aType.getScale()));
        return ImmutableList.of(aRescale, bRescale);
    }

    private static int rescaleFactor(int fromScale, int toScale)
    {
        return max(0, toScale - fromScale);
    }

    public static boolean opShortShortShortRescale(long a, long b, long aRescale, long bRescale, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, Long.compare(a * aRescale, b * bRescale));
    }

    public static boolean opShortShortLongRescale(long a, long b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
    {
        BigInteger left = BigInteger.valueOf(a).multiply(aRescale);
        BigInteger right = BigInteger.valueOf(b).multiply(bRescale);
        return invokeGetResult(getResultMethodHandle, left.compareTo(right));
    }

    public static boolean opShortLong(long a, Slice b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
    {
        BigInteger left = BigInteger.valueOf(a).multiply(aRescale);
        BigInteger right = unscaledValueToBigInteger(b).multiply(bRescale);
        return invokeGetResult(getResultMethodHandle, left.compareTo(right));
    }

    public static boolean opLongShort(Slice a, long b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
    {
        BigInteger left = unscaledValueToBigInteger(a).multiply(aRescale);
        BigInteger right = BigInteger.valueOf(b).multiply(bRescale);
        return invokeGetResult(getResultMethodHandle, left.compareTo(right));
    }

    public static boolean opLongLong(Slice a, Slice b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
    {
        BigInteger left = unscaledValueToBigInteger(a).multiply(aRescale);
        BigInteger right = unscaledValueToBigInteger(b).multiply(bRescale);
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
            throw new PrestoException(INTERNAL_ERROR, t);
        }
    }

    private static ParametricFunction betweenOperator()
    {
        Signature signature = Signature.builder()
                .operatorType(BETWEEN)
                .typeParameters(
                        comparableWithVariadicBound("V", DECIMAL),
                        comparableWithVariadicBound("A", DECIMAL),
                        comparableWithVariadicBound("B", DECIMAL))
                .returnType(BOOLEAN)
                .argumentTypes("V", "A", "B")
                .build();
        return ParametricFunction.builder(DecimalInequalityOperators.class)
                .signature(signature)
                .methods(
                        "betweenShortShortShort", "betweenShortShortLong",
                        "betweenShortLongShort", "betweenShortLongLong",
                        "betweenLongShortShort", "betweenLongShortLong",
                        "betweenLongLongShort", "betweenLongLongLong")
                .extraParameters(DecimalInequalityOperators::boundMethodsExtraParameters)
                .build();
    }

    private static List<Object> boundMethodsExtraParameters(SpecializeContext context)
    {
        Type aType = context.getType("A");
        Type bType = context.getType("B");
        Type vType = context.getType("V");
        MethodHandle lowerBoundTestMethodHandle = DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR.specialize(
                ImmutableMap.of("A", aType, "B", vType), ImmutableList.of(aType.getTypeSignature(), vType.getTypeSignature()),
                context.getTypeManager(), context.getFunctionRegistry()).getMethodHandle();
        MethodHandle upperBoundTestMethodHandle = DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR.specialize(
                ImmutableMap.of("A", bType, "B", vType), ImmutableList.of(bType.getTypeSignature(), vType.getTypeSignature()),
                context.getTypeManager(), context.getFunctionRegistry()).getMethodHandle();
        return ImmutableList.of(lowerBoundTestMethodHandle, upperBoundTestMethodHandle);
    }

    public static boolean betweenShortShortShort(long v, long a, long b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
    }

    public static boolean betweenShortShortLong(long v, long a, Slice b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
    }

    public static boolean betweenShortLongShort(long v, Slice a, long b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
    }

    public static boolean betweenShortLongLong(long v, Slice a, Slice b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
    }

    public static boolean betweenLongShortShort(Slice v, long a, long b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
    }

    public static boolean betweenLongShortLong(Slice v, long a, Slice b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
    }

    public static boolean betweenLongLongShort(Slice v, Slice a, long b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
    }

    public static boolean betweenLongLongLong(Slice v, Slice a, Slice b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
            throws Throwable
    {
        return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
    }
}
