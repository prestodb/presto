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
package io.prestosql.type;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.PolymorphicScalarFunctionBuilder;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.UnscaledDecimal128Arithmetic;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.PolymorphicScalarFunctionBuilder.constant;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.function.OperatorType.BETWEEN;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.compare;
import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Arrays.asList;

public class DecimalInequalityOperators
{
    private static final TypeSignature DECIMAL_SIGNATURE = parseTypeSignature("decimal(a_precision, a_scale)", ImmutableSet.of("a_precision", "a_scale"));

    private static final MethodHandle IS_RESULT_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultEqual", int.class);
    private static final MethodHandle IS_RESULT_NOT_EQUAL = methodHandle(DecimalInequalityOperators.class, "isResultNotEqual", int.class);
    private static final MethodHandle IS_RESULT_LESS_THAN = methodHandle(DecimalInequalityOperators.class, "isResultLessThan", int.class);
    private static final MethodHandle IS_RESULT_LESS_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "isResultLessThanOrEqual", int.class);
    private static final MethodHandle IS_RESULT_GREATER_THAN = methodHandle(DecimalInequalityOperators.class, "isResultGreaterThan", int.class);
    private static final MethodHandle IS_RESULT_GREATER_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "isResultGreaterThanOrEqual", int.class);

    public static final SqlScalarFunction DECIMAL_EQUAL_OPERATOR = equalityOperator(EQUAL, IS_RESULT_EQUAL);
    public static final SqlScalarFunction DECIMAL_NOT_EQUAL_OPERATOR = equalityOperator(NOT_EQUAL, IS_RESULT_NOT_EQUAL);
    public static final SqlScalarFunction DECIMAL_LESS_THAN_OPERATOR = comparisonOperator(LESS_THAN, IS_RESULT_LESS_THAN);
    public static final SqlScalarFunction DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR = comparisonOperator(LESS_THAN_OR_EQUAL, IS_RESULT_LESS_THAN_OR_EQUAL);
    public static final SqlScalarFunction DECIMAL_GREATER_THAN_OPERATOR = comparisonOperator(GREATER_THAN, IS_RESULT_GREATER_THAN);
    public static final SqlScalarFunction DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR = comparisonOperator(GREATER_THAN_OR_EQUAL, IS_RESULT_GREATER_THAN_OR_EQUAL);
    public static final SqlScalarFunction DECIMAL_BETWEEN_OPERATOR = betweenOperator();
    public static final SqlScalarFunction DECIMAL_DISTINCT_FROM_OPERATOR = distinctOperator();

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

    private static PolymorphicScalarFunctionBuilder makeBinaryOperatorFunctionBuilder(OperatorType operatorType)
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(operatorType)
                .argumentTypes(DECIMAL_SIGNATURE, DECIMAL_SIGNATURE)
                .returnType(parseTypeSignature(BOOLEAN))
                .build();
        return SqlScalarFunction.builder(DecimalInequalityOperators.class)
                .signature(signature)
                .deterministic(true);
    }

    private static SqlScalarFunction equalityOperator(OperatorType operatorType, MethodHandle getResultMethodHandle)
    {
        return makeBinaryOperatorFunctionBuilder(operatorType)
                .choice(choice -> choice
                        .nullableResult(true)
                        .implementation(methodsGroup -> methodsGroup
                                .methods("boxedShortShort", "boxedLongLong")
                                .withExtraParameters(constant(getResultMethodHandle))))
                .build();
    }

    private static SqlScalarFunction comparisonOperator(OperatorType operatorType, MethodHandle getResultMethodHandle)
    {
        return makeBinaryOperatorFunctionBuilder(operatorType)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("primitiveShortShort", "primitiveLongLong")
                                .withExtraParameters(constant(getResultMethodHandle))))
                .build();
    }

    @UsedByGeneratedCode
    public static Boolean boxedShortShort(long a, long b, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, Long.compare(a, b));
    }

    @UsedByGeneratedCode
    public static Boolean boxedLongLong(Slice left, Slice right, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, compare(left, right));
    }

    @UsedByGeneratedCode
    //TODO: remove when introducing nullable comparisons (<=, <, >, >=)
    public static boolean primitiveShortShort(long a, long b, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, Long.compare(a, b));
    }

    @UsedByGeneratedCode
    //TODO: remove when introducing nullable comparisons (<=, <, >, >=)
    public static boolean primitiveLongLong(Slice left, Slice right, MethodHandle getResultMethodHandle)
    {
        return invokeGetResult(getResultMethodHandle, compare(left, right));
    }

    private static SqlScalarFunction distinctOperator()
    {
        return makeBinaryOperatorFunctionBuilder(IS_DISTINCT_FROM)
                .choice(choice -> choice
                        .argumentProperties(
                                valueTypeArgumentProperty(USE_NULL_FLAG),
                                valueTypeArgumentProperty(USE_NULL_FLAG))
                        .implementation(methodsGroup -> methodsGroup
                                .methods("distinctShortShort", "distinctLongLong")))
                .choice(choice -> choice
                        .argumentProperties(
                                valueTypeArgumentProperty(BLOCK_AND_POSITION),
                                valueTypeArgumentProperty(BLOCK_AND_POSITION))
                        .implementation(methodsGroup -> methodsGroup
                                .methodWithExplicitJavaTypes("distinctBlockPositionLongLong",
                                        asList(Optional.of(Slice.class), Optional.of(Slice.class)))
                                .methodWithExplicitJavaTypes("distinctBlockPositionShortShort",
                                        asList(Optional.of(long.class), Optional.of(long.class)))))
                .build();
    }

    @UsedByGeneratedCode
    public static boolean distinctBlockPositionLongLong(Block left, int leftPosition, Block right, int rightPosition)
    {
        if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
            return true;
        }
        if (left.isNull(leftPosition)) {
            return false;
        }

        long leftLow = left.getLong(leftPosition, 0);
        long leftHigh = left.getLong(leftPosition, SIZE_OF_LONG);
        long rightLow = left.getLong(rightPosition, 0);
        long rightHigh = left.getLong(rightPosition, SIZE_OF_LONG);
        return UnscaledDecimal128Arithmetic.compare(leftLow, leftHigh, rightLow, rightHigh) != 0;
    }

    @UsedByGeneratedCode
    public static boolean distinctBlockPositionShortShort(Block left, int leftPosition, Block right, int rightPosition)
    {
        if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
            return true;
        }
        if (left.isNull(leftPosition)) {
            return false;
        }

        long leftValue = left.getLong(leftPosition, 0);
        long rightValue = right.getLong(rightPosition, 0);
        return Long.compare(leftValue, rightValue) != 0;
    }

    @UsedByGeneratedCode
    public static boolean distinctShortShort(long left, boolean leftNull, long right, boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return primitiveShortShort(left, right, IS_RESULT_NOT_EQUAL);
    }

    @UsedByGeneratedCode
    public static boolean distinctLongLong(Slice left, boolean leftNull, Slice right, boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return primitiveLongLong(left, right, IS_RESULT_NOT_EQUAL);
    }

    private static boolean invokeGetResult(MethodHandle getResultMethodHandle, int comparisonResult)
    {
        try {
            return (boolean) getResultMethodHandle.invokeExact(comparisonResult);
        }
        catch (Throwable t) {
            throwIfInstanceOf(t, Error.class);
            throwIfInstanceOf(t, PrestoException.class);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    private static SqlScalarFunction betweenOperator()
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(BETWEEN)
                .argumentTypes(DECIMAL_SIGNATURE, DECIMAL_SIGNATURE, DECIMAL_SIGNATURE)
                .returnType(parseTypeSignature(BOOLEAN))
                .build();
        return SqlScalarFunction.builder(DecimalInequalityOperators.class)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
                                .methods("betweenShortShortShort", "betweenLongLongLong")))
                .build();
    }

    @UsedByGeneratedCode
    public static boolean betweenShortShortShort(long value, long low, long high)
    {
        return low <= value && value <= high;
    }

    @UsedByGeneratedCode
    public static boolean betweenLongLongLong(Slice value, Slice low, Slice high)
    {
        return compare(low, value) <= 0 && compare(value, high) <= 0;
    }
}
