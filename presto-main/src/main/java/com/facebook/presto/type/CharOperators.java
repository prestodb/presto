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
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public final class CharOperators
{
    public static final SqlScalarFunction CHAR_EQUAL_NO_PAD = charLogicalOperator(EQUAL, "equalNoPad");
    public static final SqlScalarFunction CHAR_NOT_EQUAL_NO_PAD = charLogicalOperator(NOT_EQUAL, "notEqualNoPad");
    public static final SqlScalarFunction CHAR_LESS_THAN_NO_PAD = charLogicalOperator(LESS_THAN, "lessThanNoPad");
    public static final SqlScalarFunction CHAR_LESS_THAN_OR_EQUAL_NO_PAD = charLogicalOperator(LESS_THAN_OR_EQUAL, "lessThanOrEqualNoPad");
    public static final SqlScalarFunction CHAR_GREATER_THAN_NO_PAD = charLogicalOperator(GREATER_THAN, "greaterThanNoPad");
    public static final SqlScalarFunction CHAR_GREATER_THAN_OR_EQUAL_NO_PAD = charLogicalOperator(GREATER_THAN_OR_EQUAL, "greaterThanOrEqualNoPad");
    public static final SqlScalarFunction CHAR_BETWEEN_NO_PAD = charBetweenOperator("betweenNoPad");

    public static final SqlScalarFunction CHAR_EQUAL_PAD_SPACES = charLogicalOperator(EQUAL, "equalPadSpaces");
    public static final SqlScalarFunction CHAR_NOT_EQUAL_PAD_SPACES = charLogicalOperator(NOT_EQUAL, "notEqualPadSpaces");
    public static final SqlScalarFunction CHAR_LESS_THAN_PAD_SPACES = charLogicalOperator(LESS_THAN, "lessThanPadSpaces");
    public static final SqlScalarFunction CHAR_LESS_THAN_OR_EQUAL_PAD_SPACES = charLogicalOperator(LESS_THAN_OR_EQUAL, "lessThanOrEqualPadSpaces");
    public static final SqlScalarFunction CHAR_GREATER_THAN_PAD_SPACES = charLogicalOperator(GREATER_THAN, "greaterThanPadSpaces");
    public static final SqlScalarFunction CHAR_GREATER_THAN_OR_EQUAL_PAD_SPACES = charLogicalOperator(GREATER_THAN_OR_EQUAL, "greaterThanOrEqualPadSpaces");
    public static final SqlScalarFunction CHAR_BETWEEN_PAD_SPACES = charBetweenOperator("betweenPadSpaces");

    private CharOperators() {}

    private static SqlScalarFunction charLogicalOperator(OperatorType operatorType, String... methods)
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(operatorType)
                .argumentTypes(
                        parseTypeSignature("char(x)", ImmutableSet.of("x")),
                        parseTypeSignature("char(y)", ImmutableSet.of("y")))
                .returnType(parseTypeSignature(BOOLEAN))
                .build();
        return SqlScalarFunction.builder(CharOperators.class)
                .signature(signature)
                .implementation(b -> b
                        .methods(methods)
                        .withExtraParameters(context -> ImmutableList.of(
                                context.getLiteral("x"),
                                context.getLiteral("y"))))
                .build();
    }

    private static SqlScalarFunction charBetweenOperator(String... methods)
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(BETWEEN)
                .argumentTypes(
                        parseTypeSignature("char(x)", ImmutableSet.of("x")),
                        parseTypeSignature("char(y)", ImmutableSet.of("y")),
                        parseTypeSignature("char(z)", ImmutableSet.of("z")))
                .returnType(parseTypeSignature(BOOLEAN))
                .build();
        return SqlScalarFunction.builder(CharOperators.class)
                .signature(signature)
                .implementation(b -> b
                        .methods(methods)
                        .withExtraParameters(context -> ImmutableList.of(
                                context.getLiteral("x"),
                                context.getLiteral("y"),
                                context.getLiteral("z"))))
                .build();
    }

    @UsedByGeneratedCode
    public static boolean equalNoPad(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return left.equals(right) && leftTypeLength == rightTypeLength;
    }

    @UsedByGeneratedCode
    public static boolean notEqualNoPad(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return !equalNoPad(left, right, leftTypeLength, rightTypeLength);
    }

    @UsedByGeneratedCode
    public static boolean lessThanNoPad(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return compareNoPad(left, right, leftTypeLength, rightTypeLength) < 0;
    }

    @UsedByGeneratedCode
    public static boolean lessThanOrEqualNoPad(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return compareNoPad(left, right, leftTypeLength, rightTypeLength) <= 0;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanNoPad(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return compareNoPad(left, right, leftTypeLength, rightTypeLength) > 0;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanOrEqualNoPad(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return compareNoPad(left, right, leftTypeLength, rightTypeLength) >= 0;
    }

    @UsedByGeneratedCode
    public static boolean betweenNoPad(Slice value, Slice min, Slice max, long valueTypeLength, long minTypeLength, long maxTypeLength)
    {
        return compareNoPad(min, value, minTypeLength, valueTypeLength) <= 0
                && compareNoPad(value, max, valueTypeLength, maxTypeLength) <= 0;
    }

    private static int compareNoPad(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        int compareResult = left.compareTo(right);
        if (compareResult != 0) {
            return compareResult;
        }

        return (int) (leftTypeLength - rightTypeLength);
    }

    @UsedByGeneratedCode
    public static boolean equalPadSpaces(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return left.equals(right);
    }

    @UsedByGeneratedCode
    public static boolean notEqualPadSpaces(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return !left.equals(right);
    }

    @UsedByGeneratedCode
    public static boolean lessThanPadSpaces(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return left.compareTo(right) < 0;
    }

    @UsedByGeneratedCode
    public static boolean lessThanOrEqualPadSpaces(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return left.compareTo(right) <= 0;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanPadSpaces(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return left.compareTo(right) > 0;
    }

    @UsedByGeneratedCode
    public static boolean greaterThanOrEqualPadSpaces(Slice left, Slice right, long leftTypeLength, long rightTypeLength)
    {
        return left.compareTo(right) >= 0;
    }

    @UsedByGeneratedCode
    public static boolean betweenPadSpaces(Slice value, Slice min, Slice max, long valueTypeLength, long minTypeLength, long maxTypeLength)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @LiteralParameters("x")
    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType("char(x)") Slice value)
    {
        return XxHash64.hash(value);
    }
}
