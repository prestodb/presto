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
package com.facebook.presto.verifier.checksum;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.QueryUtil.functionCall;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ArrayColumnValidator
        implements ColumnValidator
{
    private final FloatingPointColumnValidator floatingPointValidator;
    private final boolean useErrorMarginForFloatingPointArrays;

    @Inject
    public ArrayColumnValidator(VerifierConfig config, FloatingPointColumnValidator floatingPointValidator)
    {
        this.floatingPointValidator = requireNonNull(floatingPointValidator, "floatingPointValidator is null");
        this.useErrorMarginForFloatingPointArrays = config.isUseErrorMarginForFloatingPointArrays();
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        Type columnType = column.getType();
        boolean useFloatingPointPath = useFloatingPointPath(column);

        // For arrays of floating point numbers we have a different processing, akin to FloatingPointColumnValidator.
        if (useFloatingPointPath) {
            Type elementType = ((ArrayType) columnType).getElementType();
            Expression expression = elementType.equals(DOUBLE) ? column.getExpression() : new Cast(column.getExpression(), new ArrayType(DOUBLE).getDisplayName());

            Expression sum = functionCall(
                    "sum",
                    functionCall("array_sum", functionCall("filter", expression, generateLambdaExpression("is_finite"))));
            Expression nanCount = functionCall(
                    "sum",
                    functionCall("cardinality", functionCall("filter", expression, generateLambdaExpression("is_nan"))));
            Expression posInfCount = functionCall(
                    "sum",
                    functionCall("cardinality", functionCall("filter", expression, generateInfinityLambdaExpression(ArithmeticUnaryExpression.Sign.PLUS))));
            Expression negInfCount = functionCall(
                    "sum",
                    functionCall("cardinality", functionCall("filter", expression, generateInfinityLambdaExpression(ArithmeticUnaryExpression.Sign.MINUS))));
            Expression arrayCardinalityChecksum = functionCall("checksum", functionCall("cardinality", expression));
            Expression arrayCardinalitySum = new CoalesceExpression(
                    functionCall("sum", functionCall("cardinality", expression)), new LongLiteral("0"));
            return ImmutableList.of(
                    new SingleColumn(sum, Optional.of(delimitedIdentifier(FloatingPointColumnValidator.getSumColumnAlias(column)))),
                    new SingleColumn(nanCount, Optional.of(delimitedIdentifier(FloatingPointColumnValidator.getNanCountColumnAlias(column)))),
                    new SingleColumn(posInfCount, Optional.of(delimitedIdentifier(FloatingPointColumnValidator.getPositiveInfinityCountColumnAlias(column)))),
                    new SingleColumn(negInfCount, Optional.of(delimitedIdentifier(FloatingPointColumnValidator.getNegativeInfinityCountColumnAlias(column)))),
                    new SingleColumn(arrayCardinalityChecksum, Optional.of(delimitedIdentifier(getCardinalityChecksumColumnAlias(column)))),
                    new SingleColumn(arrayCardinalitySum, Optional.of(delimitedIdentifier(getCardinalitySumColumnAlias(column)))));
        }

        Expression checksum = generateArrayChecksum(column.getExpression(), columnType);
        Expression arrayCardinalityChecksum = functionCall("checksum", functionCall("cardinality", column.getExpression()));
        Expression arrayCardinalitySum = new CoalesceExpression(
                functionCall("sum", functionCall("cardinality", column.getExpression())), new LongLiteral("0"));
        return ImmutableList.of(
                new SingleColumn(checksum, Optional.of(delimitedIdentifier(getChecksumColumnAlias(column)))),
                new SingleColumn(arrayCardinalityChecksum, Optional.of(delimitedIdentifier(getCardinalityChecksumColumnAlias(column)))),
                new SingleColumn(arrayCardinalitySum, Optional.of(delimitedIdentifier(getCardinalitySumColumnAlias(column)))));
    }

    @Override
    public List<ColumnMatchResult<ArrayColumnChecksum>> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        checkArgument(
                controlResult.getRowCount() == testResult.getRowCount(),
                "Test row count (%s) does not match control row count (%s)",
                testResult.getRowCount(),
                controlResult.getRowCount());

        boolean useFloatingPointPath = useFloatingPointPath(column);

        ArrayColumnChecksum controlChecksum = toColumnChecksum(column, controlResult, useFloatingPointPath);
        ArrayColumnChecksum testChecksum = toColumnChecksum(column, testResult, useFloatingPointPath);

        // Non-floating point case.
        if (!useFloatingPointPath) {
            return ImmutableList.of(new ColumnMatchResult<>(Objects.equals(controlChecksum, testChecksum), column, controlChecksum, testChecksum));
        }

        // Check the non-floating point members first.
        if (!Objects.equals(controlChecksum.getCardinalityChecksum(), testChecksum.getCardinalityChecksum()) ||
                !Objects.equals(controlChecksum.getCardinalitySum(), testChecksum.getCardinalitySum())) {
            return ImmutableList.of(new ColumnMatchResult<>(false, column, Optional.of("cardinality mismatch"), controlChecksum, testChecksum));
        }

        ColumnMatchResult<FloatingPointColumnChecksum> result =
                floatingPointValidator.validate(column, controlChecksum.getFloatingPointChecksum(), testChecksum.getFloatingPointChecksum());
        return ImmutableList.of(new ColumnMatchResult<>(result.isMatched(), column, result.getMessage(), controlChecksum, testChecksum));
    }

    public static Expression generateArrayChecksum(Expression column, Type type)
    {
        checkArgument(type instanceof ArrayType, "Expect ArrayType, found %s", type.getDisplayName());
        Type elementType = ((ArrayType) type).getElementType();

        if (elementType.isOrderable()) {
            Expression arraySort = functionCall("array_sort", column);

            if (elementType instanceof ArrayType || elementType instanceof RowType) {
                return new CoalesceExpression(
                        functionCall("checksum", new TryExpression(arraySort)),
                        functionCall("checksum", column));
            }
            return functionCall("checksum", arraySort);
        }
        return functionCall("checksum", column);
    }

    private static Expression generateInfinityLambdaExpression(ArithmeticUnaryExpression.Sign sign)
    {
        ComparisonExpression lambdaBody = new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new Identifier("x"),
                new ArithmeticUnaryExpression(sign, functionCall("infinity")));
        return new LambdaExpression(ImmutableList.of(new LambdaArgumentDeclaration(identifier("x"))), lambdaBody);
    }

    private static Expression generateLambdaExpression(String functionName)
    {
        return new LambdaExpression(
                ImmutableList.of(new LambdaArgumentDeclaration(identifier("x"))),
                functionCall(functionName, new Identifier("x")));
    }

    private static ArrayColumnChecksum toColumnChecksum(Column column, ChecksumResult checksumResult, boolean useFloatingPointPath)
    {
        if (!useFloatingPointPath) {
            return new ArrayColumnChecksum(
                    checksumResult.getChecksum(getChecksumColumnAlias(column)),
                    checksumResult.getChecksum(getCardinalityChecksumColumnAlias(column)),
                    (long) checksumResult.getChecksum(getCardinalitySumColumnAlias(column)),
                    Optional.empty());
        }

        // Case for an empty result table, when some aggregation return nulls.
        Object nanCount = checksumResult.getChecksum(FloatingPointColumnValidator.getNanCountColumnAlias(column));
        if (Objects.isNull(nanCount)) {
            return new ArrayColumnChecksum(
                    null,
                    null,
                    0,
                    Optional.of(new FloatingPointColumnChecksum(null, 0, 0, 0, 0)));
        }

        long cardinalitySum = (long) checksumResult.getChecksum(getCardinalitySumColumnAlias(column));
        return new ArrayColumnChecksum(
                null,
                checksumResult.getChecksum(getCardinalityChecksumColumnAlias(column)),
                cardinalitySum,
                Optional.of(new FloatingPointColumnChecksum(
                        checksumResult.getChecksum(FloatingPointColumnValidator.getSumColumnAlias(column)),
                        (long) nanCount,
                        (long) checksumResult.getChecksum(FloatingPointColumnValidator.getPositiveInfinityCountColumnAlias(column)),
                        (long) checksumResult.getChecksum(FloatingPointColumnValidator.getNegativeInfinityCountColumnAlias(column)),
                        cardinalitySum)));
    }

    private boolean useFloatingPointPath(Column column)
    {
        Type columnType = column.getType();
        checkArgument(columnType instanceof ArrayType, "Expect ArrayType, found %s", columnType.getDisplayName());
        Type elementType = ((ArrayType) columnType).getElementType();
        return (useErrorMarginForFloatingPointArrays && Column.FLOATING_POINT_TYPES.contains(elementType));
    }

    private static String getChecksumColumnAlias(Column column)
    {
        return column.getName() + "$checksum";
    }

    private static String getCardinalityChecksumColumnAlias(Column column)
    {
        return column.getName() + "$cardinality_checksum";
    }

    private static String getCardinalitySumColumnAlias(Column column)
    {
        return column.getName() + "$cardinality_sum";
    }
}
