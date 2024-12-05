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

import com.facebook.presto.common.type.AbstractVarcharType;
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
    private final boolean validateStringAsDouble;

    @Inject
    public ArrayColumnValidator(VerifierConfig config, FloatingPointColumnValidator floatingPointValidator)
    {
        this.floatingPointValidator = requireNonNull(floatingPointValidator, "floatingPointValidator is null");
        this.useErrorMarginForFloatingPointArrays = config.isUseErrorMarginForFloatingPointArrays();
        this.validateStringAsDouble = config.isValidateStringAsDouble();
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        Type columnType = column.getType();
        ImmutableList.Builder<SingleColumn> builder = ImmutableList.builder();

        // coalesce(checksum(try(array_sort(array_column))), checksum(array_column))
        Expression checksum = generateArrayChecksum(column.getExpression(), columnType);
        // checksum(cardinality(array_column))
        Expression arrayCardinalityChecksum = functionCall("checksum", functionCall("cardinality", column.getExpression()));
        // coalesce(sum(cardinality(array_column)), 0)
        Expression arrayCardinalitySum = new CoalesceExpression(
                functionCall("sum", functionCall("cardinality", column.getExpression())), new LongLiteral("0"));

        // For arrays of floating point numbers we have a different processing, akin to FloatingPointColumnValidator.
        if (useFloatingPointPath(column)) {
            builder.addAll(generateFloatingPointArrayChecksumColumns(column));
        }
        else if (useStringAsDoublePath(column)) {
            builder.add(new SingleColumn(checksum, delimitedIdentifier(getChecksumColumnAlias(column))));
            builder.addAll(generateStringArrayChecksumColumns(column));
        }
        else {
            builder.add(new SingleColumn(checksum, delimitedIdentifier(getChecksumColumnAlias(column))));
        }
        builder.add(new SingleColumn(arrayCardinalityChecksum, delimitedIdentifier(getCardinalityChecksumColumnAlias(column))));
        builder.add(new SingleColumn(arrayCardinalitySum, delimitedIdentifier(getCardinalitySumColumnAlias(column))));

        return builder.build();
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
        boolean useStringAsDoublePath = useStringAsDoublePath(column) && ColumnValidatorUtil.isStringAsDoubleColumn(column, controlResult, testResult);

        ArrayColumnChecksum controlChecksum = toColumnChecksum(column, controlResult, useFloatingPointPath, useStringAsDoublePath);
        ArrayColumnChecksum testChecksum = toColumnChecksum(column, testResult, useFloatingPointPath, useStringAsDoublePath);

        if (!Objects.equals(controlChecksum.getCardinalityChecksum(), testChecksum.getCardinalityChecksum()) ||
                !Objects.equals(controlChecksum.getCardinalitySum(), testChecksum.getCardinalitySum())) {
            return ImmutableList.of(new ColumnMatchResult<>(false, column, Optional.of("cardinality mismatch"), controlChecksum, testChecksum));
        }

        if (useFloatingPointPath) {
            ColumnMatchResult<FloatingPointColumnChecksum> result =
                    floatingPointValidator.validate(column, controlChecksum.getFloatingPointChecksum(), testChecksum.getFloatingPointChecksum());
            return ImmutableList.of(new ColumnMatchResult<>(result.isMatched(), column, result.getMessage(), controlChecksum, testChecksum));
        }

        if (useStringAsDoublePath) {
            Column asDoubleArrayColumn = getAsDoubleArrayColumn(column);
            ColumnMatchResult<FloatingPointColumnChecksum> result =
                    floatingPointValidator.validate(asDoubleArrayColumn, controlChecksum.getFloatingPointChecksum(), testChecksum.getFloatingPointChecksum());
            return ImmutableList.of(new ColumnMatchResult<>(result.isMatched(), column, result.getMessage(), controlChecksum, testChecksum));
        }

        return ImmutableList.of(new ColumnMatchResult<>(Objects.equals(controlChecksum, testChecksum), column, controlChecksum, testChecksum));
    }

    public static List<SingleColumn> generateFloatingPointArrayChecksumColumns(Column column)
    {
        checkArgument(column.getType() instanceof ArrayType, "Expect ArrayType, found %s", column.getType().getDisplayName());
        Type elementType = ((ArrayType) column.getType()).getElementType();
        checkArgument(Column.FLOATING_POINT_TYPES.contains(elementType), "Expect Double or Real, found %s", elementType.getDisplayName());

        Expression expression = elementType.equals(DOUBLE) ? column.getExpression() : new Cast(column.getExpression(), new ArrayType(DOUBLE).getDisplayName());

        // sum(array_sum(filter(array_column, x -> is_finite(x))))
        Expression sum = functionCall(
                "sum",
                functionCall("array_sum", functionCall("filter", expression, generateLambdaExpression("is_finite"))));
        // sum(cardinality(filter(array_column, x -> is_nan(x))))
        Expression nanCount = functionCall(
                "sum",
                functionCall("cardinality", functionCall("filter", expression, generateLambdaExpression("is_nan"))));
        // sum(cardinality(filter(array_column, x -> x = Infinite())))
        Expression posInfCount = functionCall(
                "sum",
                functionCall("cardinality", functionCall("filter", expression, generateInfinityLambdaExpression(ArithmeticUnaryExpression.Sign.PLUS))));
        // sum(cardinality(filter(array_column, x -> x = -Infinite())))
        Expression negInfCount = functionCall(
                "sum",
                functionCall("cardinality", functionCall("filter", expression, generateInfinityLambdaExpression(ArithmeticUnaryExpression.Sign.MINUS))));
        return ImmutableList.of(
                new SingleColumn(sum, Optional.of(delimitedIdentifier(FloatingPointColumnValidator.getSumColumnAlias(column)))),
                new SingleColumn(nanCount, Optional.of(delimitedIdentifier(FloatingPointColumnValidator.getNanCountColumnAlias(column)))),
                new SingleColumn(posInfCount, Optional.of(delimitedIdentifier(FloatingPointColumnValidator.getPositiveInfinityCountColumnAlias(column)))),
                new SingleColumn(negInfCount, Optional.of(delimitedIdentifier(FloatingPointColumnValidator.getNegativeInfinityCountColumnAlias(column)))));
    }

    public static List<SingleColumn> generateStringArrayChecksumColumns(Column column)
    {
        checkArgument(column.getType() instanceof ArrayType, "Expect ArrayType, found %s", column.getType().getDisplayName());
        Type elementType = ((ArrayType) column.getType()).getElementType();
        checkArgument(elementType instanceof AbstractVarcharType, "Expect VarcharType, found %s", elementType.getDisplayName());

        Column asDoubleArrayColumn = getAsDoubleArrayColumn(column);
        return ImmutableList.<SingleColumn>builder()
                .addAll(generateFloatingPointArrayChecksumColumns(asDoubleArrayColumn))
                .addAll(ColumnValidatorUtil.generateNullCountColumns(column, asDoubleArrayColumn))
                .build();
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

    private static ArrayColumnChecksum toColumnChecksum(Column column, ChecksumResult checksumResult, boolean useFloatingPointPath, boolean useStringAsDoublePath)
    {
        if (checksumResult.getRowCount() == 0) {
            return new ArrayColumnChecksum(
                    null, null, 0,
                    useFloatingPointPath || useStringAsDoublePath ? Optional.of(new FloatingPointColumnChecksum(null, 0, 0, 0, 0)) : Optional.empty());
        }

        Object cardinalityChecksum = checksumResult.getChecksum(getCardinalityChecksumColumnAlias(column));
        long cardinalitySum = (long) checksumResult.getChecksum(getCardinalitySumColumnAlias(column));

        if (useFloatingPointPath) {
            return new ArrayColumnChecksum(
                    null,
                    cardinalityChecksum,
                    cardinalitySum,
                    Optional.of(FloatingPointColumnValidator.toColumnChecksum(column, checksumResult, checksumResult.getRowCount())));
        }

        Object checksum = checksumResult.getChecksum(getChecksumColumnAlias(column));

        if (useStringAsDoublePath) {
            Column asDoubleArrayColumn = getAsDoubleArrayColumn(column);
            return new ArrayColumnChecksum(
                    null,
                    cardinalityChecksum,
                    cardinalitySum,
                    Optional.of(FloatingPointColumnValidator.toColumnChecksum(asDoubleArrayColumn, checksumResult, checksumResult.getRowCount())));
        }

        return new ArrayColumnChecksum(checksum, cardinalityChecksum, cardinalitySum, Optional.empty());
    }

    private boolean useFloatingPointPath(Column column)
    {
        Type columnType = column.getType();
        checkArgument(columnType instanceof ArrayType, "Expect ArrayType, found %s", columnType.getDisplayName());
        Type elementType = ((ArrayType) columnType).getElementType();
        return (useErrorMarginForFloatingPointArrays && Column.FLOATING_POINT_TYPES.contains(elementType));
    }

    private boolean useStringAsDoublePath(Column column)
    {
        Type columnType = column.getType();
        checkArgument(columnType instanceof ArrayType, "Expect ArrayType, found %s", columnType.getDisplayName());
        Type elementType = ((ArrayType) columnType).getElementType();
        return (validateStringAsDouble && elementType instanceof AbstractVarcharType);
    }

    public static Column getAsDoubleArrayColumn(Column column)
    {
        return Column.create(column.getName() + "_as_double", getAsDoubleArrayExpression(column), new ArrayType(DOUBLE));
    }

    private static Expression getAsDoubleArrayExpression(Column column)
    {
        // transform(array_column, x -> try_cast(x as double))
        return functionCall("transform", column.getExpression(), new LambdaExpression(ImmutableList.of(new LambdaArgumentDeclaration(identifier("x"))),
                new Cast(identifier("x"), DOUBLE.getDisplayName(), true, false)));
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
