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

import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.abs;
import static java.lang.Math.min;

public class FloatingPointColumnValidator
        implements ColumnValidator
{
    private final double relativeErrorMargin;
    private final double absoluteErrorMargin;

    @Inject
    public FloatingPointColumnValidator(VerifierConfig config)
    {
        this.relativeErrorMargin = config.getRelativeErrorMargin();
        this.absoluteErrorMargin = config.getAbsoluteErrorMargin();
    }

    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        Expression doubleColumn = column.getType().equals(DOUBLE) ? column.getExpression() : new Cast(column.getExpression(), DOUBLE.getDisplayName());
        Expression positiveInfinity = new FunctionCall(QualifiedName.of("infinity"), ImmutableList.of());
        Expression negativeInfinity = new ArithmeticUnaryExpression(MINUS, positiveInfinity);

        return ImmutableList.of(
                new SingleColumn(
                        new FunctionCall(
                                QualifiedName.of("sum"),
                                Optional.empty(),
                                Optional.of(new FunctionCall(QualifiedName.of("is_finite"), ImmutableList.of(column.getExpression()))),
                                Optional.empty(),
                                false,
                                ImmutableList.of(doubleColumn)),
                        Optional.of(delimitedIdentifier(getSumColumnAlias(column)))),
                new SingleColumn(
                        new FunctionCall(
                                QualifiedName.of("count"),
                                Optional.empty(),
                                Optional.of(new FunctionCall(QualifiedName.of("is_nan"), ImmutableList.of(column.getExpression()))),
                                Optional.empty(),
                                false,
                                ImmutableList.of(column.getExpression())),
                        Optional.of(delimitedIdentifier(getNanCountColumnAlias(column)))),
                new SingleColumn(
                        new FunctionCall(
                                QualifiedName.of("count"),
                                Optional.empty(),
                                Optional.of(new ComparisonExpression(EQUAL, column.getExpression(), positiveInfinity)),
                                Optional.empty(),
                                false,
                                ImmutableList.of(column.getExpression())),
                        Optional.of(delimitedIdentifier(getPositiveInfinityCountColumnAlias(column)))),
                new SingleColumn(
                        new FunctionCall(
                                QualifiedName.of("count"),
                                Optional.empty(),
                                Optional.of(new ComparisonExpression(EQUAL, column.getExpression(), negativeInfinity)),
                                Optional.empty(),
                                false,
                                ImmutableList.of(column.getExpression())),
                        Optional.of(delimitedIdentifier(getNegativeInfinityCountColumnAlias(column)))));
    }

    @Override
    public List<ColumnMatchResult<FloatingPointColumnChecksum>> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        checkArgument(
                controlResult.getRowCount() == testResult.getRowCount(),
                "Test row count (%s) does not match control row count (%s)",
                testResult.getRowCount(),
                controlResult.getRowCount());

        long rowCount = controlResult.getRowCount();

        FloatingPointColumnChecksum controlChecksum = toColumnChecksum(column, controlResult, rowCount);
        FloatingPointColumnChecksum testChecksum = toColumnChecksum(column, testResult, rowCount);
        return ImmutableList.of(validate(column, controlChecksum, testChecksum, rowCount));
    }

    private static FloatingPointColumnChecksum toColumnChecksum(Column column, ChecksumResult checksumResult, long rowCount)
    {
        return new FloatingPointColumnChecksum(
                checksumResult.getChecksum(getSumColumnAlias(column)),
                (long) checksumResult.getChecksum(getNanCountColumnAlias(column)),
                (long) checksumResult.getChecksum(getPositiveInfinityCountColumnAlias(column)),
                (long) checksumResult.getChecksum(getNegativeInfinityCountColumnAlias(column)),
                rowCount);
    }

    private ColumnMatchResult<FloatingPointColumnChecksum> validate(
            Column column,
            FloatingPointColumnChecksum controlChecksum,
            FloatingPointColumnChecksum testChecksum,
            long rowCount)
    {
        if (!Objects.equals(controlChecksum.getNanCount(), testChecksum.getNanCount()) ||
                !Objects.equals(controlChecksum.getPositiveInfinityCount(), testChecksum.getPositiveInfinityCount()) ||
                !Objects.equals(controlChecksum.getNegativeInfinityCount(), testChecksum.getNegativeInfinityCount())) {
            return new ColumnMatchResult<>(false, column, controlChecksum, testChecksum);
        }

        if (controlChecksum.getSum() == null || testChecksum.getSum() == null) {
            return new ColumnMatchResult<>(controlChecksum.getSum() == null && testChecksum.getSum() == null, column, controlChecksum, testChecksum);
        }

        // Implementation according to http://floating-point-gui.de/errors/comparison/
        double controlSum = (double) controlChecksum.getSum();
        double testSum = (double) testChecksum.getSum();

        // Fail if either sum is NaN or Infinity
        if (isNaN(controlSum) || isNaN(testSum) || isInfinite(controlSum) || isInfinite(testSum)) {
            return new ColumnMatchResult<>(false, column, controlChecksum, testChecksum);
        }

        // Use absolute error margin if either control sum or test sum is 0.
        // Row count won't be zero since otherwise controlSum and testSum will both be null, and this has already been handled above.
        double controlMean = controlSum / rowCount;
        double testMean = testSum / rowCount;
        if (abs(controlMean) < absoluteErrorMargin || abs(testMean) < absoluteErrorMargin) {
            return new ColumnMatchResult<>(abs(controlMean) < absoluteErrorMargin && abs(testMean) < absoluteErrorMargin, column, controlChecksum, testChecksum);
        }

        // Use relative error margin for the common cases
        double difference = abs(controlSum - testSum);
        double relativeError = difference / min((abs(controlSum) + abs(testSum)) / 2, Double.MAX_VALUE);
        return new ColumnMatchResult<>(relativeError < relativeErrorMargin, column, Optional.of("relative error: " + relativeError), controlChecksum, testChecksum);
    }

    private static String getSumColumnAlias(Column column)
    {
        return column.getName() + "$sum";
    }

    private static String getNanCountColumnAlias(Column column)
    {
        return column.getName() + "$nan_count";
    }

    private static String getPositiveInfinityCountColumnAlias(Column column)
    {
        return column.getName() + "$pos_inf_count";
    }

    private static String getNegativeInfinityCountColumnAlias(Column column)
    {
        return column.getName() + "$neg_inf_count";
    }
}
