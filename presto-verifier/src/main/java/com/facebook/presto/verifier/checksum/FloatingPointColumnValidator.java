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

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.String.format;

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
        Expression doubleColumn = column.getType().equals(DOUBLE) ? column.getIdentifier() : new Cast(column.getIdentifier(), DOUBLE.getDisplayName());
        Expression positiveInfinity = new FunctionCall(QualifiedName.of("infinity"), ImmutableList.of());
        Expression negativeInfinity = new ArithmeticUnaryExpression(MINUS, positiveInfinity);

        return ImmutableList.of(
                new SingleColumn(
                        new FunctionCall(
                                QualifiedName.of("sum"),
                                Optional.empty(),
                                Optional.of(new FunctionCall(QualifiedName.of("is_finite"), ImmutableList.of(column.getIdentifier()))),
                                Optional.empty(),
                                false,
                                ImmutableList.of(doubleColumn)),
                        Optional.of(delimitedIdentifier(getSumColumnAlias(column)))),
                new SingleColumn(
                        new FunctionCall(
                                QualifiedName.of("count"),
                                Optional.empty(),
                                Optional.of(new FunctionCall(QualifiedName.of("is_nan"), ImmutableList.of(column.getIdentifier()))),
                                Optional.empty(),
                                false,
                                ImmutableList.of(column.getIdentifier())),
                        Optional.of(delimitedIdentifier(getNanCountColumnAlias(column)))),
                new SingleColumn(
                        new FunctionCall(
                                QualifiedName.of("count"),
                                Optional.empty(),
                                Optional.of(new ComparisonExpression(EQUAL, column.getIdentifier(), positiveInfinity)),
                                Optional.empty(),
                                false,
                                ImmutableList.of(column.getIdentifier())),
                        Optional.of(delimitedIdentifier(getPositiveInfinityCountColumnAlias(column)))),
                new SingleColumn(
                        new FunctionCall(
                                QualifiedName.of("count"),
                                Optional.empty(),
                                Optional.of(new ComparisonExpression(EQUAL, column.getIdentifier(), negativeInfinity)),
                                Optional.empty(),
                                false,
                                ImmutableList.of(column.getIdentifier())),
                        Optional.of(delimitedIdentifier(getNegativeInfinityCountColumnAlias(column)))));
    }

    @Override
    public ColumnMatchResult validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        checkArgument(
                controlResult.getRowCount() == testResult.getRowCount(),
                "Test row count (%s) does not match control row count (%s)",
                testResult.getRowCount(),
                controlResult.getRowCount());

        String sumColumnAlias = getSumColumnAlias(column);
        String nanCountColumnAlias = getNanCountColumnAlias(column);
        String positiveInfinityCountColumnAlias = getPositiveInfinityCountColumnAlias(column);
        String negativeInfinityCountColumnAlias = getNegativeInfinityCountColumnAlias(column);

        long controlNanCount = (long) controlResult.getChecksum(nanCountColumnAlias);
        long testNanCount = (long) testResult.getChecksum(nanCountColumnAlias);
        long controlPositiveInfinityCount = (long) controlResult.getChecksum(positiveInfinityCountColumnAlias);
        long testPositiveInfinityCount = (long) testResult.getChecksum(positiveInfinityCountColumnAlias);
        long controlNegativeInfinityCount = (long) controlResult.getChecksum(negativeInfinityCountColumnAlias);
        long testNegativeInfinityCount = (long) testResult.getChecksum(negativeInfinityCountColumnAlias);
        if (!Objects.equals(controlNanCount, testNanCount) ||
                !Objects.equals(controlPositiveInfinityCount, testPositiveInfinityCount) ||
                !Objects.equals(controlNegativeInfinityCount, testNegativeInfinityCount)) {
            return new ColumnMatchResult(
                    false,
                    format(
                            "control(NaN: %s, +infinity: %s, -infinity: %s) test(NaN: %s, +infinity: %s, -infinity: %s)",
                            controlNanCount,
                            controlPositiveInfinityCount,
                            controlNegativeInfinityCount,
                            testNanCount,
                            testPositiveInfinityCount,
                            testNegativeInfinityCount));
        }

        Object controlSumObject = controlResult.getChecksum(sumColumnAlias);
        Object testSumObject = testResult.getChecksum(sumColumnAlias);
        if (controlSumObject == null || testSumObject == null) {
            return new ColumnMatchResult(
                    controlSumObject == null && testSumObject == null,
                    format("control(sum: %s) test(sum: %s)", controlSumObject, testSumObject));
        }

        // Implementation according to http://floating-point-gui.de/errors/comparison/
        double controlSum = (double) controlSumObject;
        double testSum = (double) testSumObject;

        // Fail if either sum is NaN or Infinity
        if (isNaN(controlSum) || isNaN(testSum) || isInfinite(controlSum) || isInfinite(testSum)) {
            return new ColumnMatchResult(
                    false,
                    format("control(sum: %s) test(sum: %s)", controlSum, testSum));
        }

        // Use absolute error margin if either control sum or test sum is 0
        if (controlSum == 0 || testSum == 0) {
            double controlMean = controlSum / controlResult.getRowCount();
            double testMean = testSum / controlResult.getRowCount();
            double difference = abs(controlMean - testMean);
            return new ColumnMatchResult(
                    difference < absoluteErrorMargin,
                    format("control(mean: %s) test(mean: %s) difference: %s", controlMean, testMean, difference));
        }

        // Use relative error margin for the common cases
        double difference = abs(controlSum - testSum);
        double relativeError = difference / min((abs(controlSum) + abs(testSum)) / 2, Double.MAX_VALUE);
        return new ColumnMatchResult(
                relativeError < relativeErrorMargin,
                format("control(sum: %s) test(sum: %s) relative error: %s", controlSum, testSum, relativeError));
    }

    private static String getSumColumnAlias(Column column)
    {
        return column.getName() + "_sum";
    }

    private static String getNanCountColumnAlias(Column column)
    {
        return column.getName() + "_nan_count";
    }

    private static String getPositiveInfinityCountColumnAlias(Column column)
    {
        return column.getName() + "_pos_inf_count";
    }

    private static String getNegativeInfinityCountColumnAlias(Column column)
    {
        return column.getName() + "_neg_inf_count";
    }
}
