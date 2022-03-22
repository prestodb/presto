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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isNaN;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class TestComparisonStatsCalculator
{
    private FilterStatsCalculator filterStatsCalculator;
    private Session session;
    private PlanNodeStatsEstimate standardInputStatistics;
    private TypeProvider types;
    private VariableStatsEstimate uStats;
    private VariableStatsEstimate wStats;
    private VariableStatsEstimate xStats;
    private VariableStatsEstimate yStats;
    private VariableStatsEstimate zStats;
    private VariableStatsEstimate leftOpenStats;
    private VariableStatsEstimate rightOpenStats;
    private VariableStatsEstimate unknownRangeStats;
    private VariableStatsEstimate emptyRangeStats;
    private VariableStatsEstimate varcharStats;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        session = testSessionBuilder().build();
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        filterStatsCalculator = new FilterStatsCalculator(metadata, new ScalarStatsCalculator(metadata), new StatsNormalizer());

        uStats = VariableStatsEstimate.builder()
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(300)
                .setLowValue(0)
                .setHighValue(20)
                .setNullsFraction(0.1)
                .build();
        wStats = VariableStatsEstimate.builder()
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(30)
                .setLowValue(0)
                .setHighValue(20)
                .setNullsFraction(0.1)
                .build();
        xStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(40.0)
                .setLowValue(-10.0)
                .setHighValue(10.0)
                .setNullsFraction(0.25)
                .build();
        yStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(20.0)
                .setLowValue(0.0)
                .setHighValue(5.0)
                .setNullsFraction(0.5)
                .build();
        zStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(5.0)
                .setLowValue(-100.0)
                .setHighValue(100.0)
                .setNullsFraction(0.1)
                .build();
        leftOpenStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(15.0)
                .setNullsFraction(0.1)
                .build();
        rightOpenStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(-15.0)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        unknownRangeStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        emptyRangeStats = VariableStatsEstimate.builder()
                .setAverageRowSize(0.0)
                .setDistinctValuesCount(0.0)
                .setLowValue(NaN)
                .setHighValue(NaN)
                .setNullsFraction(1.0)
                .build();
        varcharStats = VariableStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        standardInputStatistics = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression("u", DOUBLE), uStats)
                .addVariableStatistics(new VariableReferenceExpression("w", DOUBLE), wStats)
                .addVariableStatistics(new VariableReferenceExpression("x", DOUBLE), xStats)
                .addVariableStatistics(new VariableReferenceExpression("y", DOUBLE), yStats)
                .addVariableStatistics(new VariableReferenceExpression("z", DOUBLE), zStats)
                .addVariableStatistics(new VariableReferenceExpression("leftOpen", DOUBLE), leftOpenStats)
                .addVariableStatistics(new VariableReferenceExpression("rightOpen", DOUBLE), rightOpenStats)
                .addVariableStatistics(new VariableReferenceExpression("unknownRange", DOUBLE), unknownRangeStats)
                .addVariableStatistics(new VariableReferenceExpression("emptyRange", DOUBLE), emptyRangeStats)
                .addVariableStatistics(new VariableReferenceExpression("varchar", VarcharType.createVarcharType(10)), varcharStats)
                .setOutputRowCount(1000.0)
                .build();

        types = TypeProvider.fromVariables(ImmutableList.<VariableReferenceExpression>builder()
                .add(new VariableReferenceExpression("u", DOUBLE))
                .add(new VariableReferenceExpression("w", DOUBLE))
                .add(new VariableReferenceExpression("x", DOUBLE))
                .add(new VariableReferenceExpression("y", DOUBLE))
                .add(new VariableReferenceExpression("z", DOUBLE))
                .add(new VariableReferenceExpression("leftOpen", DOUBLE))
                .add(new VariableReferenceExpression("rightOpen", DOUBLE))
                .add(new VariableReferenceExpression("unknownRange", DOUBLE))
                .add(new VariableReferenceExpression("emptyRange", DOUBLE))
                .add(new VariableReferenceExpression("varchar", VarcharType.createVarcharType(10)))
                .build());
    }

    private Consumer<VariableStatsAssertion> equalTo(VariableStatsEstimate estimate)
    {
        return symbolAssert -> {
            symbolAssert
                    .lowValue(estimate.getLowValue())
                    .highValue(estimate.getHighValue())
                    .distinctValuesCount(estimate.getDistinctValuesCount())
                    .nullsFraction(estimate.getNullsFraction());
        };
    }

    private VariableStatsEstimate updateNDV(VariableStatsEstimate symbolStats, double delta)
    {
        return symbolStats.mapDistinctValuesCount(ndv -> ndv + delta);
    }

    private VariableStatsEstimate capNDV(VariableStatsEstimate symbolStats, double rowCount)
    {
        double ndv = symbolStats.getDistinctValuesCount();
        double nulls = symbolStats.getNullsFraction();
        if (isNaN(ndv) || isNaN(rowCount) || isNaN(nulls)) {
            return symbolStats;
        }
        if (ndv <= rowCount * (1 - nulls)) {
            return symbolStats;
        }
        return symbolStats
                .mapDistinctValuesCount(n -> (min(ndv, rowCount) + rowCount * (1 - nulls)) / 2)
                .mapNullsFraction(n -> nulls / 2);
    }

    private VariableStatsEstimate zeroNullsFraction(VariableStatsEstimate symbolStats)
    {
        return symbolStats.mapNullsFraction(fraction -> 0.0);
    }

    private PlanNodeStatsAssertion assertCalculate(Expression comparisonExpression)
    {
        return PlanNodeStatsAssertion.assertThat(filterStatsCalculator.filterStats(standardInputStatistics, comparisonExpression, session, types));
    }

    @Test
    public void verifyTestInputConsistent()
    {
        // if tests' input is not normalized, other tests don't make sense
        checkConsistent(
                new StatsNormalizer(),
                "standardInputStatistics",
                standardInputStatistics,
                standardInputStatistics.getVariablesWithKnownStatistics(),
                types);
    }

    @Test
    public void symbolToLiteralEqualStats()
    {
        // Simple case
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("y"), new DoubleLiteral("2.5")))
                .outputRowsCount(25.0) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("y", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(2.5)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("x"), new DoubleLiteral("10.0")))
                .outputRowsCount(18.75) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("x", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal out of symbol range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("y"), new DoubleLiteral("10.0")))
                .outputRowsCount(0.0) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("y", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(0.0)
                            .distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Literal in left open range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("leftOpen"), new DoubleLiteral("2.5")))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("leftOpen", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(2.5)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("rightOpen"), new DoubleLiteral("-2.5")))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("rightOpen", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(-2.5)
                            .highValue(-2.5)
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("unknownRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("unknownRange", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(0.0)
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("emptyRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(0.0)
                .variableStats(new VariableReferenceExpression("emptyRange", DOUBLE), equalTo(emptyRangeStats));

        // Column with values not representable as double (unknown range)
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("varchar"), new StringLiteral("blah")))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("varchar", VarcharType.createVarcharType(10)), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(NEGATIVE_INFINITY)
                            .highValue(POSITIVE_INFINITY)
                            .nullsFraction(0.0);
                });
    }

    @Test
    public void symbolToLiteralNotEqualStats()
    {
        // Simple case
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("y"), new DoubleLiteral("2.5")))
                .outputRowsCount(475.0) // all rows minus nulls multiplied by ((distinct values - 1) / distinct values)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(19.0)
                            .lowValue(0.0)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new DoubleLiteral("10.0")))
                .outputRowsCount(731.25) // all rows minus nulls multiplied by ((distinct values - 1) / distinct values)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(39.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal out of symbol range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("y"), new DoubleLiteral("10.0")))
                .outputRowsCount(500.0) // all rows minus nulls
                .variableStats(new VariableReferenceExpression("y", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(19.0)
                            .lowValue(0.0)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Literal in left open range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("leftOpen"), new DoubleLiteral("2.5")))
                .outputRowsCount(882.0) // all rows minus nulls multiplied by ((distinct values - 1) / distinct values)
                .variableStats(new VariableReferenceExpression("leftOpen", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValueUnknown()
                            .highValue(15.0)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("rightOpen"), new DoubleLiteral("-2.5")))
                .outputRowsCount(882.0) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("rightOpen", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValue(-15.0)
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("unknownRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(882.0) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("unknownRange", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValueUnknown()
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("emptyRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(0.0)
                .variableStats(new VariableReferenceExpression("emptyRange", DOUBLE), equalTo(emptyRangeStats));

        // Column with values not representable as double (unknown range)
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("varchar"), new StringLiteral("blah")))
                .outputRowsCount(882.0) // all rows minus nulls divided by distinct values count
                .variableStats(new VariableReferenceExpression("varchar", VarcharType.createVarcharType(10)), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValueUnknown()
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });
    }

    @Test
    public void symbolToLiteralLessThanStats()
    {
        // Simple case
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("y"), new DoubleLiteral("2.5")))
                .outputRowsCount(250.0) // all rows minus nulls times range coverage (50%)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(10.0)
                            .lowValue(0.0)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range included)
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral("10.0")))
                .outputRowsCount(750.0) // all rows minus nulls times range coverage (100%)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(40.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range excluded)
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral("-10.0")))
                .outputRowsCount(18.75) // all rows minus nulls divided by NDV (one value from edge is included as approximation)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(-10.0)
                            .highValue(-10.0)
                            .nullsFraction(0.0);
                });

        // Literal range out of symbol range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("y"), new DoubleLiteral("-10.0")))
                .outputRowsCount(0.0) // all rows minus nulls times range coverage (0%)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(0.0)
                            .distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Literal in left open range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("leftOpen"), new DoubleLiteral("0.0")))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .variableStats(new VariableReferenceExpression("leftOpen", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) //(50% heuristic)
                            .lowValueUnknown()
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("rightOpen"), new DoubleLiteral("0.0")))
                .outputRowsCount(225.0) // all rows minus nulls times range coverage (25% - heuristic)
                .variableStats(new VariableReferenceExpression("rightOpen", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(12.5) //(25% heuristic)
                            .lowValue(-15.0)
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("unknownRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .variableStats(new VariableReferenceExpression("unknownRange", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) // (50% heuristic)
                            .lowValueUnknown()
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("emptyRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(0.0)
                .variableStats(new VariableReferenceExpression("emptyRange", DOUBLE), equalTo(emptyRangeStats));
    }

    @Test
    public void symbolToLiteralGreaterThanStats()
    {
        // Simple case
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new DoubleLiteral("2.5")))
                .outputRowsCount(250.0) // all rows minus nulls times range coverage (50%)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(10.0)
                            .lowValue(2.5)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range included)
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new DoubleLiteral("-10.0")))
                .outputRowsCount(750.0) // all rows minus nulls times range coverage (100%)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(40.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range excluded)
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new DoubleLiteral("10.0")))
                .outputRowsCount(18.75) // all rows minus nulls divided by NDV (one value from edge is included as approximation)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal range out of symbol range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new DoubleLiteral("10.0")))
                .outputRowsCount(0.0) // all rows minus nulls times range coverage (0%)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(0.0)
                            .distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Literal in left open range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("leftOpen"), new DoubleLiteral("0.0")))
                .outputRowsCount(225.0) // all rows minus nulls times range coverage (25% - heuristic)
                .variableStats(new VariableReferenceExpression("leftOpen", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(12.5) //(25% heuristic)
                            .lowValue(0.0)
                            .highValue(15.0)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("rightOpen"), new DoubleLiteral("0.0")))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .variableStats(new VariableReferenceExpression("rightOpen", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) //(50% heuristic)
                            .lowValue(0.0)
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("unknownRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .variableStats(new VariableReferenceExpression("unknownRange", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) // (50% heuristic)
                            .lowValue(0.0)
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("emptyRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(0.0)
                .variableStats(new VariableReferenceExpression("emptyRange", DOUBLE), equalTo(emptyRangeStats));
    }

    @Test
    public void symbolToSymbolEqualStats()
    {
        // z's stats should be unchanged when not involved, except NDV capping to row count
        // Equal ranges
        double rowCount = 2.7;
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("u"), new SymbolReference("w")))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("u", DOUBLE), equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .variableStats(new VariableReferenceExpression("w", DOUBLE), equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));

        // One symbol's range is within the other's
        rowCount = 9.375;
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("x"), new SymbolReference("y")))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4)
                            .lowValue(0)
                            .highValue(5)
                            .distinctValuesCount(9.375 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .variableStats(new VariableReferenceExpression("y", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(4)
                            .lowValue(0)
                            .highValue(5)
                            .distinctValuesCount(9.375 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));

        // Partially overlapping ranges
        rowCount = 16.875;
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("x"), new SymbolReference("w")))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(16.875 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .variableStats(new VariableReferenceExpression("w", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(16.875 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));

        // None of the ranges is included in the other, and one symbol has much higher cardinality, so that it has bigger NDV in intersect than the other in total
        rowCount = 2.25;
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("x"), new SymbolReference("u")))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(2.25 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .variableStats(new VariableReferenceExpression("u", DOUBLE), symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(2.25 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));
    }

    @Test
    public void symbolToSymbolNotEqual()
    {
        // Equal ranges
        double rowCount = 807.3;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("u"), new SymbolReference("w")))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("u", DOUBLE), equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .variableStats(new VariableReferenceExpression("w", DOUBLE), equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));

        // One symbol's range is within the other's
        rowCount = 365.625;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new SymbolReference("y")))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), equalTo(capNDV(zeroNullsFraction(xStats), rowCount)))
                .variableStats(new VariableReferenceExpression("y", DOUBLE), equalTo(capNDV(zeroNullsFraction(yStats), rowCount)))
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));

        // Partially overlapping ranges
        rowCount = 658.125;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new SymbolReference("w")))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), equalTo(capNDV(zeroNullsFraction(xStats), rowCount)))
                .variableStats(new VariableReferenceExpression("w", DOUBLE), equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));

        // None of the ranges is included in the other, and one symbol has much higher cardinality, so that it has bigger NDV in intersect than the other in total
        rowCount = 672.75;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new SymbolReference("u")))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), equalTo(capNDV(zeroNullsFraction(xStats), rowCount)))
                .variableStats(new VariableReferenceExpression("u", DOUBLE), equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));
    }

    @Test
    public void symbolToCastExpressionNotEqual()
    {
        double rowCount = 807.3;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("u"), new Cast(new SymbolReference("w"), StandardTypes.BIGINT)))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("u", DOUBLE), equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .variableStats(new VariableReferenceExpression("w", DOUBLE), equalTo(capNDV(wStats, rowCount)))
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));

        rowCount = 897.0;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("u"), new Cast(new LongLiteral("10"), StandardTypes.BIGINT)))
                .outputRowsCount(rowCount)
                .variableStats(new VariableReferenceExpression("u", DOUBLE), equalTo(capNDV(updateNDV(zeroNullsFraction(uStats), -1), rowCount)))
                .variableStats(new VariableReferenceExpression("z", DOUBLE), equalTo(capNDV(zStats, rowCount)));
    }

    private static void checkConsistent(StatsNormalizer normalizer, String source, PlanNodeStatsEstimate stats, Collection<VariableReferenceExpression> outputVariables, TypeProvider types)
    {
        PlanNodeStatsEstimate normalized = normalizer.normalize(stats, outputVariables);
        if (Objects.equals(stats, normalized)) {
            return;
        }

        List<String> problems = new ArrayList<>();

        if (Double.compare(stats.getOutputRowCount(), normalized.getOutputRowCount()) != 0) {
            problems.add(format(
                    "Output row count is %s, should be normalized to %s",
                    stats.getOutputRowCount(),
                    normalized.getOutputRowCount()));
        }

        for (VariableReferenceExpression variable : stats.getVariablesWithKnownStatistics()) {
            if (!Objects.equals(stats.getVariableStatistics(variable), normalized.getVariableStatistics(variable))) {
                problems.add(format(
                        "Variable stats for '%s' are \n\t\t\t\t\t%s, should be normalized to \n\t\t\t\t\t%s",
                        variable,
                        stats.getVariableStatistics(variable),
                        normalized.getVariableStatistics(variable)));
            }
        }

        if (problems.isEmpty()) {
            problems.add(stats.toString());
        }
        throw new IllegalStateException(format(
                "Rule %s returned inconsistent stats: %s",
                source,
                problems.stream().collect(joining("\n\t\t\t", "\n\t\t\t", ""))));
    }
}
