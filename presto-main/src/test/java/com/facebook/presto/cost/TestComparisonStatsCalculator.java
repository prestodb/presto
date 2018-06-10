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
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
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
    private SymbolStatsEstimate uStats;
    private SymbolStatsEstimate wStats;
    private SymbolStatsEstimate xStats;
    private SymbolStatsEstimate yStats;
    private SymbolStatsEstimate zStats;
    private SymbolStatsEstimate leftOpenStats;
    private SymbolStatsEstimate rightOpenStats;
    private SymbolStatsEstimate unknownRangeStats;
    private SymbolStatsEstimate emptyRangeStats;
    private SymbolStatsEstimate varcharStats;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        session = testSessionBuilder().build();
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        filterStatsCalculator = new FilterStatsCalculator(metadata, new ScalarStatsCalculator(metadata), new StatsNormalizer());

        uStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(300)
                .setLowValue(0)
                .setHighValue(20)
                .setNullsFraction(0.1)
                .build();
        wStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(30)
                .setLowValue(0)
                .setHighValue(20)
                .setNullsFraction(0.1)
                .build();
        xStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(40.0)
                .setLowValue(-10.0)
                .setHighValue(10.0)
                .setNullsFraction(0.25)
                .build();
        yStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(20.0)
                .setLowValue(0.0)
                .setHighValue(5.0)
                .setNullsFraction(0.5)
                .build();
        zStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(5.0)
                .setLowValue(-100.0)
                .setHighValue(100.0)
                .setNullsFraction(0.1)
                .build();
        leftOpenStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(15.0)
                .setNullsFraction(0.1)
                .build();
        rightOpenStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(-15.0)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        unknownRangeStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        emptyRangeStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(0.0)
                .setDistinctValuesCount(0.0)
                .setLowValue(NaN)
                .setHighValue(NaN)
                .setNullsFraction(1.0)
                .build();
        varcharStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        standardInputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol("u"), uStats)
                .addSymbolStatistics(new Symbol("w"), wStats)
                .addSymbolStatistics(new Symbol("x"), xStats)
                .addSymbolStatistics(new Symbol("y"), yStats)
                .addSymbolStatistics(new Symbol("z"), zStats)
                .addSymbolStatistics(new Symbol("leftOpen"), leftOpenStats)
                .addSymbolStatistics(new Symbol("rightOpen"), rightOpenStats)
                .addSymbolStatistics(new Symbol("unknownRange"), unknownRangeStats)
                .addSymbolStatistics(new Symbol("emptyRange"), emptyRangeStats)
                .addSymbolStatistics(new Symbol("varchar"), varcharStats)
                .setOutputRowCount(1000.0)
                .build();

        types = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
                .put(new Symbol("u"), DoubleType.DOUBLE)
                .put(new Symbol("w"), DoubleType.DOUBLE)
                .put(new Symbol("x"), DoubleType.DOUBLE)
                .put(new Symbol("y"), DoubleType.DOUBLE)
                .put(new Symbol("z"), DoubleType.DOUBLE)
                .put(new Symbol("leftOpen"), DoubleType.DOUBLE)
                .put(new Symbol("rightOpen"), DoubleType.DOUBLE)
                .put(new Symbol("unknownRange"), DoubleType.DOUBLE)
                .put(new Symbol("emptyRange"), DoubleType.DOUBLE)
                .put(new Symbol("varchar"), VarcharType.createVarcharType(10))
                .build());
    }

    private Consumer<SymbolStatsAssertion> equalTo(SymbolStatsEstimate estimate)
    {
        return symbolAssert -> {
            symbolAssert
                    .lowValue(estimate.getLowValue())
                    .highValue(estimate.getHighValue())
                    .distinctValuesCount(estimate.getDistinctValuesCount())
                    .nullsFraction(estimate.getNullsFraction());
        };
    }

    private SymbolStatsEstimate updateNDV(SymbolStatsEstimate symbolStats, double delta)
    {
        return symbolStats.mapDistinctValuesCount(ndv -> ndv + delta);
    }

    private SymbolStatsEstimate capNDV(SymbolStatsEstimate symbolStats, double rowCount)
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

    private SymbolStatsEstimate zeroNullsFraction(SymbolStatsEstimate symbolStats)
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
                standardInputStatistics.getSymbolsWithKnownStatistics(),
                types);
    }

    @Test
    public void symbolToLiteralEqualStats()
    {
        // Simple case
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("y"), new DoubleLiteral("2.5")))
                .outputRowsCount(25.0) // all rows minus nulls divided by distinct values count
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(2.5)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("x"), new DoubleLiteral("10.0")))
                .outputRowsCount(18.75) // all rows minus nulls divided by distinct values count
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal out of symbol range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("y"), new DoubleLiteral("10.0")))
                .outputRowsCount(0.0) // all rows minus nulls divided by distinct values count
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(0.0)
                            .distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Literal in left open range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("leftOpen"), new DoubleLiteral("2.5")))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .symbolStats("leftOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(2.5)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("rightOpen"), new DoubleLiteral("-2.5")))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .symbolStats("rightOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(-2.5)
                            .highValue(-2.5)
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("unknownRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .symbolStats("unknownRange", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(0.0)
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("emptyRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", equalTo(emptyRangeStats));

        // Column with values not representable as double (unknown range)
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("varchar"), new StringLiteral("blah")))
                .outputRowsCount(18.0) // all rows minus nulls divided by distinct values count
                .symbolStats("varchar", symbolAssert -> {
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
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(19.0)
                            .lowValue(0.0)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new DoubleLiteral("10.0")))
                .outputRowsCount(731.25) // all rows minus nulls multiplied by ((distinct values - 1) / distinct values)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(39.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal out of symbol range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("y"), new DoubleLiteral("10.0")))
                .outputRowsCount(500.0) // all rows minus nulls
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(19.0)
                            .lowValue(0.0)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Literal in left open range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("leftOpen"), new DoubleLiteral("2.5")))
                .outputRowsCount(882.0) // all rows minus nulls multiplied by ((distinct values - 1) / distinct values)
                .symbolStats("leftOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValueUnknown()
                            .highValue(15.0)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("rightOpen"), new DoubleLiteral("-2.5")))
                .outputRowsCount(882.0) // all rows minus nulls divided by distinct values count
                .symbolStats("rightOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValue(-15.0)
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("unknownRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(882.0) // all rows minus nulls divided by distinct values count
                .symbolStats("unknownRange", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(49.0)
                            .lowValueUnknown()
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("emptyRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", equalTo(emptyRangeStats));

        // Column with values not representable as double (unknown range)
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("varchar"), new StringLiteral("blah")))
                .outputRowsCount(882.0) // all rows minus nulls divided by distinct values count
                .symbolStats("varchar", symbolAssert -> {
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
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(10.0)
                            .lowValue(0.0)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range included)
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral("10.0")))
                .outputRowsCount(750.0) // all rows minus nulls times range coverage (100%)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(40.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range excluded)
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral("-10.0")))
                .outputRowsCount(18.75) // all rows minus nulls divided by NDV (one value from edge is included as approximation)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(-10.0)
                            .highValue(-10.0)
                            .nullsFraction(0.0);
                });

        // Literal range out of symbol range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("y"), new DoubleLiteral("-10.0")))
                .outputRowsCount(0.0) // all rows minus nulls times range coverage (0%)
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(0.0)
                            .distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Literal in left open range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("leftOpen"), new DoubleLiteral("0.0")))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .symbolStats("leftOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) //(50% heuristic)
                            .lowValueUnknown()
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("rightOpen"), new DoubleLiteral("0.0")))
                .outputRowsCount(225.0) // all rows minus nulls times range coverage (25% - heuristic)
                .symbolStats("rightOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(12.5) //(25% heuristic)
                            .lowValue(-15.0)
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("unknownRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .symbolStats("unknownRange", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) // (50% heuristic)
                            .lowValueUnknown()
                            .highValue(0.0)
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new ComparisonExpression(LESS_THAN, new SymbolReference("emptyRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", equalTo(emptyRangeStats));
    }

    @Test
    public void symbolToLiteralGreaterThanStats()
    {
        // Simple case
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new DoubleLiteral("2.5")))
                .outputRowsCount(250.0) // all rows minus nulls times range coverage (50%)
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(10.0)
                            .lowValue(2.5)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range included)
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new DoubleLiteral("-10.0")))
                .outputRowsCount(750.0) // all rows minus nulls times range coverage (100%)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(40.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal on the edge of symbol range (whole range excluded)
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new DoubleLiteral("10.0")))
                .outputRowsCount(18.75) // all rows minus nulls divided by NDV (one value from edge is included as approximation)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(1.0)
                            .lowValue(10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Literal range out of symbol range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new DoubleLiteral("10.0")))
                .outputRowsCount(0.0) // all rows minus nulls times range coverage (0%)
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(0.0)
                            .distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Literal in left open range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("leftOpen"), new DoubleLiteral("0.0")))
                .outputRowsCount(225.0) // all rows minus nulls times range coverage (25% - heuristic)
                .symbolStats("leftOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(12.5) //(25% heuristic)
                            .lowValue(0.0)
                            .highValue(15.0)
                            .nullsFraction(0.0);
                });

        // Literal in right open range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("rightOpen"), new DoubleLiteral("0.0")))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .symbolStats("rightOpen", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) //(50% heuristic)
                            .lowValue(0.0)
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in unknown range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("unknownRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(450.0) // all rows minus nulls times range coverage (50% - heuristic)
                .symbolStats("unknownRange", symbolAssert -> {
                    symbolAssert.averageRowSize(4.0)
                            .distinctValuesCount(25.0) // (50% heuristic)
                            .lowValue(0.0)
                            .highValueUnknown()
                            .nullsFraction(0.0);
                });

        // Literal in empty range
        assertCalculate(new ComparisonExpression(GREATER_THAN, new SymbolReference("emptyRange"), new DoubleLiteral("0.0")))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", equalTo(emptyRangeStats));
    }

    @Test
    public void symbolToSymbolEqualStats()
    {
        // z's stats should be unchanged when not involved, except NDV capping to row count
        // Equal ranges
        double rowCount = 2.7;
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("u"), new SymbolReference("w")))
                .outputRowsCount(rowCount)
                .symbolStats("u", equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .symbolStats("w", equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // One symbol's range is within the other's
        rowCount = 4.6875;
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("x"), new SymbolReference("y")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(4)
                            .lowValue(0)
                            .highValue(5)
                            .distinctValuesCount(4.6875 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("y", symbolAssert -> {
                    symbolAssert.averageRowSize(4)
                            .lowValue(0)
                            .highValue(5)
                            .distinctValuesCount(4.6875 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // Partially overlapping ranges
        rowCount = 8.4375;
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("x"), new SymbolReference("w")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(8.4375 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("w", symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(8.4375 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // None of the ranges is included in the other, and one symbol has much higher cardinality, so that it has bigger NDV in intersect than the other in total
        rowCount = 1.125;
        assertCalculate(new ComparisonExpression(EQUAL, new SymbolReference("x"), new SymbolReference("u")))
                .outputRowsCount(rowCount)
                .symbolStats("x", symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(1.125 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("u", symbolAssert -> {
                    symbolAssert.averageRowSize(6)
                            .lowValue(0)
                            .highValue(10)
                            .distinctValuesCount(1.125 /* min(rowCount, ndv in intersection */)
                            .nullsFraction(0);
                })
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
    }

    @Test
    public void symbolToSymbolNotEqual()
    {
        // Equal ranges
        double rowCount = 807.3;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("u"), new SymbolReference("w")))
                .outputRowsCount(rowCount)
                .symbolStats("u", equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .symbolStats("w", equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // One symbol's range is within the other's
        rowCount = 370.3125;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new SymbolReference("y")))
                .outputRowsCount(rowCount)
                .symbolStats("x", equalTo(capNDV(zeroNullsFraction(xStats), rowCount)))
                .symbolStats("y", equalTo(capNDV(zeroNullsFraction(yStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // Partially overlapping ranges
        rowCount = 666.5625;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new SymbolReference("w")))
                .outputRowsCount(rowCount)
                .symbolStats("x", equalTo(capNDV(zeroNullsFraction(xStats), rowCount)))
                .symbolStats("w", equalTo(capNDV(zeroNullsFraction(wStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        // None of the ranges is included in the other, and one symbol has much higher cardinality, so that it has bigger NDV in intersect than the other in total
        rowCount = 673.875;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("x"), new SymbolReference("u")))
                .outputRowsCount(rowCount)
                .symbolStats("x", equalTo(capNDV(zeroNullsFraction(xStats), rowCount)))
                .symbolStats("u", equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
    }

    @Test
    public void symbolToCastExpressionNotEqual()
    {
        double rowCount = 807.3;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("u"), new Cast(new SymbolReference("w"), BIGINT)))
                .outputRowsCount(rowCount)
                .symbolStats("u", equalTo(capNDV(zeroNullsFraction(uStats), rowCount)))
                .symbolStats("w", equalTo(capNDV(wStats, rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));

        rowCount = 897.0;
        assertCalculate(new ComparisonExpression(NOT_EQUAL, new SymbolReference("u"), new Cast(new LongLiteral("10"), BIGINT)))
                .outputRowsCount(rowCount)
                .symbolStats("u", equalTo(capNDV(updateNDV(zeroNullsFraction(uStats), -1), rowCount)))
                .symbolStats("z", equalTo(capNDV(zStats, rowCount)));
    }

    private static void checkConsistent(StatsNormalizer normalizer, String source, PlanNodeStatsEstimate stats, Collection<Symbol> outputSymbols, TypeProvider types)
    {
        PlanNodeStatsEstimate normalized = normalizer.normalize(stats, outputSymbols, types);
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

        for (Symbol symbol : stats.getSymbolsWithKnownStatistics()) {
            if (!Objects.equals(stats.getSymbolStatistics(symbol), normalized.getSymbolStatistics(symbol))) {
                problems.add(format(
                        "Symbol stats for '%s' are \n\t\t\t\t\t%s, should be normalized to \n\t\t\t\t\t%s",
                        symbol,
                        stats.getSymbolStatistics(symbol),
                        normalized.getSymbolStatistics(symbol)));
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
