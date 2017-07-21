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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Type.ADD;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

@Test(singleThreaded = true)
public class TestFilterStatsCalculator
{
    private FilterStatsCalculator statsCalculator;
    private PlanNodeStatsEstimate standardInputStatistics;
    private Map<Symbol, Type> standardTypes;
    private Session session;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        SymbolStatsEstimate xStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(40.0)
                .setLowValue(-10.0)
                .setHighValue(10.0)
                .setNullsFraction(0.25)
                .build();
        SymbolStatsEstimate yStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(20.0)
                .setLowValue(0.0)
                .setHighValue(5.0)
                .setNullsFraction(0.5)
                .build();
        SymbolStatsEstimate zStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(5.0)
                .setLowValue(-100.0)
                .setHighValue(100.0)
                .setNullsFraction(0.1)
                .build();
        SymbolStatsEstimate leftOpenStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(15.0)
                .setNullsFraction(0.1)
                .build();
        SymbolStatsEstimate rightOpenStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(-15.0)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        SymbolStatsEstimate unknownRangeStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(50.0)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .build();
        SymbolStatsEstimate emptyRangeStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(0.0)
                .setLowValue(NaN)
                .setHighValue(NaN)
                .setNullsFraction(NaN)
                .build();
        standardInputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol("x"), xStats)
                .addSymbolStatistics(new Symbol("y"), yStats)
                .addSymbolStatistics(new Symbol("z"), zStats)
                .addSymbolStatistics(new Symbol("leftOpen"), leftOpenStats)
                .addSymbolStatistics(new Symbol("rightOpen"), rightOpenStats)
                .addSymbolStatistics(new Symbol("unknownRange"), unknownRangeStats)
                .addSymbolStatistics(new Symbol("emptyRange"), emptyRangeStats)
                .setOutputRowCount(1000.0)
                .build();

        standardTypes = ImmutableMap.<Symbol, Type>builder()
                .put(new Symbol("x"), DoubleType.DOUBLE)
                .put(new Symbol("y"), DoubleType.DOUBLE)
                .put(new Symbol("z"), DoubleType.DOUBLE)
                .put(new Symbol("leftOpen"), DoubleType.DOUBLE)
                .put(new Symbol("rightOpen"), DoubleType.DOUBLE)
                .put(new Symbol("unknownRange"), DoubleType.DOUBLE)
                .put(new Symbol("emptyRange"), DoubleType.DOUBLE).build();

        session = testSessionBuilder().build();
        statsCalculator = new FilterStatsCalculator(MetadataManager.createTestMetadataManager());
    }

    public PlanNodeStatsAssertion assertExpression(Expression expression)
    {
        return PlanNodeStatsAssertion.assertThat(statsCalculator.filterStats(standardInputStatistics,
                expression,
                session,
                standardTypes));
    }

    @Test
    public void testBooleanLiteralStas()
    {
        assertExpression(TRUE_LITERAL).equalTo(standardInputStatistics);

        assertExpression(FALSE_LITERAL).outputRowsCount(0.0)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                })
                .symbolStats("y", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                })
                .symbolStats("z", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                })
                .symbolStats("leftOpen", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                })
                .symbolStats("rightOpen", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                })
                .symbolStats("emptyRange", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                })
                .symbolStats("unknownRange", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });
    }

    @Test
    public void testOrStats()
    {
        Expression leftExpression = new ComparisonExpression(ComparisonExpressionType.LESS_THAN, new SymbolReference("x"), new DoubleLiteral("0.0"));
        Expression rightExpression = new ComparisonExpression(ComparisonExpressionType.LESS_THAN, new SymbolReference("x"), new DoubleLiteral("-7.5"));

        assertExpression(or(leftExpression, rightExpression))
                .outputRowsCount(375)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(0.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.0));

        Expression leftExpressionSingleValue = new ComparisonExpression(ComparisonExpressionType.EQUAL, new SymbolReference("x"), new DoubleLiteral("0.0"));
        Expression rightExpressionSingleValue = new ComparisonExpression(ComparisonExpressionType.EQUAL, new SymbolReference("x"), new DoubleLiteral("-7.5"));

        assertExpression(or(leftExpressionSingleValue, rightExpressionSingleValue))
                .outputRowsCount(37.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(2.0)
                                .nullsFraction(0.0));

        assertExpression(or(
                new ComparisonExpression(ComparisonExpressionType.EQUAL, new SymbolReference("x"), new DoubleLiteral("1")),
                new ComparisonExpression(ComparisonExpressionType.EQUAL, new SymbolReference("x"), new DoubleLiteral("3"))))
                .outputRowsCount(37.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(1)
                                .highValue(3)
                                .distinctValuesCount(2)
                                .nullsFraction(0));
    }

    @Test
    public void testAndStats()
    {
        Expression leftExpression = new ComparisonExpression(ComparisonExpressionType.LESS_THAN, new SymbolReference("x"), new DoubleLiteral("0.0"));
        Expression rightExpression = new ComparisonExpression(ComparisonExpressionType.GREATER_THAN, new SymbolReference("x"), new DoubleLiteral("-7.5"));

        assertExpression(and(leftExpression, rightExpression))
                .outputRowsCount(281.25)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(15.0)
                                .nullsFraction(0.0));

       // Impossible, with symbol-to-expression comparisons
        assertExpression(and(
                new ComparisonExpression(ComparisonExpressionType.EQUAL, new SymbolReference("x"), new ArithmeticBinaryExpression(ADD, new DoubleLiteral("0"), new DoubleLiteral("1"))),
                new ComparisonExpression(ComparisonExpressionType.EQUAL, new SymbolReference("x"), new ArithmeticBinaryExpression(ADD, new DoubleLiteral("0"), new DoubleLiteral("3")))))
                .outputRowsCount(0)
                .symbolStats(new Symbol("x"), SymbolStatsAssertion::emptyRange);
        // TODO .symbolStats(new Symbol("y"), SymbolStatsAssertion::emptyRange);
    }

    @Test
    public void testNotStats()
    {
        Expression innerExpression = new ComparisonExpression(ComparisonExpressionType.LESS_THAN, new SymbolReference("x"), new DoubleLiteral("0.0"));

        assertExpression(new NotExpression(innerExpression))
                .outputRowsCount(625) // FIXME - nulls shouldn't be restored
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(0.0)
                                .highValue(10.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.4)); // FIXME - nulls shouldn't be restored
    }

    @Test
    public void testIsNullFilter()
    {
        Expression isNullPredicate = new IsNullPredicate(new SymbolReference("x"));
        assertExpression(isNullPredicate)
                .outputRowsCount(250.0)
                .symbolStats(new Symbol("x"), symbolStats -> {
                    symbolStats.distinctValuesCount(0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        Expression isNullEmptyRangePredicate = new IsNullPredicate(new SymbolReference("emptyRange"));
        assertExpression(isNullEmptyRangePredicate)
                .outputRowsCount(1000.0)
                .symbolStats(new Symbol("emptyRange"), symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });
    }

    @Test
    public void testIsNotNullFilter()
    {
        Expression isNotNullPredicate = new IsNotNullPredicate(new SymbolReference("x"));
        assertExpression(isNotNullPredicate)
                .outputRowsCount(750.0)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(40.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        Expression isNotNullEmptyRangePredicate = new IsNotNullPredicate(new SymbolReference("emptyRange"));
        assertExpression(isNotNullEmptyRangePredicate)
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });
    }

    @Test
    public void testBetweenOperatorFilter()
    {
        // Only right side cut
        Expression betweenPredicateRightCut = new BetweenPredicate(new SymbolReference("x"), new DoubleLiteral("7.5"), new DoubleLiteral("12.0"));
        assertExpression(betweenPredicateRightCut)
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(7.5)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Only left side cut
        Expression betweenPredicateLeftCut = new BetweenPredicate(new SymbolReference("x"), new DoubleLiteral("-12.0"), new DoubleLiteral("-7.5"));
        assertExpression(betweenPredicateLeftCut)
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(-10)
                            .highValue(-7.5)
                            .nullsFraction(0.0);
                });

        // Both sides cut
        Expression betweenPredicateBothSidesCut = new BetweenPredicate(new SymbolReference("x"), new DoubleLiteral("-2.5"), new DoubleLiteral("2.5"));
        assertExpression(betweenPredicateBothSidesCut)
                .outputRowsCount(187.5)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(10.0)
                            .lowValue(-2.5)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Both sides cut unknownRange
        Expression betweenPredicateBothSidesCutUnknownRange = new BetweenPredicate(new SymbolReference("unknownRange"), new DoubleLiteral("2.72"), new DoubleLiteral("3.14"));
        assertExpression(betweenPredicateBothSidesCutUnknownRange)
                .outputRowsCount(112.5)
                .symbolStats("unknownRange", symbolStats -> {
                    symbolStats.distinctValuesCount(6.25)
                            .lowValue(2.72)
                            .highValue(3.14)
                            .nullsFraction(0.0);
                });

        // Left side open, cut on open side
        Expression betweenPredicateCutOnLeftOpenSide = new BetweenPredicate(new SymbolReference("leftOpen"), new DoubleLiteral("-10.0"), new DoubleLiteral("10.0"));
        assertExpression(betweenPredicateCutOnLeftOpenSide)
                .outputRowsCount(180.0)
                .symbolStats("leftOpen", symbolStats -> {
                    symbolStats.distinctValuesCount(10.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Right side open, cut on open side
        Expression betweenPredicateCutOnRightOpenSide = new BetweenPredicate(new SymbolReference("rightOpen"), new DoubleLiteral("-10.0"), new DoubleLiteral("10.0"));
        assertExpression(betweenPredicateCutOnRightOpenSide)
                .outputRowsCount(180.0)
                .symbolStats("rightOpen", symbolStats -> {
                    symbolStats.distinctValuesCount(10.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Filter all
        Expression betweenPredicateFilterAll = new BetweenPredicate(new SymbolReference("y"), new DoubleLiteral("27.5"), new DoubleLiteral("107.0"));
        assertExpression(betweenPredicateFilterAll)
                .outputRowsCount(0.0)
                .symbolStats("y", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // Filter nothing
        Expression betweenPredicateFilterNothing = new BetweenPredicate(new SymbolReference("y"), new DoubleLiteral("-100.0"), new DoubleLiteral("100.0"));
        assertExpression(betweenPredicateFilterNothing)
                .outputRowsCount(500.0)
                .symbolStats("y", symbolStats -> {
                    symbolStats.distinctValuesCount(20.0)
                            .lowValue(0.0)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Filter non exact match
        Expression betweenPredicateFilterNothingExact = new BetweenPredicate(new SymbolReference("z"), new DoubleLiteral("-100.0"), new DoubleLiteral("100.0"));
        assertExpression(betweenPredicateFilterNothingExact)
                .outputRowsCount(900.0)
                .symbolStats("z", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(-100.0)
                            .highValue(100.0)
                            .nullsFraction(0.0);
                });
    }

    @Test
    public void testInPredicateFilter()
    {
        // One value in range
        Expression singleValueInIn = new InPredicate(new SymbolReference("x"), new InListExpression(ImmutableList.of(new DoubleLiteral("7.5"))));
        assertExpression(singleValueInIn)
                .outputRowsCount(18.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(1.0)
                            .lowValue(7.5)
                            .highValue(7.5)
                            .nullsFraction(0.0);
                });

        // Multiple values in range
        Expression multipleValuesInIn = new InPredicate(new SymbolReference("x"), new InListExpression(
                ImmutableList.of(new DoubleLiteral("1.5"),
                        new DoubleLiteral("2.5"),
                        new DoubleLiteral("7.5"))));
        assertExpression(multipleValuesInIn)
                .outputRowsCount(56.25)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(3.0)
                            .lowValue(1.5)
                            .highValue(7.5)
                            .nullsFraction(0.0);
                })
                .symbolStats("y", symbolStats -> {
                    // Symbol not involved in the comparison should have stats basically unchanged
                    symbolStats.distinctValuesCount(20.0)
                            .lowValue(0.0)
                            .highValue(5)
                            .nullsFraction(0.5);
                });

        // Multiple values some in some out of range
        Expression multipleValuesInInSomeOutOfRange = new InPredicate(new SymbolReference("x"), new InListExpression(
                ImmutableList.of(new DoubleLiteral("-42.0"),
                        new DoubleLiteral("1.5"),
                        new DoubleLiteral("2.5"),
                        new DoubleLiteral("7.5"),
                        new DoubleLiteral("314.0"))));
        assertExpression(multipleValuesInInSomeOutOfRange)
                .outputRowsCount(56.25)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(3.0)
                            .lowValue(1.5)
                            .highValue(7.5)
                            .nullsFraction(0.0);
                });

        // Multiple values in unknown range
        Expression multipleValuesInUnknownRange = new InPredicate(new SymbolReference("unknownRange"), new InListExpression(
                ImmutableList.of(new DoubleLiteral("-42.0"),
                        new DoubleLiteral("1.5"),
                        new DoubleLiteral("2.5"),
                        new DoubleLiteral("7.5"),
                        new DoubleLiteral("314.0"))));
        assertExpression(multipleValuesInUnknownRange)
                .outputRowsCount(90.0)
                .symbolStats("unknownRange", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(-42.0)
                            .highValue(314.0)
                            .nullsFraction(0.0);
                });

        // No value in range
        Expression noValuesInRange = new InPredicate(new SymbolReference("y"), new InListExpression(
                ImmutableList.of(new DoubleLiteral("-42.0"),
                        new DoubleLiteral("6.0"),
                        new DoubleLiteral("31.1341"),
                        new DoubleLiteral("-0.000000002"),
                        new DoubleLiteral("314.0"))));
        assertExpression(noValuesInRange)
                .outputRowsCount(0.0)
                .symbolStats("y", symbolStats -> {
                    symbolStats.distinctValuesCount(0.0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        // More values in range than distinct values
        Expression ndvOverflowInIn = new InPredicate(new SymbolReference("z"), new InListExpression(
                ImmutableList.of(new DoubleLiteral("-1.0"),
                        new DoubleLiteral("3.14"),
                        new DoubleLiteral("0.0"),
                        new DoubleLiteral("1.0"),
                        new DoubleLiteral("2.0"),
                        new DoubleLiteral("3.0"),
                        new DoubleLiteral("4.0"),
                        new DoubleLiteral("5.0"),
                        new DoubleLiteral("6.0"),
                        new DoubleLiteral("7.0"),
                        new DoubleLiteral("8.0"),
                        new DoubleLiteral("-2.0"))));
        assertExpression(ndvOverflowInIn)
                .outputRowsCount(900.0)
                .symbolStats("z", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(-2.0)
                            .highValue(8.0)
                            .nullsFraction(0.0);
                });

        // Values in weird order
        Expression ndvOverflowInNotSortedValues = new InPredicate(new SymbolReference("z"), new InListExpression(
                ImmutableList.of(new DoubleLiteral("-1.0"),
                        new DoubleLiteral("1.0"),
                        new DoubleLiteral("0.0"))));
        assertExpression(ndvOverflowInNotSortedValues)
                .outputRowsCount(540.0)
                .symbolStats("z", symbolStats -> {
                    symbolStats.distinctValuesCount(3.0)
                            .lowValue(-1.0)
                            .highValue(1.0)
                            .nullsFraction(0.0);
                });
    }
}
