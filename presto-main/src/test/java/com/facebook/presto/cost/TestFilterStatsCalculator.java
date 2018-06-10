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
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.String.format;

public class TestFilterStatsCalculator
{
    private static final VarcharType MEDIUM_VARCHAR_TYPE = VarcharType.createVarcharType(100);

    private FilterStatsCalculator statsCalculator;
    private PlanNodeStatsEstimate standardInputStatistics;
    private TypeProvider standardTypes;
    private Session session;

    @BeforeClass
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
                .setAverageRowSize(0.0)
                .setDistinctValuesCount(0.0)
                .setLowValue(NaN)
                .setHighValue(NaN)
                .setNullsFraction(NaN)
                .build();
        SymbolStatsEstimate mediumVarcharStats = SymbolStatsEstimate.builder()
                .setAverageRowSize(85.0)
                .setDistinctValuesCount(165)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.34)
                .build();
        standardInputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol("x"), xStats)
                .addSymbolStatistics(new Symbol("y"), yStats)
                .addSymbolStatistics(new Symbol("z"), zStats)
                .addSymbolStatistics(new Symbol("leftOpen"), leftOpenStats)
                .addSymbolStatistics(new Symbol("rightOpen"), rightOpenStats)
                .addSymbolStatistics(new Symbol("unknownRange"), unknownRangeStats)
                .addSymbolStatistics(new Symbol("emptyRange"), emptyRangeStats)
                .addSymbolStatistics(new Symbol("mediumVarchar"), mediumVarcharStats)
                .setOutputRowCount(1000.0)
                .build();

        standardTypes = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
                .put(new Symbol("x"), DoubleType.DOUBLE)
                .put(new Symbol("y"), DoubleType.DOUBLE)
                .put(new Symbol("z"), DoubleType.DOUBLE)
                .put(new Symbol("leftOpen"), DoubleType.DOUBLE)
                .put(new Symbol("rightOpen"), DoubleType.DOUBLE)
                .put(new Symbol("unknownRange"), DoubleType.DOUBLE)
                .put(new Symbol("emptyRange"), DoubleType.DOUBLE)
                .put(new Symbol("mediumVarchar"), MEDIUM_VARCHAR_TYPE)
                .build());

        session = testSessionBuilder().build();
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        statsCalculator = new FilterStatsCalculator(metadata, new ScalarStatsCalculator(metadata), new StatsNormalizer());
    }

    @Test
    public void testBooleanLiteralStats()
    {
        assertExpression("true")
                .equalTo(standardInputStatistics);

        assertExpression("false")
                .outputRowsCount(0.0)
                .symbolStats("x", SymbolStatsAssertion::empty)
                .symbolStats("y", SymbolStatsAssertion::empty)
                .symbolStats("z", SymbolStatsAssertion::empty)
                .symbolStats("leftOpen", SymbolStatsAssertion::empty)
                .symbolStats("rightOpen", SymbolStatsAssertion::empty)
                .symbolStats("emptyRange", SymbolStatsAssertion::empty)
                .symbolStats("unknownRange", SymbolStatsAssertion::empty);
    }

    @Test
    public void testComparison()
    {
        double lessThan3Rows = 487.5;
        assertExpression("x < 3e0")
                .outputRowsCount(lessThan3Rows)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10)
                                .highValue(3)
                                .distinctValuesCount(26)
                                .nullsFraction(0.0));

        assertExpression("-x > -3e0")
                .outputRowsCount(lessThan3Rows);

        for (String minusThree : ImmutableList.of("DECIMAL '-3'", "-3e0", "(4e0-7e0)", "CAST(-3 AS DECIMAL)")) {
            System.out.println(minusThree);

            assertExpression("x = " + minusThree)
                    .outputRowsCount(18.75)
                    .symbolStats(new Symbol("x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-3)
                                    .highValue(-3)
                                    .distinctValuesCount(1)
                                    .nullsFraction(0.0));

            assertExpression("x < " + minusThree)
                    .outputRowsCount(262.5)
                    .symbolStats(new Symbol("x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-10)
                                    .highValue(-3)
                                    .distinctValuesCount(14)
                                    .nullsFraction(0.0));
        }
    }

    @Test
    public void testOrStats()
    {
        assertExpression("x < 0e0 OR x < DOUBLE '-7.5'")
                .outputRowsCount(375)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(0.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.0));

        assertExpression("x = 0e0 OR x = DOUBLE '-7.5'")
                .outputRowsCount(37.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(2.0)
                                .nullsFraction(0.0));

        assertExpression("x = 1e0 OR x = 3e0")
                .outputRowsCount(37.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(1)
                                .highValue(3)
                                .distinctValuesCount(2)
                                .nullsFraction(0));
    }

    @Test
    public void testUnsupportedExpression()
    {
        assertExpression("sin(x)")
                .outputRowsCount(900);
        assertExpression("x = sin(x)")
                .outputRowsCount(900);
    }

    @Test
    public void testAndStats()
    {
        assertExpression("x < 0e0 AND x > DOUBLE '-7.5'")
                .outputRowsCount(281.25)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(15.0)
                                .nullsFraction(0.0));

        // Impossible, with symbol-to-expression comparisons
        assertExpression("x = (0e0 + 1e0) AND x = (0e0 + 3e0)")
                .outputRowsCount(0)
                .symbolStats(new Symbol("x"), SymbolStatsAssertion::emptyRange)
                .symbolStats(new Symbol("y"), SymbolStatsAssertion::emptyRange);

        // first argument unknown
        assertExpression("json_array_contains(JSON '[]', x) AND x < 0e0")
                .outputRowsCount(337.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.lowValue(-10)
                                .highValue(0)
                                .distinctValuesCount(20)
                                .nullsFraction(0));

        // second argument unknown
        assertExpression("x < 0e0 AND json_array_contains(JSON '[]', x)")
                .outputRowsCount(337.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.lowValue(-10)
                                .highValue(0)
                                .distinctValuesCount(20)
                                .nullsFraction(0));

        // both arguments unknown
        assertExpression("json_array_contains(JSON '[11]', x) AND json_array_contains(JSON '[13]', x)")
                .outputRowsCount(900)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.lowValue(-10)
                                .highValue(10)
                                .distinctValuesCount(40)
                                .nullsFraction(0.25));
    }

    @Test
    public void testNotStats()
    {
        assertExpression("NOT(x < 0e0)")
                .outputRowsCount(625) // FIXME - nulls shouldn't be restored
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(0.0)
                                .highValue(10.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.4)); // FIXME - nulls shouldn't be restored

        assertExpression("NOT(json_array_contains(JSON '[]', x))")
                .outputRowsCount(900)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .distinctValuesCount(40.0)
                                .nullsFraction(0.25));

        assertExpression("NOT(x IS NULL)")
                .outputRowsCount(750)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .distinctValuesCount(40.0)
                                .nullsFraction(0));
    }

    @Test
    public void testIsNullFilter()
    {
        assertExpression("x IS NULL")
                .outputRowsCount(250.0)
                .symbolStats(new Symbol("x"), symbolStats -> {
                    symbolStats.distinctValuesCount(0)
                            .emptyRange()
                            .nullsFraction(1.0);
                });

        assertExpression("emptyRange IS NULL")
                .outputRowsCount(1000.0)
                .symbolStats(new Symbol("emptyRange"), SymbolStatsAssertion::empty);
    }

    @Test
    public void testIsNotNullFilter()
    {
        assertExpression("x IS NOT NULL")
                .outputRowsCount(750.0)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(40.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        assertExpression("emptyRange IS NOT NULL")
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", SymbolStatsAssertion::empty);
    }

    @Test
    public void testBetweenOperatorFilter()
    {
        // Only right side cut
        assertExpression("x BETWEEN 7.5e0 AND 12e0")
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(7.5)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Only left side cut
        assertExpression("x BETWEEN DOUBLE '-12' AND DOUBLE '-7.5'")
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(-10)
                            .highValue(-7.5)
                            .nullsFraction(0.0);
                });
        assertExpression("x BETWEEN -12e0 AND -7.5e0")
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(-10)
                            .highValue(-7.5)
                            .nullsFraction(0.0);
                });

        // Both sides cut
        assertExpression("x BETWEEN DOUBLE '-2.5' AND 2.5e0")
                .outputRowsCount(187.5)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(10.0)
                            .lowValue(-2.5)
                            .highValue(2.5)
                            .nullsFraction(0.0);
                });

        // Both sides cut unknownRange
        assertExpression("unknownRange BETWEEN 2.72e0 AND 3.14e0")
                .outputRowsCount(112.5)
                .symbolStats("unknownRange", symbolStats -> {
                    symbolStats.distinctValuesCount(6.25)
                            .lowValue(2.72)
                            .highValue(3.14)
                            .nullsFraction(0.0);
                });

        // Left side open, cut on open side
        assertExpression("leftOpen BETWEEN DOUBLE '-10' AND 10e0")
                .outputRowsCount(180.0)
                .symbolStats("leftOpen", symbolStats -> {
                    symbolStats.distinctValuesCount(10.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Right side open, cut on open side
        assertExpression("rightOpen BETWEEN DOUBLE '-10' AND 10e0")
                .outputRowsCount(180.0)
                .symbolStats("rightOpen", symbolStats -> {
                    symbolStats.distinctValuesCount(10.0)
                            .lowValue(-10.0)
                            .highValue(10.0)
                            .nullsFraction(0.0);
                });

        // Filter all
        assertExpression("y BETWEEN 27.5e0 AND 107e0")
                .outputRowsCount(0.0)
                .symbolStats("y", SymbolStatsAssertion::empty);

        // Filter nothing
        assertExpression("y BETWEEN DOUBLE '-100' AND 100e0")
                .outputRowsCount(500.0)
                .symbolStats("y", symbolStats -> {
                    symbolStats.distinctValuesCount(20.0)
                            .lowValue(0.0)
                            .highValue(5.0)
                            .nullsFraction(0.0);
                });

        // Filter non exact match
        assertExpression("z BETWEEN DOUBLE '-100' AND 100e0")
                .outputRowsCount(900.0)
                .symbolStats("z", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(-100.0)
                            .highValue(100.0)
                            .nullsFraction(0.0);
                });
    }

    @Test
    public void testSymbolEqualsSameSymbolFilter()
    {
        assertExpression("x = x")
                .outputRowsCount(750)
                .symbolStats("x", symbolStats -> {
                    SymbolStatsEstimate.builder()
                            .setAverageRowSize(4.0)
                            .setDistinctValuesCount(40.0)
                            .setLowValue(-10.0)
                            .setHighValue(10.0)
                            .build();
                });
    }

    @Test
    public void testInPredicateFilter()
    {
        // One value in range
        assertExpression("x IN (7.5e0)")
                .outputRowsCount(18.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(1.0)
                            .lowValue(7.5)
                            .highValue(7.5)
                            .nullsFraction(0.0);
                });
        assertExpression("x IN (DOUBLE '-7.5')")
                .outputRowsCount(18.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(1.0)
                            .lowValue(-7.5)
                            .highValue(-7.5)
                            .nullsFraction(0.0);
                });
        assertExpression("x IN (BIGINT '2' + 5.5e0)")
                .outputRowsCount(18.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(1.0)
                            .lowValue(7.5)
                            .highValue(7.5)
                            .nullsFraction(0.0);
                });
        assertExpression("x IN (-7.5e0)")
                .outputRowsCount(18.75)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(1.0)
                            .lowValue(-7.5)
                            .highValue(-7.5)
                            .nullsFraction(0.0);
                });

        // Multiple values in range
        assertExpression("x IN (1.5e0, 2.5e0, 7.5e0)")
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
        assertExpression("x IN (DOUBLE '-42', 1.5e0, 2.5e0, 7.5e0, 314e0)")
                .outputRowsCount(56.25)
                .symbolStats("x", symbolStats -> {
                    symbolStats.distinctValuesCount(3.0)
                            .lowValue(1.5)
                            .highValue(7.5)
                            .nullsFraction(0.0);
                });

        // Multiple values in unknown range
        assertExpression("unknownRange IN (DOUBLE '-42', 1.5e0, 2.5e0, 7.5e0, 314e0)")
                .outputRowsCount(90.0)
                .symbolStats("unknownRange", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(-42.0)
                            .highValue(314.0)
                            .nullsFraction(0.0);
                });

        // Casted literals as value
        assertExpression(format("mediumVarchar IN (CAST('abc' AS %s))", MEDIUM_VARCHAR_TYPE.toString()))
                .outputRowsCount(4)
                .symbolStats("mediumVarchar", symbolStats -> {
                    symbolStats.distinctValuesCount(1)
                            .nullsFraction(0.0);
                });

        assertExpression(format("mediumVarchar IN (CAST('abc' AS %1$s), CAST('def' AS %1$s))", MEDIUM_VARCHAR_TYPE.toString()))
                .outputRowsCount(8)
                .symbolStats("mediumVarchar", symbolStats -> {
                    symbolStats.distinctValuesCount(2)
                            .nullsFraction(0.0);
                });

        // No value in range
        assertExpression("y IN (DOUBLE '-42', 6e0, 31.1341e0, DOUBLE '-0.000000002', 314e0)")
                .outputRowsCount(0.0)
                .symbolStats("y", SymbolStatsAssertion::empty);

        // More values in range than distinct values
        assertExpression("z IN (DOUBLE '-1', 3.14e0, 0e0, 1e0, 2e0, 3e0, 4e0, 5e0, 6e0, 7e0, 8e0, DOUBLE '-2')")
                .outputRowsCount(900.0)
                .symbolStats("z", symbolStats -> {
                    symbolStats.distinctValuesCount(5.0)
                            .lowValue(-2.0)
                            .highValue(8.0)
                            .nullsFraction(0.0);
                });

        // Values in weird order
        assertExpression("z IN (DOUBLE '-1', 1e0, 0e0)")
                .outputRowsCount(540.0)
                .symbolStats("z", symbolStats -> {
                    symbolStats.distinctValuesCount(3.0)
                            .lowValue(-1.0)
                            .highValue(1.0)
                            .nullsFraction(0.0);
                });
    }

    private PlanNodeStatsAssertion assertExpression(String expression)
    {
        return assertExpression(expression(expression));
    }

    private PlanNodeStatsAssertion assertExpression(Expression expression)
    {
        return PlanNodeStatsAssertion.assertThat(statsCalculator.filterStats(
                standardInputStatistics,
                expression,
                session,
                standardTypes));
    }
}
