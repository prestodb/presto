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
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestFilterStatsCalculator
{
    private static final VarcharType MEDIUM_VARCHAR_TYPE = VarcharType.createVarcharType(100);

    private VariableStatsEstimate xStats;
    private VariableStatsEstimate yStats;
    private VariableStatsEstimate zStats;
    private VariableStatsEstimate leftOpenStats;
    private VariableStatsEstimate rightOpenStats;
    private VariableStatsEstimate unknownRangeStats;
    private VariableStatsEstimate emptyRangeStats;
    private VariableStatsEstimate mediumVarcharStats;
    private FilterStatsCalculator statsCalculator;
    private PlanNodeStatsEstimate standardInputStatistics;
    private TypeProvider standardTypes;
    private Session session;
    private TestingRowExpressionTranslator translator;

    @BeforeClass
    public void setUp()
            throws Exception
    {
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
                .setNullsFraction(NaN)
                .build();
        mediumVarcharStats = VariableStatsEstimate.builder()
                .setAverageRowSize(85.0)
                .setDistinctValuesCount(165)
                .setLowValue(NEGATIVE_INFINITY)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.34)
                .build();
        standardInputStatistics = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(new VariableReferenceExpression("x", DOUBLE), xStats)
                .addVariableStatistics(new VariableReferenceExpression("y", DOUBLE), yStats)
                .addVariableStatistics(new VariableReferenceExpression("z", DOUBLE), zStats)
                .addVariableStatistics(new VariableReferenceExpression("leftOpen", DOUBLE), leftOpenStats)
                .addVariableStatistics(new VariableReferenceExpression("rightOpen", DOUBLE), rightOpenStats)
                .addVariableStatistics(new VariableReferenceExpression("unknownRange", DOUBLE), unknownRangeStats)
                .addVariableStatistics(new VariableReferenceExpression("emptyRange", DOUBLE), emptyRangeStats)
                .addVariableStatistics(new VariableReferenceExpression("mediumVarchar", MEDIUM_VARCHAR_TYPE), mediumVarcharStats)
                .setOutputRowCount(1000.0)
                .build();

        standardTypes = TypeProvider.fromVariables(ImmutableList.<VariableReferenceExpression>builder()
                .add(new VariableReferenceExpression("x", DOUBLE))
                .add(new VariableReferenceExpression("y", DOUBLE))
                .add(new VariableReferenceExpression("z", DOUBLE))
                .add(new VariableReferenceExpression("leftOpen", DOUBLE))
                .add(new VariableReferenceExpression("rightOpen", DOUBLE))
                .add(new VariableReferenceExpression("unknownRange", DOUBLE))
                .add(new VariableReferenceExpression("emptyRange", DOUBLE))
                .add(new VariableReferenceExpression("mediumVarchar", MEDIUM_VARCHAR_TYPE))
                .build());

        session = testSessionBuilder().build();
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        statsCalculator = new FilterStatsCalculator(metadata, new ScalarStatsCalculator(metadata), new StatsNormalizer());
        translator = new TestingRowExpressionTranslator(MetadataManager.createTestMetadataManager());
    }

    @Test
    public void testBooleanLiteralStats()
    {
        assertExpression("true")
                .equalTo(standardInputStatistics);

        assertExpression("false")
                .outputRowsCount(0.0)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("z", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("leftOpen", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("rightOpen", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("emptyRange", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("unknownRange", DOUBLE), VariableStatsAssertion::empty);

        // `null AND null` is interpreted as null
        assertExpression("cast(null as boolean) AND cast(null as boolean)")
                .outputRowsCount(0.0)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("z", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("leftOpen", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("rightOpen", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("emptyRange", DOUBLE), VariableStatsAssertion::empty)
                .variableStats(new VariableReferenceExpression("unknownRange", DOUBLE), VariableStatsAssertion::empty);

        // more complicated expressions with null
        assertExpression("cast(null as boolean) OR sin(x) > x").outputRowsCount(NaN);

        // RowExpressionInterpreter does more aggressive constant folding than ExpressionInterpreter.
        // For ExpressionInterpreter, cast will return unknown; but RowExpressionInterpreter can actually evaluate cast.
        Expression expression = expression("cast(null as boolean) AND sin(x) > x");
        RowExpression rowExpression = translator.translateAndOptimize(expression, standardTypes);
        PlanNodeStatsEstimate rowExpressionStatsEstimate = statsCalculator.filterStats(standardInputStatistics, rowExpression, session);
        PlanNodeStatsAssertion.assertThat(rowExpressionStatsEstimate).outputRowsCount(0.0);
    }

    @Test
    public void testComparison()
    {
        double lessThan3Rows = 487.5;
        assertExpression("x < 3e0")
                .outputRowsCount(lessThan3Rows)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.averageRowSize(4.0)
                                .lowValue(-10)
                                .highValue(3)
                                .distinctValuesCount(26)
                                .nullsFraction(0.0));

        assertExpression("-x > -3e0")
                .outputRowsCount(lessThan3Rows);

        for (String minusThree : ImmutableList.of("DECIMAL '-3'", "-3e0", "(4e0-7e0)", "CAST(-3 AS DECIMAL(7,3))"/*, "CAST('1' AS BIGINT) - 4"*/)) {
            for (String xEquals : ImmutableList.of("x = %s", "%s = x", "COALESCE(x * CAST(NULL AS BIGINT), x) = %s", "%s = CAST(x AS DOUBLE)")) {
                assertExpression(format(xEquals, minusThree))
                        .outputRowsCount(18.75)
                        .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                                variableAssert.averageRowSize(4.0)
                                        .lowValue(-3)
                                        .highValue(-3)
                                        .distinctValuesCount(1)
                                        .nullsFraction(0.0));
            }

            for (String xLessThan : ImmutableList.of("x < %s", "%s > x", "%s > CAST(x AS DOUBLE)")) {
                assertExpression(format(xLessThan, minusThree))
                        .outputRowsCount(262.5)
                        .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                                variableAssert.averageRowSize(4.0)
                                        .lowValue(-10)
                                        .highValue(-3)
                                        .distinctValuesCount(14)
                                        .nullsFraction(0.0));
            }
        }
    }

    @Test
    public void testOrStats()
    {
        assertExpression("x < 0e0 OR x < DOUBLE '-7.5'")
                .outputRowsCount(375)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(0.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.0));

        assertExpression("x = 0e0 OR x = DOUBLE '-7.5'")
                .outputRowsCount(37.5)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(2.0)
                                .nullsFraction(0.0));

        assertExpression("x = 1e0 OR x = 3e0")
                .outputRowsCount(37.5)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.averageRowSize(4.0)
                                .lowValue(1)
                                .highValue(3)
                                .distinctValuesCount(2)
                                .nullsFraction(0));

        assertExpression("x = 1e0 OR 'a' = 'b' OR x = 3e0")
                .outputRowsCount(37.5)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.averageRowSize(4.0)
                                .lowValue(1)
                                .highValue(3)
                                .distinctValuesCount(2)
                                .nullsFraction(0));

        assertExpression("x = 1e0 OR (CAST('b' AS VARCHAR(3)) IN (CAST('a' AS VARCHAR(3)), CAST('b' AS VARCHAR(3)))) OR x = 3e0")
                .equalTo(standardInputStatistics);
    }

    @Test
    public void testUnsupportedExpression()
    {
        assertExpression("sin(x)")
                .outputRowsCountUnknown();
        assertExpression("x = sin(x)")
                .outputRowsCountUnknown();
    }

    @Test
    public void testAndStats()
    {
        assertExpression("x < 0e0 AND x > DOUBLE '-7.5'")
                .outputRowsCount(281.25)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(15.0)
                                .nullsFraction(0.0));

        // Impossible, with symbol-to-expression comparisons
        assertExpression("x = (0e0 + 1e0) AND x = (0e0 + 3e0)")
                .outputRowsCount(0)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), VariableStatsAssertion::emptyRange)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), VariableStatsAssertion::emptyRange);

        // first argument unknown
        assertExpression("json_array_contains(JSON '[]', x) AND x < 0e0")
                .outputRowsCount(337.5)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.lowValue(-10)
                                .highValue(0)
                                .distinctValuesCount(20)
                                .nullsFraction(0));

        // second argument unknown
        assertExpression("x < 0e0 AND json_array_contains(JSON '[]', x)")
                .outputRowsCount(337.5)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.lowValue(-10)
                                .highValue(0)
                                .distinctValuesCount(20)
                                .nullsFraction(0));

        // both arguments unknown
        assertExpression("json_array_contains(JSON '[11]', x) AND json_array_contains(JSON '[13]', x)")
                .outputRowsCountUnknown();

        assertExpression("'a' IN ('b', 'c') AND unknownRange = 3e0")
                .outputRowsCount(0);
    }

    @Test
    public void testNotStats()
    {
        assertExpression("NOT(x < 0e0)")
                .outputRowsCount(625) // FIXME - nulls shouldn't be restored
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.4)) // FIXME - nulls shouldn't be restored
                .variableStats(new VariableReferenceExpression("y", DOUBLE), variableAssert -> variableAssert.isEqualTo(yStats));

        assertExpression("NOT(x IS NULL)")
                .outputRowsCount(750)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableAssert ->
                        variableAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .distinctValuesCount(40.0)
                                .nullsFraction(0))
                .variableStats(new VariableReferenceExpression("y", DOUBLE), variableAssert -> variableAssert.isEqualTo(yStats));

        assertExpression("NOT(json_array_contains(JSON '[]', x))")
                .outputRowsCountUnknown();
    }

    @Test
    public void testIsNullFilter()
    {
        assertExpression("x IS NULL")
                .outputRowsCount(250.0)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(0)
                                .emptyRange()
                                .nullsFraction(1.0));

        assertExpression("emptyRange IS NULL")
                .outputRowsCount(1000.0)
                .variableStats(new VariableReferenceExpression("emptyRange", DOUBLE), VariableStatsAssertion::empty);
    }

    @Test
    public void testIsNotNullFilter()
    {
        assertExpression("x IS NOT NULL")
                .outputRowsCount(750.0)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(40.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        assertExpression("emptyRange IS NOT NULL")
                .outputRowsCount(0.0)
                .variableStats(new VariableReferenceExpression("emptyRange", DOUBLE), VariableStatsAssertion::empty);
    }

    @Test
    public void testBetweenOperatorFilter()
    {
        // Only right side cut
        assertExpression("x BETWEEN 7.5e0 AND 12e0")
                .outputRowsCount(93.75)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(5.0)
                                .lowValue(7.5)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        // Only left side cut
        assertExpression("x BETWEEN DOUBLE '-12' AND DOUBLE '-7.5'")
                .outputRowsCount(93.75)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(5.0)
                                .lowValue(-10)
                                .highValue(-7.5)
                                .nullsFraction(0.0));
        assertExpression("x BETWEEN -12e0 AND -7.5e0")
                .outputRowsCount(93.75)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(5.0)
                                .lowValue(-10)
                                .highValue(-7.5)
                                .nullsFraction(0.0));

        // Both sides cut
        assertExpression("x BETWEEN DOUBLE '-2.5' AND 2.5e0")
                .outputRowsCount(187.5)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(10.0)
                                .lowValue(-2.5)
                                .highValue(2.5)
                                .nullsFraction(0.0));

        // Both sides cut unknownRange
        assertExpression("unknownRange BETWEEN 2.72e0 AND 3.14e0")
                .outputRowsCount(112.5)
                .variableStats(new VariableReferenceExpression("unknownRange", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(6.25)
                                .lowValue(2.72)
                                .highValue(3.14)
                                .nullsFraction(0.0));

        // Left side open, cut on open side
        assertExpression("leftOpen BETWEEN DOUBLE '-10' AND 10e0")
                .outputRowsCount(180.0)
                .variableStats(new VariableReferenceExpression("leftOpen", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(10.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        // Right side open, cut on open side
        assertExpression("rightOpen BETWEEN DOUBLE '-10' AND 10e0")
                .outputRowsCount(180.0)
                .variableStats(new VariableReferenceExpression("rightOpen", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(10.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        // Filter all
        assertExpression("y BETWEEN 27.5e0 AND 107e0")
                .outputRowsCount(0.0)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), VariableStatsAssertion::empty);

        // Filter nothing
        assertExpression("y BETWEEN DOUBLE '-100' AND 100e0")
                .outputRowsCount(500.0)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(20.0)
                                .lowValue(0.0)
                                .highValue(5.0)
                                .nullsFraction(0.0));

        // Filter non exact match
        assertExpression("z BETWEEN DOUBLE '-100' AND 100e0")
                .outputRowsCount(900.0)
                .variableStats(new VariableReferenceExpression("z", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(5.0)
                                .lowValue(-100.0)
                                .highValue(100.0)
                                .nullsFraction(0.0));

        assertExpression("'a' IN ('a', 'b')").equalTo(standardInputStatistics);
        assertExpression("'a' IN ('b', 'c')").outputRowsCount(0);
        assertExpression("CAST('b' AS VARCHAR(3)) IN (CAST('a' AS VARCHAR(3)), CAST('b' AS VARCHAR(3)))").equalTo(standardInputStatistics);
        assertExpression("CAST('c' AS VARCHAR(3)) IN (CAST('a' AS VARCHAR(3)), CAST('b' AS VARCHAR(3)))").outputRowsCount(0);
    }

    @Test
    public void testSymbolEqualsSameSymbolFilter()
    {
        assertExpression("x = x")
                .outputRowsCount(750)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        VariableStatsEstimate.builder()
                                .setAverageRowSize(4.0)
                                .setDistinctValuesCount(40.0)
                                .setLowValue(-10.0)
                                .setHighValue(10.0)
                                .build());
    }

    @Test
    public void testInPredicateFilter()
    {
        assertExpression("x IN (null)").outputRowsCount(0.0);

        // One value in range
        assertExpression("x IN (7.5e0)")
                .outputRowsCount(18.75)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(1.0)
                                .lowValue(7.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));
        assertExpression("x IN (DOUBLE '-7.5')")
                .outputRowsCount(18.75)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(1.0)
                                .lowValue(-7.5)
                                .highValue(-7.5)
                                .nullsFraction(0.0));
        assertExpression("x IN (BIGINT '2' + 5.5e0)")
                .outputRowsCount(18.75)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(1.0)
                                .lowValue(7.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));
        assertExpression("x IN (-7.5e0)")
                .outputRowsCount(18.75)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(1.0)
                                .lowValue(-7.5)
                                .highValue(-7.5)
                                .nullsFraction(0.0));

        // Multiple values in range
        assertExpression("x IN (1.5e0, 2.5e0, 7.5e0)")
                .outputRowsCount(56.25)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(3.0)
                                .lowValue(1.5)
                                .highValue(7.5)
                                .nullsFraction(0.0))
                .variableStats(new VariableReferenceExpression("y", DOUBLE), variableStats ->
                        // Symbol not involved in the comparison should have stats basically unchanged
                        variableStats.distinctValuesCount(20.0)
                                .lowValue(0.0)
                                .highValue(5)
                                .nullsFraction(0.5));

        // Multiple values some in some out of range
        assertExpression("x IN (DOUBLE '-42', 1.5e0, 2.5e0, 7.5e0, 314e0)")
                .outputRowsCount(56.25)
                .variableStats(new VariableReferenceExpression("x", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(3.0)
                                .lowValue(1.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));

        // Multiple values in unknown range
        assertExpression("unknownRange IN (DOUBLE '-42', 1.5e0, 2.5e0, 7.5e0, 314e0)")
                .outputRowsCount(90.0)
                .variableStats(new VariableReferenceExpression("unknownRange", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(5.0)
                                .lowValue(-42.0)
                                .highValue(314.0)
                                .nullsFraction(0.0));

        // Casted literals as value
        assertExpression(format("mediumVarchar IN (CAST('abc' AS %s))", MEDIUM_VARCHAR_TYPE.toString()))
                .outputRowsCount(4)
                .variableStats(new VariableReferenceExpression("mediumVarchar", MEDIUM_VARCHAR_TYPE), variableStats ->
                        variableStats.distinctValuesCount(1)
                                .nullsFraction(0.0));

        assertExpression(format("mediumVarchar IN (CAST('abc' AS %1$s), CAST('def' AS %1$s))", MEDIUM_VARCHAR_TYPE.toString()))
                .outputRowsCount(8)
                .variableStats(new VariableReferenceExpression("mediumVarchar", MEDIUM_VARCHAR_TYPE), variableStats ->
                        variableStats.distinctValuesCount(2)
                                .nullsFraction(0.0));

        // No value in range
        assertExpression("y IN (DOUBLE '-42', 6e0, 31.1341e0, DOUBLE '-0.000000002', 314e0)")
                .outputRowsCount(0.0)
                .variableStats(new VariableReferenceExpression("y", DOUBLE), VariableStatsAssertion::empty);

        // More values in range than distinct values
        assertExpression("z IN (DOUBLE '-1', 3.14e0, 0e0, 1e0, 2e0, 3e0, 4e0, 5e0, 6e0, 7e0, 8e0, DOUBLE '-2')")
                .outputRowsCount(900.0)
                .variableStats(new VariableReferenceExpression("z", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(5.0)
                                .lowValue(-2.0)
                                .highValue(8.0)
                                .nullsFraction(0.0));

        // Values in weird order
        assertExpression("z IN (DOUBLE '-1', 1e0, 0e0)")
                .outputRowsCount(540.0)
                .variableStats(new VariableReferenceExpression("z", DOUBLE), variableStats ->
                        variableStats.distinctValuesCount(3.0)
                                .lowValue(-1.0)
                                .highValue(1.0)
                                .nullsFraction(0.0));
    }

    private PlanNodeStatsAssertion assertExpression(String expression)
    {
        return assertExpression(expression(expression));
    }

    private PlanNodeStatsAssertion assertExpression(Expression expression)
    {
        // assert both visitors yield the same result
        RowExpression rowExpression = translator.translateAndOptimize(expression, standardTypes);
        PlanNodeStatsEstimate expressionStatsEstimate = statsCalculator.filterStats(standardInputStatistics, expression, session, standardTypes);
        PlanNodeStatsEstimate rowExpressionStatsEstimate = statsCalculator.filterStats(standardInputStatistics, rowExpression, session);
        assertEquals(expressionStatsEstimate, rowExpressionStatsEstimate);

        return PlanNodeStatsAssertion.assertThat(expressionStatsEstimate);
    }
}
