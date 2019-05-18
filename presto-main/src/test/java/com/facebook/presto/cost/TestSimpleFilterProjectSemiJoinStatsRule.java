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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestSimpleFilterProjectSemiJoinStatsRule
        extends BaseStatsCalculatorTest
{
    private SymbolStatsEstimate aStats = SymbolStatsEstimate.builder()
            .setLowValue(0)
            .setHighValue(10)
            .setDistinctValuesCount(10)
            .setNullsFraction(0.1)
            .build();

    private SymbolStatsEstimate bStats = SymbolStatsEstimate.builder()
            .setLowValue(0)
            .setHighValue(100)
            .setDistinctValuesCount(10)
            .setNullsFraction(0)
            .build();

    private SymbolStatsEstimate cStats = SymbolStatsEstimate.builder()
            .setLowValue(5)
            .setHighValue(30)
            .setDistinctValuesCount(2)
            .setNullsFraction(0.5)
            .build();

    private SymbolStatsEstimate expectedAInC = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(2)
            .setLowValue(0)
            .setHighValue(10)
            .setNullsFraction(0)
            .build();

    private SymbolStatsEstimate expectedANotInC = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(1.6)
            .setLowValue(0)
            .setHighValue(8)
            .setNullsFraction(0)
            .build();

    private SymbolStatsEstimate expectedANotInCWithExtraFilter = SymbolStatsEstimate.builder()
            .setDistinctValuesCount(8)
            .setLowValue(0)
            .setHighValue(10)
            .setNullsFraction(0)
            .build();

    private static final PlanNodeId LEFT_SOURCE_ID = new PlanNodeId("left_source_values");
    private static final PlanNodeId RIGHT_SOURCE_ID = new PlanNodeId("right_source_values");
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(MetadataManager.createTestMetadataManager());

    @DataProvider(name = "toRowExpression")
    public Object[][] toRowExpressionProvider()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "toRowExpression")
    public void testFilterPositiveSemiJoin(boolean toRowExpression)
    {
        getStatsCalculatorAssertion(new Symbol("sjo").toSymbolReference(), toRowExpression)
                .withSourceStats(LEFT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(new Symbol("a"), aStats)
                        .addSymbolStatistics(new Symbol("b"), bStats)
                        .build())
                .withSourceStats(RIGHT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addSymbolStatistics(new Symbol("c"), cStats)
                        .build())
                .check(check -> check.outputRowsCount(180)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedAInC))
                            .symbolStats("b", assertion -> assertion.isEqualTo(bStats))
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo"));
    }

    @Test(dataProvider = "toRowExpression")
    public void testFilterPositiveNarrowingProjectSemiJoin(boolean toRowExpression)
    {
        tester().assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);

            PlanNode semiJoinNode = pb.semiJoin(
                    pb.values(LEFT_SOURCE_ID, a, b),
                    pb.values(RIGHT_SOURCE_ID, c),
                    a,
                    c,
                    semiJoinOutput,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            if (toRowExpression) {
                return pb.filter(
                        TRANSLATOR.translateAndOptimize(expression("sjo"), pb.getTypes()),
                        pb.project(Assignments.identity(semiJoinOutput, a), semiJoinNode));
            }
            return pb.filter(expression("sjo"), pb.project(Assignments.identity(semiJoinOutput, a), semiJoinNode));
        })
                .withSourceStats(LEFT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(new Symbol("a"), aStats)
                        .addSymbolStatistics(new Symbol("b"), bStats)
                        .build())
                .withSourceStats(RIGHT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addSymbolStatistics(new Symbol("c"), cStats)
                        .build())
                .check(check -> check.outputRowsCount(180)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedAInC))
                            .symbolStatsUnknown("b")
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo"));
    }

    @Test(dataProvider = "toRowExpression")
    public void testFilterPositivePlusExtraConjunctSemiJoin(boolean toRowExpression)
    {
        getStatsCalculatorAssertion(expression("sjo AND a < 8"), toRowExpression)
                .withSourceStats(LEFT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(new Symbol("a"), aStats)
                        .addSymbolStatistics(new Symbol("b"), bStats)
                        .build())
                .withSourceStats(RIGHT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addSymbolStatistics(new Symbol("c"), cStats)
                        .build())
                .check(check -> check.outputRowsCount(144)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedANotInC))
                            .symbolStats("b", assertion -> assertion.isEqualTo(bStats))
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo"));
    }

    @Test(dataProvider = "toRowExpression")
    public void testFilterNegativeSemiJoin(boolean toRowExpression)
    {
        getStatsCalculatorAssertion(expression("NOT sjo"), toRowExpression)
                .withSourceStats(LEFT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(new Symbol("a"), aStats)
                        .addSymbolStatistics(new Symbol("b"), bStats)
                        .build())
                .withSourceStats(RIGHT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addSymbolStatistics(new Symbol("c"), cStats)
                        .build())
                .check(check -> check.outputRowsCount(720)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedANotInCWithExtraFilter))
                            .symbolStats("b", assertion -> assertion.isEqualTo(bStats))
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo"));
    }

    private StatsCalculatorAssertion getStatsCalculatorAssertion(Expression expression, boolean toRowExpression)
    {
        return tester().assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);

            PlanNode semiJoinNode = pb.semiJoin(
                    pb.values(LEFT_SOURCE_ID, a, b),
                    pb.values(RIGHT_SOURCE_ID, c),
                    a,
                    c,
                    semiJoinOutput,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            if (toRowExpression) {
                return pb.filter(TRANSLATOR.translateAndOptimize(expression, pb.getTypes()), semiJoinNode);
            }
            return pb.filter(expression, semiJoinNode);
        });
    }
}
