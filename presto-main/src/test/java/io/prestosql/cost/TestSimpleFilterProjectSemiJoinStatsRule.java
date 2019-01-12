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
package io.prestosql.cost;

import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;

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

    @Test
    public void testFilterPositiveSemiJoin()
    {
        tester().assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);
            return pb.filter(
                    semiJoinOutput.toSymbolReference(),
                    pb.semiJoin(
                            pb.values(LEFT_SOURCE_ID, a, b),
                            pb.values(RIGHT_SOURCE_ID, c),
                            a,
                            c,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
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
                .check(check -> {
                    check.outputRowsCount(180)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedAInC))
                            .symbolStats("b", assertion -> assertion.isEqualTo(bStats))
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo");
                });
    }

    @Test
    public void testFilterPositiveNarrowingProjectSemiJoin()
    {
        tester().assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);
            return pb.filter(
                    expression("sjo"),
                    pb.project(Assignments.identity(semiJoinOutput, a),
                            pb.semiJoin(
                                    pb.values(LEFT_SOURCE_ID, a, b),
                                    pb.values(RIGHT_SOURCE_ID, c),
                                    a,
                                    c,
                                    semiJoinOutput,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty())));
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
                .check(check -> {
                    check.outputRowsCount(180)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedAInC))
                            .symbolStatsUnknown("b")
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo");
                });
    }

    @Test
    public void testFilterPositivePlusExtraConjunctSemiJoin()
    {
        tester().assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);
            return pb.filter(
                    expression("sjo AND a < 8"),
                    pb.semiJoin(
                            pb.values(LEFT_SOURCE_ID, a, b),
                            pb.values(RIGHT_SOURCE_ID, c),
                            a,
                            c,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
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
                .check(check -> {
                    check.outputRowsCount(144)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedANotInC))
                            .symbolStats("b", assertion -> assertion.isEqualTo(bStats))
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo");
                });
    }

    @Test
    public void testFilterNegativeSemiJoin()
    {
        tester().assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);
            return pb.filter(
                    expression("NOT sjo"),
                    pb.semiJoin(
                            pb.values(LEFT_SOURCE_ID, a, b),
                            pb.values(RIGHT_SOURCE_ID, c),
                            a,
                            c,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
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
                .check(check -> {
                    check.outputRowsCount(720)
                            .symbolStats("a", assertion -> assertion.isEqualTo(expectedANotInCWithExtraFilter))
                            .symbolStats("b", assertion -> assertion.isEqualTo(bStats))
                            .symbolStatsUnknown("c")
                            .symbolStatsUnknown("sjo");
                });
    }
}
