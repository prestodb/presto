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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;

public class TestSimpleFilterProjectSemiJoinStatsRule
        extends BaseStatsCalculatorTest
{
    private VariableStatsEstimate aStats = VariableStatsEstimate.builder()
            .setLowValue(0)
            .setHighValue(10)
            .setDistinctValuesCount(10)
            .setNullsFraction(0.1)
            .build();

    private VariableStatsEstimate bStats = VariableStatsEstimate.builder()
            .setLowValue(0)
            .setHighValue(100)
            .setDistinctValuesCount(10)
            .setNullsFraction(0)
            .build();

    private VariableStatsEstimate cStats = VariableStatsEstimate.builder()
            .setLowValue(5)
            .setHighValue(30)
            .setDistinctValuesCount(2)
            .setNullsFraction(0.5)
            .build();

    private VariableStatsEstimate expectedAInC = VariableStatsEstimate.builder()
            .setDistinctValuesCount(2)
            .setLowValue(0)
            .setHighValue(10)
            .setNullsFraction(0)
            .build();

    private VariableStatsEstimate expectedANotInC = VariableStatsEstimate.builder()
            .setDistinctValuesCount(1.6)
            .setLowValue(0)
            .setHighValue(8)
            .setNullsFraction(0)
            .build();

    private VariableStatsEstimate expectedANotInCWithExtraFilter = VariableStatsEstimate.builder()
            .setDistinctValuesCount(8)
            .setLowValue(0)
            .setHighValue(10)
            .setNullsFraction(0)
            .build();

    private static final PlanNodeId LEFT_SOURCE_ID = new PlanNodeId("left_source_values");
    private static final PlanNodeId RIGHT_SOURCE_ID = new PlanNodeId("right_source_values");
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(MetadataManager.createTestMetadataManager());

    @Test
    public void testFilterPositiveSemiJoin()
    {
        getStatsCalculatorAssertion(new SymbolReference("sjo"))
                .withSourceStats(LEFT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), aStats)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "b", BIGINT), bStats)
                        .build())
                .withSourceStats(RIGHT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "c", BIGINT), cStats)
                        .build())
                .check(check -> check.outputRowsCount(180)
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), assertion -> assertion.isEqualTo(expectedAInC))
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "b", BIGINT), assertion -> assertion.isEqualTo(bStats))
                        .variableStatsUnknown("c")
                        .variableStatsUnknown("sjo"));
    }

    @Test
    public void testFilterPositiveNarrowingProjectSemiJoin()
    {
        tester().assertStatsFor(pb -> {
            VariableReferenceExpression a = pb.variable("a", BIGINT);
            VariableReferenceExpression b = pb.variable("b", BIGINT);
            VariableReferenceExpression c = pb.variable("c", BIGINT);
            VariableReferenceExpression semiJoinOutput = pb.variable("sjo", BOOLEAN);

            PlanNode semiJoinNode = pb.semiJoin(
                    pb.values(LEFT_SOURCE_ID, a, b),
                    pb.values(RIGHT_SOURCE_ID, c),
                    a,
                    c,
                    semiJoinOutput,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            return pb.filter(
                    TRANSLATOR.translate(expression("sjo"), pb.getTypes()),
                    pb.project(identityAssignments(semiJoinOutput, a), semiJoinNode));
        })
                .withSourceStats(LEFT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), aStats)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "b", BIGINT), bStats)
                        .build())
                .withSourceStats(RIGHT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "c", BIGINT), cStats)
                        .build())
                .check(check -> check.outputRowsCount(180)
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), assertion -> assertion.isEqualTo(expectedAInC))
                        .variableStatsUnknown("b")
                        .variableStatsUnknown("c")
                        .variableStatsUnknown("sjo"));
    }

    @Test
    public void testFilterPositivePlusExtraConjunctSemiJoin()
    {
        getStatsCalculatorAssertion(expression("sjo AND a < 8"))
                .withSourceStats(LEFT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), aStats)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "b", BIGINT), bStats)
                        .build())
                .withSourceStats(RIGHT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "c", BIGINT), cStats)
                        .build())
                .check(check -> check.outputRowsCount(144)
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), assertion -> assertion.isEqualTo(expectedANotInC))
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "b", BIGINT), assertion -> assertion.isEqualTo(bStats))
                        .variableStatsUnknown("c")
                        .variableStatsUnknown("sjo"));
    }

    @Test
    public void testFilterNegativeSemiJoin()
    {
        getStatsCalculatorAssertion(expression("NOT sjo"))
                .withSourceStats(LEFT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), aStats)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "b", BIGINT), bStats)
                        .build())
                .withSourceStats(RIGHT_SOURCE_ID, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2000)
                        .addVariableStatistics(new VariableReferenceExpression(Optional.empty(), "c", BIGINT), cStats)
                        .build())
                .check(check -> check.outputRowsCount(720)
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), assertion -> assertion.isEqualTo(expectedANotInCWithExtraFilter))
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "b", BIGINT), assertion -> assertion.isEqualTo(bStats))
                        .variableStatsUnknown("c")
                        .variableStatsUnknown("sjo"));
    }

    private StatsCalculatorAssertion getStatsCalculatorAssertion(Expression expression)
    {
        return tester().assertStatsFor(pb -> {
            VariableReferenceExpression a = pb.variable("a", BIGINT);
            VariableReferenceExpression b = pb.variable("b", BIGINT);
            VariableReferenceExpression c = pb.variable("c", BIGINT);
            VariableReferenceExpression semiJoinOutput = pb.variable("sjo", BOOLEAN);

            PlanNode semiJoinNode = pb.semiJoin(
                    pb.values(LEFT_SOURCE_ID, a, b),
                    pb.values(RIGHT_SOURCE_ID, c),
                    a,
                    c,
                    semiJoinOutput,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            return pb.filter(TRANSLATOR.translate(expression, pb.getTypes()), semiJoinNode);
        });
    }
}
