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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.BiFunction;

import static com.facebook.presto.SystemSessionProperties.SEGMENTED_AGGREGATION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests {@code segmented_aggregation_enabled} for a partial aggregation pushed below an exchange, which is
 * the only aggregation that ends up on the ordered input.
 */
public class TestSegmentedPartialAggregation
        extends BaseRuleTest
{
    /**
     * Sorted on {@code a} and grouped by {@code (a, b)}: the partial can flush whenever {@code a} changes.
     */
    @Test
    public void testPrefixOfGroupingKeysIsSegmented()
    {
        PlanNode result = applyRule(true, (p, variables) -> p.sort(ImmutableList.of(variables.get(0)), values(p, variables)));
        assertEquals(preGroupedNames(result), ImmutableList.of("a"));
    }

    /**
     * Sorted on both grouping keys, so the partial aggregation can stream and needs no hash table.
     */
    @Test
    public void testAllGroupingKeysPreGroupedStreams()
    {
        PlanNode result = applyRule(true, (p, variables) -> p.sort(ImmutableList.of(variables.get(0), variables.get(1)), values(p, variables)));
        assertEquals(preGroupedNames(result), ImmutableList.of("a", "b"));
    }

    /**
     * Sorted on {@code (a, b)} and grouped by {@code (a, b, c)}: only {@code a} is claimed. The operator
     * flushes whenever any pre-grouped variable changes, so claiming the shortest prefix flushes the least,
     * at the cost of holding {@code (b, c)} in the hash table for longer.
     */
    @Test
    public void testOnlyTheLeadingSortedColumnIsClaimed()
    {
        PlanNode result = tester().assertThat(new PushPartialAggregationThroughExchange(tester().getMetadata(), getFunctionManager(), false))
                .setSystemProperty(SEGMENTED_AGGREGATION_ENABLED, "true")
                .setSystemProperty(STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED, "false")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression c = p.variable("c", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("sum_x", BIGINT), p.rowExpression("sum(x)"))
                            .singleGroupingSet(a, b, c)
                            .step(PARTIAL)
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.sort(ImmutableList.of(a, b), p.values(a, b, c, x)))));
                })
                .get();
        assertEquals(preGroupedNames(result), ImmutableList.of("a"), "Only the leading sorted column should be claimed");
    }

    /**
     * A cross join against a single row keeps the probe's order: the nested loop join emits the probe
     * unchanged with the build's columns appended, so the input is still grouped for the aggregation above
     * it. This is the shape of a broadcast scalar, such as a total joined onto every row.
     */
    @Test
    public void testOrderSurvivesCrossJoinWithScalarBuild()
    {
        PlanNode result = tester().assertThat(new PushPartialAggregationThroughExchange(tester().getMetadata(), getFunctionManager(), false))
                .setSystemProperty(SEGMENTED_AGGREGATION_ENABLED, "true")
                .setSystemProperty(STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED, "false")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression total = p.variable("total", BIGINT);
                    PlanNode scalarBuild = p.aggregation(agg -> agg
                            .addAggregation(total, p.rowExpression("count()"))
                            .globalGrouping()
                            .source(p.values(p.variable("ignored", BIGINT))));
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("sum_x", BIGINT), p.rowExpression("sum(x)"))
                            .singleGroupingSet(a, b)
                            .step(PARTIAL)
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.join(
                                            JoinType.INNER,
                                            p.sort(ImmutableList.of(a), p.values(a, b, x)),
                                            scalarBuild))));
                })
                .get();
        assertEquals(preGroupedNames(result), ImmutableList.of("a"), "A scalar cross join must not hide the input's order");
    }

    /**
     * The order is not claimed across a local exchange. A local exchange splits the input into streams, and
     * a source with several drivers feeds it pages from different splits, so the runs an aggregation would
     * flush on are interleaved rather than merely shortened. Property derivation reports no local
     * properties for one, and this rule does not second guess it.
     */
    @Test
    public void testOrderIsNotClaimedAcrossLocalExchange()
    {
        PlanNode result = applyRule(true, (p, variables) -> roundRobinExchange(
                p.getIdAllocator().getNextId(),
                ExchangeNode.Scope.LOCAL,
                p.sort(ImmutableList.of(variables.get(0)), values(p, variables))));
        assertTrue(preGroupedNames(result).isEmpty(), "A local exchange does not preserve the input's order");
    }

    /**
     * streaming_for_partial_aggregation_enabled marks every grouping key, and segmenting must not narrow
     * that to a prefix when both properties are on.
     */
    @Test
    public void testStreamingForPartialAggregationIsNotNarrowed()
    {
        PlanNode result = tester().assertThat(new PushPartialAggregationThroughExchange(tester().getMetadata(), getFunctionManager(), false))
                .setSystemProperty(SEGMENTED_AGGREGATION_ENABLED, "true")
                .setSystemProperty(STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("sum_x", BIGINT), p.rowExpression("sum(x)"))
                            .singleGroupingSet(a, b)
                            .preGroupedVariables(a, b)
                            .step(PARTIAL)
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.sort(ImmutableList.of(a), p.values(a, b, x)))));
                })
                .get();
        assertEquals(preGroupedNames(result), ImmutableList.of("a", "b"), "An existing pre-grouped decision must be left alone");
    }

    @Test
    public void testUnsortedSourceIsNotSegmented()
    {
        PlanNode result = applyRule(true, (p, variables) -> values(p, variables));
        assertTrue(preGroupedNames(result).isEmpty(), "An unsorted input has no pre-grouped prefix");
    }

    @Test
    public void testDisabledByDefault()
    {
        PlanNode result = applyRule(false, (p, variables) -> p.sort(ImmutableList.of(variables.get(0)), values(p, variables)));
        assertTrue(preGroupedNames(result).isEmpty(), "No pre-grouped variables expected when the session property is off");
    }

    private static PlanNode values(PlanBuilder planBuilder, List<VariableReferenceExpression> variables)
    {
        return planBuilder.values(variables.get(0), variables.get(1), variables.get(2));
    }

    /**
     * Builds {@code PARTIAL aggregation on (a, b) -> remote exchange -> source} and applies the rule, which
     * pushes the aggregation below the exchange.
     */
    private PlanNode applyRule(boolean enabled, BiFunction<PlanBuilder, List<VariableReferenceExpression>, PlanNode> source)
    {
        return tester().assertThat(new PushPartialAggregationThroughExchange(tester().getMetadata(), getFunctionManager(), false))
                .setSystemProperty(SEGMENTED_AGGREGATION_ENABLED, enabled ? "true" : "false")
                .setSystemProperty(STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED, "false")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression b = p.variable("b", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(agg -> agg
                            .addAggregation(p.variable("sum_x", BIGINT), p.rowExpression("sum(x)"))
                            .singleGroupingSet(a, b)
                            .step(PARTIAL)
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    source.apply(p, ImmutableList.of(a, b, x)))));
                })
                .get();
    }

    /**
     * The rule rewrites the exchange so that every branch is {@code Project(PARTIAL aggregation)}. Walk the
     * result structurally: the untouched parts of the plan are still group references, which cannot be
     * traversed.
     */
    private static List<String> preGroupedNames(PlanNode plan)
    {
        assertEquals(plan.getSources().size(), 1, "Expected a single exchange branch");
        ProjectNode project = (ProjectNode) plan.getSources().get(0);
        AggregationNode partial = (AggregationNode) project.getSource();
        assertEquals(partial.getStep(), PARTIAL);
        return partial.getPreGroupedVariables().stream()
                .map(VariableReferenceExpression::getName)
                .collect(toImmutableList());
    }
}
