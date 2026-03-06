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

import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.MVRewriteCandidatesNode;
import com.facebook.presto.spi.plan.MVRewriteCandidatesNode.MVRewriteCandidate;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for {@link SelectLowestCostMVRewrite}, the cost-based optimizer rule
 * that selects the lowest-cost plan from the original query and all
 * materialized view rewrite candidates.
 *
 * Stats are overridden on ValuesNode (source) IDs because
 * CostCalculatorUsingExchanges.visitFilter() computes cost from
 * getStats(node.getSource()), not the filter's own stats.
 */
@Test(singleThreaded = true)
public class TestSelectLowestCostMVRewrite
{
    private static final CostComparator COST_COMPARATOR = new CostComparator(1, 1, 1);
    private static final int NODES_COUNT = 4;

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(ImmutableList.of(), ImmutableMap.of(
                "materialized_view_query_rewrite_cost_based_selection_enabled", "true"), Optional.of(NODES_COUNT));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testSelectsCheaperCandidate()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", statsEstimate(10000, 80000))
                .overrideStats("mv1Src", statsEstimate(100, 800))
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(new MVRewriteCandidate(
                                    p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv1Src"), 1, col)),
                                    "catalog", "schema", "mv1")),
                            ImmutableList.of(col));
                })
                .get();
        // MV candidate should be selected; verify it reads from mv1Src
        assertTrue(result instanceof FilterNode);
        assertEquals(((FilterNode) result).getSource().getId(), new PlanNodeId("mv1Src"));
    }

    @Test
    public void testKeepsOriginalWhenCheaper()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", statsEstimate(100, 800))
                .overrideStats("mv1Src", statsEstimate(10000, 80000))
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(new MVRewriteCandidate(
                                    p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv1Src"), 1, col)),
                                    "catalog", "schema", "mv1")),
                            ImmutableList.of(col));
                })
                .get();
        assertTrue(result instanceof FilterNode);
        assertEquals(((FilterNode) result).getSource().getId(), new PlanNodeId("origSrc"));
    }

    @Test
    public void testKeepsOriginalWhenCostsAreEqual()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", statsEstimate(100, 800))
                .overrideStats("mv1Src", statsEstimate(100, 800))
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(new MVRewriteCandidate(
                                    p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv1Src"), 1, col)),
                                    "catalog", "schema", "mv1")),
                            ImmutableList.of(col));
                })
                .get();
        assertTrue(result instanceof FilterNode);
        assertEquals(((FilterNode) result).getSource().getId(), new PlanNodeId("origSrc"));
    }

    @Test
    public void testSelectsCheapestAmongMultipleCandidates()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", statsEstimate(10000, 80000))
                .overrideStats("mv1Src", statsEstimate(5000, 40000))
                .overrideStats("mv2Src", statsEstimate(50, 400))
                .overrideStats("mv3Src", statsEstimate(1000, 8000))
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(
                                    new MVRewriteCandidate(
                                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv1Src"), 1, col)),
                                            "catalog", "schema", "mv1"),
                                    new MVRewriteCandidate(
                                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv2Src"), 1, col)),
                                            "catalog", "schema", "mv2"),
                                    new MVRewriteCandidate(
                                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv3Src"), 1, col)),
                                            "catalog", "schema", "mv3")),
                            ImmutableList.of(col));
                })
                .get();
        // mv2 has the lowest cost (50 rows)
        assertTrue(result instanceof FilterNode);
        assertEquals(((FilterNode) result).getSource().getId(), new PlanNodeId("mv2Src"));
    }

    @Test
    public void testSkipsCandidateWithUnknownCost()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", statsEstimate(100, 800))
                .overrideStats("mv1Src", PlanNodeStatsEstimate.unknown())
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(new MVRewriteCandidate(
                                    p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv1Src"), 1, col)),
                                    "catalog", "schema", "mv1")),
                            ImmutableList.of(col));
                })
                .get();
        assertTrue(result instanceof FilterNode);
        assertEquals(((FilterNode) result).getSource().getId(), new PlanNodeId("origSrc"));
    }

    @Test
    public void testPrefersCandidateWhenOriginalCostIsUnknown()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", PlanNodeStatsEstimate.unknown())
                .overrideStats("mv1Src", statsEstimate(100, 800))
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(new MVRewriteCandidate(
                                    p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv1Src"), 1, col)),
                                    "catalog", "schema", "mv1")),
                            ImmutableList.of(col));
                })
                .get();
        assertTrue(result instanceof FilterNode);
        assertEquals(((FilterNode) result).getSource().getId(), new PlanNodeId("mv1Src"));
    }

    @Test
    public void testSelectsOriginalWhenAllCostsAreUnknown()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", PlanNodeStatsEstimate.unknown())
                .overrideStats("mv1Src", PlanNodeStatsEstimate.unknown())
                .overrideStats("mv2Src", PlanNodeStatsEstimate.unknown())
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(
                                    new MVRewriteCandidate(
                                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv1Src"), 1, col)),
                                            "catalog", "schema", "mv1"),
                                    new MVRewriteCandidate(
                                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv2Src"), 1, col)),
                                            "catalog", "schema", "mv2")),
                            ImmutableList.of(col));
                })
                .get();
        // All unknown: falls back to original
        assertTrue(result instanceof FilterNode);
        assertEquals(((FilterNode) result).getSource().getId(), new PlanNodeId("origSrc"));
    }

    @Test
    public void testNoCandidatesSelectsOriginal()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", statsEstimate(100, 800))
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(),
                            ImmutableList.of(col));
                })
                .get();
        assertTrue(result instanceof FilterNode);
        assertEquals(((FilterNode) result).getSource().getId(), new PlanNodeId("origSrc"));
    }

    @Test
    public void testAddsProjectionForDifferentOutputVariables()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", statsEstimate(10000, 80000))
                .overrideStats("mv1Src", statsEstimate(100, 800))
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    VariableReferenceExpression mvCol = p.variable("mv_col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(new MVRewriteCandidate(
                                    p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv1Src"), 1, mvCol)),
                                    "catalog", "schema", "mv1")),
                            ImmutableList.of(col));
                })
                .get();
        assertTrue(result instanceof ProjectNode);
        assertEquals(result.getOutputVariables().size(), 1);
        assertEquals(result.getOutputVariables().get(0).getName(), "col");
    }

    @Test
    public void testMixedKnownUnknownCandidateCosts()
    {
        PlanNode result = tester.assertThat(new SelectLowestCostMVRewrite(COST_COMPARATOR))
                .overrideStats("origSrc", statsEstimate(10000, 80000))
                .overrideStats("mv1Src", PlanNodeStatsEstimate.unknown())
                .overrideStats("mv2Src", statsEstimate(500, 4000))
                .overrideStats("mv3Src", PlanNodeStatsEstimate.unknown())
                .on(p -> {
                    VariableReferenceExpression col = p.variable("col", BIGINT);
                    return new MVRewriteCandidatesNode(
                            Optional.empty(),
                            new PlanNodeId("mvnode"),
                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("origSrc"), 1, col)),
                            ImmutableList.of(
                                    new MVRewriteCandidate(
                                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv1Src"), 1, col)),
                                            "catalog", "schema", "mv1"),
                                    new MVRewriteCandidate(
                                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv2Src"), 1, col)),
                                            "catalog", "schema", "mv2"),
                                    new MVRewriteCandidate(
                                            p.filter(TRUE_CONSTANT, p.values(new PlanNodeId("mv3Src"), 1, col)),
                                            "catalog", "schema", "mv3")),
                            ImmutableList.of(col));
                })
                .get();
        // mv2 is the only candidate with known cost, and it's cheaper than original
        assertTrue(result instanceof FilterNode);
        assertEquals(((FilterNode) result).getSource().getId(), new PlanNodeId("mv2Src"));
    }

    private static PlanNodeStatsEstimate statsEstimate(double rowCount, double totalSize)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount)
                .setTotalSize(totalSize)
                .build();
    }
}
