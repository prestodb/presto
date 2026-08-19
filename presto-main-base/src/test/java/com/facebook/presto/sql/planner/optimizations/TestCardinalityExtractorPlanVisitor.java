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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.plan.AggregationNode.GroupingSetDescriptor;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCardinalityExtractorPlanVisitor
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testLimitOnTopOfValues()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);

        assertEquals(
                extractCardinality(planBuilder.limit(3, planBuilder.values(emptyList(), ImmutableList.of(emptyList())))),
                Range.singleton(1L));

        assertEquals(
                extractCardinality(planBuilder.limit(3, planBuilder.values(emptyList(), ImmutableList.of(emptyList(), emptyList(), emptyList(), emptyList())))),
                Range.singleton(3L));
    }

    @Test
    public void testGlobalAggregation()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        assertEquals(
                extractCardinality(planBuilder.aggregation(aggregationBuilder -> aggregationBuilder
                        .addAggregation(planBuilder.variable("count", BIGINT), planBuilder.rowExpression("count()"))
                        .globalGrouping()
                        .source(planBuilder.values(planBuilder.variable("x", BIGINT), planBuilder.variable("y", BIGINT), planBuilder.variable("z", BIGINT))))),
                Range.singleton(1L));
    }

    @Test
    public void testSimpleGroupedAggregation()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        assertEquals(
                extractCardinality(planBuilder.aggregation(aggregationBuilder -> aggregationBuilder
                        .addAggregation(planBuilder.variable("count", BIGINT), planBuilder.rowExpression("count()"))
                        .singleGroupingSet(planBuilder.variable("y", BIGINT), planBuilder.variable("z", BIGINT))
                        .source(planBuilder.values(planBuilder.variable("x", BIGINT), planBuilder.variable("y", BIGINT), planBuilder.variable("z", BIGINT))))),
                Range.atLeast(0L));
    }

    @Test
    public void testMultipleGlobalGroupingSets()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        assertEquals(
                extractCardinality(planBuilder.aggregation(aggregationBuilder -> aggregationBuilder
                        .addAggregation(planBuilder.variable("count", BIGINT), planBuilder.rowExpression("count()"))
                        .groupingSets(new GroupingSetDescriptor(ImmutableList.of(), 2, ImmutableSet.of(0, 1)))
                        .source(planBuilder.values(planBuilder.variable("x", BIGINT), planBuilder.variable("y", BIGINT), planBuilder.variable("z", BIGINT))))),
                Range.singleton(2L));
    }

    /**
     * Pins the trap that makes a structurally-expressed zero untestable, so no future test
     * repeats the mistake.
     * <p>
     * Cardinality extraction is <b>structural</b> and never consults a {@code StatsProvider}. A 0-row
     * {@code ValuesNode} therefore has cardinality {@code [0, 0]}, which makes
     * {@code isAtMostScalar} true, which makes {@code DetermineJoinDistributionType.mustReplicate} true,
     * which -- combined with {@code mustPartition} being true for RIGHT -- silently excludes the flipped
     * candidate. A {@code TableScanNode}, {@code FilterNode} or {@code JoinNode} with a 0-row
     * <i>estimate</i> provably cannot trigger it: there is no {@code visitTableScan} or
     * {@code visitJoin} override, so they fall to {@code visitPlan} → {@code [0, infinity)}.
     * <p>
     * Consequence for anyone writing a test about empty inputs: express emptiness through statistics,
     * never through a 0-row {@code ValuesNode}. Note {@code visitFilter}/{@code visitProject} inherit the
     * source's bound, so wrapping a 0-row {@code ValuesNode} does not launder it either.
     */
    @Test
    public void testZeroRowValuesIsScalarButZeroEstimateNodesAreNot()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        VariableReferenceExpression x = planBuilder.variable("x", BIGINT);

        // A 0-row ValuesNode: structurally scalar-or-less. This is the trap.
        ValuesNode emptyValues = planBuilder.values(x);
        assertEquals(extractCardinality(emptyValues), Range.singleton(0L));
        assertTrue(isAtMostScalar(emptyValues));

        // ... and wrapping it does not remove the bound, because visitFilter/visitProject inherit it.
        assertTrue(isAtMostScalar(planBuilder.filter(TRUE_CONSTANT, emptyValues)));
        assertTrue(isAtMostScalar(planBuilder.project(identityAssignments(x), emptyValues)));

        // >= 2 structural rows: not scalar, whatever the statistics say. This is how a test must model
        // an empty scan.
        ValuesNode twoRows = planBuilder.values(2, x);
        assertEquals(extractCardinality(twoRows), Range.singleton(2L));
        assertFalse(isAtMostScalar(twoRows));

        // A real TableScanNode has no override, so it falls to visitPlan regardless of its estimate.
        TableScanNode tableScan = planBuilder.tableScan(ImmutableList.of(x), ImmutableMap.of(x, new TestingColumnHandle("x")));
        assertEquals(extractCardinality(tableScan), Range.atLeast(0L));
        assertFalse(isAtMostScalar(tableScan));
        assertFalse(isAtMostScalar(planBuilder.filter(TRUE_CONSTANT, tableScan)));

        // As does a JoinNode -- the shape of the real probe-side subtree.
        VariableReferenceExpression y = planBuilder.variable("y", BIGINT);
        TableScanNode otherScan = planBuilder.tableScan(ImmutableList.of(y), ImmutableMap.of(y, new TestingColumnHandle("y")));
        JoinNode join = planBuilder.join(INNER, tableScan, otherScan, new EquiJoinClause(x, y));
        assertEquals(extractCardinality(join), Range.atLeast(0L));
        assertFalse(isAtMostScalar(join));
    }

    @Test
    public void testEmptyAndNonEmptyGroupingSets()
    {
        PlanBuilder planBuilder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
        assertEquals(
                extractCardinality(planBuilder.aggregation(aggregationBuilder -> aggregationBuilder
                        .addAggregation(planBuilder.variable("count", BIGINT), planBuilder.rowExpression("count()"))
                        .groupingSets(new GroupingSetDescriptor(ImmutableList.of(planBuilder.variable("y", BIGINT)), 2, ImmutableSet.of(0)))
                        .source(planBuilder.values(planBuilder.variable("x", BIGINT), planBuilder.variable("y", BIGINT), planBuilder.variable("z", BIGINT))))),
                Range.atLeast(1L));
    }
}
