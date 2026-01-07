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

import com.facebook.presto.Session;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.ExchangeNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.SystemPartitioningHandle;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.SORTED_EXCHANGE_ENABLED;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.spi.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Tests for the SortedExchangeRule optimizer which pushes sort operations
 * down to exchange nodes for distributed queries.
 */
public class TestSortedExchangeRule
        extends BasePlanTest
{
    @Test
    public void testOptimizationDisabled()
    {
        Session disabledSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SORTED_EXCHANGE_ENABLED, "false")
                .build();

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(disabledSession, idAllocator, getQueryRunner().getMetadata());
        VariableReferenceExpression col = builder.variable("col");

        // Create: Sort -> Exchange -> Values
        PlanNode exchange = builder.exchange(ex -> ex
                .addSource(builder.values(col))
                .addInputsSet(col)
                .fixedHashDistributionPartitioningScheme(ImmutableList.of(col), ImmutableList.of(col))
                .scope(REMOTE_STREAMING));

        PlanNode plan = builder.sort(ImmutableList.of(col), exchange);

        SortedExchangeRule rule = new SortedExchangeRule(true);  // true for testing
        VariableAllocator variableAllocator = new VariableAllocator(builder.getTypes().allVariables());
        PlanOptimizerResult result = rule.optimize(
                plan,
                disabledSession,
                builder.getTypes(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP);

        // Plan should remain unchanged - still has SortNode
        assertTrue(result.getPlanNode() instanceof SortNode);
        SortNode sortNode = (SortNode) result.getPlanNode();
        assertTrue(sortNode.getSource() instanceof ExchangeNode);
        ExchangeNode exchangeNode = (ExchangeNode) sortNode.getSource();
        assertFalse(exchangeNode.getOrderingScheme().isPresent());
    }

    @Test
    public void testPushSortToRemoteRepartitionExchange()
    {
        Session enabledSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SORTED_EXCHANGE_ENABLED, "true")
                .build();

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(enabledSession, idAllocator, getQueryRunner().getMetadata());
        VariableReferenceExpression col = builder.variable("col");

        // Create: Sort -> Exchange -> Values
        PlanNode exchange = builder.exchange(ex -> ex
                .addSource(builder.values(col))
                .addInputsSet(col)
                .fixedHashDistributionPartitioningScheme(ImmutableList.of(col), ImmutableList.of(col))
                .scope(REMOTE_STREAMING));

        PlanNode plan = builder.sort(ImmutableList.of(col), exchange);

        SortedExchangeRule rule = new SortedExchangeRule(true);  // true for testing
        VariableAllocator variableAllocator = new VariableAllocator(builder.getTypes().allVariables());
        PlanOptimizerResult result = rule.optimize(
                plan,
                enabledSession,
                builder.getTypes(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP);

        // SortNode should be removed and ordering moved to ExchangeNode
        assertTrue(result.getPlanNode() instanceof ExchangeNode);
        ExchangeNode exchangeNode = (ExchangeNode) result.getPlanNode();
        assertTrue(exchangeNode.getOrderingScheme().isPresent());

        assertEquals(exchangeNode.getOrderingScheme().get().getOrderByVariables().size(), 1);
        assertEquals(exchangeNode.getOrderingScheme().get().getOrderBy().get(0).getSortOrder(), ASC_NULLS_FIRST);
    }

    @Test
    public void testMultipleOrderingColumns()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SORTED_EXCHANGE_ENABLED, "true")
                .build();

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());
        VariableReferenceExpression col1 = builder.variable("col1");
        VariableReferenceExpression col2 = builder.variable("col2");

        // Create: Sort(col1 ASC, col2 DESC) -> Exchange -> Values
        PlanNode exchange = builder.exchange(ex -> ex
                .addSource(builder.values(col1, col2))
                .addInputsSet(col1, col2)
                .fixedHashDistributionPartitioningScheme(ImmutableList.of(col1, col2), ImmutableList.of(col1, col2))
                .scope(REMOTE_STREAMING));

        // Manually create a SortNode with custom ordering
        OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(
                new Ordering(col1, ASC_NULLS_FIRST),
                new Ordering(col2, DESC_NULLS_LAST)));
        PlanNode plan = new SortNode(
                Optional.empty(),
                idAllocator.getNextId(),
                exchange,
                orderingScheme,
                false,
                ImmutableList.of());

        SortedExchangeRule rule = new SortedExchangeRule(true);  // true for testing
        VariableAllocator variableAllocator = new VariableAllocator(builder.getTypes().allVariables());
        PlanOptimizerResult result = rule.optimize(
                plan,
                session,
                builder.getTypes(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP);

        // Verify ordering moved to exchange
        assertTrue(result.getPlanNode() instanceof ExchangeNode);
        ExchangeNode exchangeNode = (ExchangeNode) result.getPlanNode();
        assertTrue(exchangeNode.getOrderingScheme().isPresent());

        assertEquals(exchangeNode.getOrderingScheme().get().getOrderByVariables().size(), 2);
        assertEquals(exchangeNode.getOrderingScheme().get().getOrderBy().get(0).getSortOrder(), ASC_NULLS_FIRST);
        assertEquals(exchangeNode.getOrderingScheme().get().getOrderBy().get(1).getSortOrder(), DESC_NULLS_LAST);
    }

    @Test
    public void testDoesNotOptimizeNonRemoteExchange()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SORTED_EXCHANGE_ENABLED, "true")
                .build();

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());
        VariableReferenceExpression col = builder.variable("col");

        // Create: Sort -> LOCAL Exchange -> Values (not REMOTE)
        PlanNode exchange = builder.exchange(ex -> ex
                .addSource(builder.values(col))
                .addInputsSet(col)
                .fixedHashDistributionPartitioningScheme(ImmutableList.of(col), ImmutableList.of(col))
                .scope(ExchangeNode.Scope.LOCAL));

        PlanNode plan = builder.sort(ImmutableList.of(col), exchange);

        SortedExchangeRule rule = new SortedExchangeRule(true);  // true for testing
        VariableAllocator variableAllocator = new VariableAllocator(builder.getTypes().allVariables());
        PlanOptimizerResult result = rule.optimize(
                plan,
                session,
                builder.getTypes(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP);

        // Plan should remain unchanged since exchange is not remote
        assertTrue(result.getPlanNode() instanceof SortNode);
    }

    @Test
    public void testDoesNotOptimizeReplicatedExchange()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SORTED_EXCHANGE_ENABLED, "true")
                .build();

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());
        VariableReferenceExpression col = builder.variable("col");

        // Create: Sort -> REPLICATE Exchange -> Values
        PlanNode exchange = SystemPartitioningHandle.replicatedExchange(
                idAllocator.getNextId(),
                ExchangeNode.Scope.REMOTE_STREAMING,
                builder.values(col));

        PlanNode plan = builder.sort(ImmutableList.of(col), exchange);

        SortedExchangeRule rule = new SortedExchangeRule(true);  // true for testing
        VariableAllocator variableAllocator = new VariableAllocator(builder.getTypes().allVariables());
        PlanOptimizerResult result = rule.optimize(
                plan,
                session,
                builder.getTypes(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP);

        // Plan should remain unchanged since exchange is REPLICATE type
        assertTrue(result.getPlanNode() instanceof SortNode);
        SortNode sortNode = (SortNode) result.getPlanNode();
        assertTrue(sortNode.getSource() instanceof ExchangeNode);
        ExchangeNode exchangeNode = (ExchangeNode) sortNode.getSource();
        assertEquals(exchangeNode.getType(), ExchangeNode.Type.REPLICATE);
    }

    @Test
    public void testNestedSortExchangePattern()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SORTED_EXCHANGE_ENABLED, "true")
                .build();

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());
        VariableReferenceExpression col1 = builder.variable("col1");
        VariableReferenceExpression col2 = builder.variable("col2");

        // Create nested pattern: Sort -> Project -> Exchange -> Project -> Sort -> Exchange -> Values
        // Bottom layer: Exchange2 -> Values
        PlanNode values = builder.values(col1, col2);
        PlanNode exchange2 = builder.exchange(ex -> ex
                .addSource(values)
                .addInputsSet(col1, col2)
                .fixedHashDistributionPartitioningScheme(ImmutableList.of(col1, col2), ImmutableList.of(col1, col2))
                .scope(REMOTE_STREAMING));

        // Second layer: Sort2 on exchange2
        PlanNode sort2 = builder.sort(ImmutableList.of(col1), exchange2);

        // Third layer: Project on sort2 (some other nodes)
        PlanNode project1 = builder.project(builder.assignment(col1, col1, col2, col2), sort2);

        // Fourth layer: Exchange1 on project1
        PlanNode exchange1 = builder.exchange(ex -> ex
                .addSource(project1)
                .addInputsSet(col1, col2)
                .fixedHashDistributionPartitioningScheme(ImmutableList.of(col2, col1), ImmutableList.of(col1, col2))
                .scope(REMOTE_STREAMING));

        // Fifth layer: Project on exchange1 (some other nodes)
        PlanNode project2 = builder.project(builder.assignment(col1, col1, col2, col2), exchange1);

        // Top layer: Sort1 on project2
        PlanNode plan = builder.sort(ImmutableList.of(col2), project2);

        // Run the optimizer
        // IMPORTANT: Only Sort2 can be optimized (its immediate child is Exchange2)
        // Sort1 CANNOT be optimized (its immediate child is Project, not Exchange1)
        SortedExchangeRule rule = new SortedExchangeRule(true);  // true for testing
        VariableAllocator variableAllocator = new VariableAllocator(builder.getTypes().allVariables());

        PlanOptimizerResult result = rule.optimize(
                plan,
                session,
                builder.getTypes(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP);

        // Expected result after optimization:
        // Sort1 -> Project -> Exchange1 (NO ordering) -> Project -> Exchange2 (WITH ordering) -> Values
        //
        // Sort1 remains because its immediate child is Project, not Exchange1
        // Sort2 is removed and its ordering is pushed to Exchange2

        PlanNode finalPlan = result.getPlanNode();

        // The root SHOULD still be a SortNode (Sort1 cannot be optimized)
        assertTrue(finalPlan instanceof SortNode, "Top-level node should still be SortNode because its immediate child is Project, not Exchange");
        SortNode sort1 = (SortNode) finalPlan;
        assertEquals(sort1.getOrderingScheme().getOrderByVariables().get(0), col2);

        // Navigate through to find exchanges
        boolean foundExchange1 = false;
        boolean foundExchange2WithOrdering = false;

        PlanNode current = finalPlan.getSources().get(0); // Skip Sort1, start at Project
        while (current != null) {
            if (current instanceof ExchangeNode) {
                ExchangeNode ex = (ExchangeNode) current;
                if (!foundExchange1) {
                    // First exchange we encounter should be Exchange1 (should NOT have ordering)
                    foundExchange1 = true;
                    assertFalse(ex.getOrderingScheme().isPresent(),
                            "Exchange1 should NOT have ordering (Sort1's immediate child was Project, not Exchange1)");
                }
                else if (!foundExchange2WithOrdering && ex.getOrderingScheme().isPresent()) {
                    // Second exchange with ordering should be Exchange2
                    foundExchange2WithOrdering = true;
                    assertEquals(ex.getOrderingScheme().get().getOrderByVariables().size(), 1);
                    assertEquals(ex.getOrderingScheme().get().getOrderByVariables().get(0), col1,
                            "Exchange2 should have ordering from Sort2 on col1");
                }
            }

            // Move to next node
            if (current.getSources().isEmpty()) {
                break;
            }
            current = current.getSources().get(0);
        }

        assertTrue(foundExchange1, "Should have found Exchange1");
        assertTrue(foundExchange2WithOrdering, "Exchange2 should have ordering (Sort2's immediate child was Exchange2)");
    }

    @Test
    public void testDoesNotOptimizeWhenOrderingVariablesNotInSourceOutput()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SORTED_EXCHANGE_ENABLED, "true")
                .build();

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());

        // Source produces: [a, b]
        VariableReferenceExpression varA = builder.variable("a");
        VariableReferenceExpression varB = builder.variable("b");
        PlanNode source = builder.values(varA, varB);

        // Exchange outputs: [x, y] (different variables!)
        VariableReferenceExpression varX = builder.variable("x");
        VariableReferenceExpression varY = builder.variable("y");
        PlanNode exchange = builder.exchange(ex -> ex
                .addSource(source)
                .addInputsSet(varA, varB)  // Source produces [a, b]
                .fixedHashDistributionPartitioningScheme(ImmutableList.of(varX, varY), ImmutableList.of(varX, varY))  // Exchange outputs [x, y]
                .scope(REMOTE_STREAMING));

        // Try to sort by varX, which is NOT in the source output [a, b]
        // This should NOT be optimized because the ordering variable doesn't exist in source
        PlanNode plan = builder.sort(ImmutableList.of(varX), exchange);

        SortedExchangeRule rule = new SortedExchangeRule(true);  // true for testing
        VariableAllocator variableAllocator = new VariableAllocator(builder.getTypes().allVariables());
        PlanOptimizerResult result = rule.optimize(
                plan,
                session,
                builder.getTypes(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP);

        // The sort should NOT be pushed down because varX is not in source output [a, b]
        assertTrue(result.getPlanNode() instanceof SortNode, "Sort should remain because ordering variable not in source output");
        SortNode sortNode = (SortNode) result.getPlanNode();
        assertTrue(sortNode.getSource() instanceof ExchangeNode, "Sort's child should still be Exchange");
        ExchangeNode exchangeNode = (ExchangeNode) sortNode.getSource();
        assertFalse(exchangeNode.getOrderingScheme().isPresent(), "Exchange should NOT have ordering scheme");
    }
}
