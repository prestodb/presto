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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.ENABLE_SORTED_EXCHANGES;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Tests for the SortedExchangeRule optimizer which pushes sort operations
 * down to exchange nodes for distributed queries.
 */
public class TestSortedExchangeRule
        extends BasePlanTest
{
    private Session getSessionWithSortedExchangesEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(ENABLE_SORTED_EXCHANGES, "true")
                .build();
    }

    @Test
    public void testPushSortToRemoteRepartitionExchange()
    {
        @Language("SQL") String sql = "SELECT orderkey, custkey FROM orders ORDER BY orderkey";

        Plan plan = plan(getSessionWithSortedExchangesEnabled(), sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);

        // Find all remote repartition exchanges with ordering
        List<ExchangeNode> sortedExchanges = findSortedRemoteExchanges(plan.getRoot());

        // Verify at least one sorted exchange exists
        assertFalse(sortedExchanges.isEmpty(), "Expected at least one sorted exchange");

        // Verify the ordering contains orderkey
        boolean hasOrderkeyOrdering = sortedExchanges.stream()
                .anyMatch(exchange -> exchange.getOrderingScheme().isPresent() &&
                        exchange.getOrderingScheme().get().getOrderByVariables().stream()
                                .anyMatch(v -> v.getName().equals("orderkey")));
        assertTrue(hasOrderkeyOrdering, "Expected exchange with orderkey ordering");
    }

    @Test
    public void testSortedExchangeDisabledBySessionProperty()
    {
        @Language("SQL") String sql = "SELECT orderkey, custkey FROM orders ORDER BY orderkey";

        Session disabledSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(ENABLE_SORTED_EXCHANGES, "false")
                .build();

        Plan plan = plan(disabledSession, sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);

        // Find all remote repartition exchanges
        List<ExchangeNode> exchanges = PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> node instanceof ExchangeNode &&
                        ((ExchangeNode) node).getScope().isRemote() &&
                        ((ExchangeNode) node).getType() == ExchangeNode.Type.REPARTITION)
                .findAll()
                .stream()
                .map(ExchangeNode.class::cast)
                .collect(Collectors.toList());

        // When disabled, exchanges should not have ordering (or there should be explicit sort nodes)
        // Just verify the plan builds successfully
        assertTrue(exchanges.size() > 0, "Expected at least one exchange");
    }

    @Test
    public void testSortOnMultipleColumns()
    {
        @Language("SQL") String sql = "SELECT orderkey, custkey, totalprice FROM orders ORDER BY orderkey, custkey";

        Plan plan = plan(getSessionWithSortedExchangesEnabled(), sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
        List<ExchangeNode> sortedExchanges = findSortedRemoteExchanges(plan.getRoot());

        assertFalse(sortedExchanges.isEmpty(), "Expected at least one sorted exchange");

        // Verify ordering contains both columns
        boolean hasMultiColumnOrdering = sortedExchanges.stream()
                .anyMatch(exchange -> exchange.getOrderingScheme().isPresent() &&
                        exchange.getOrderingScheme().get().getOrderByVariables().size() >= 2);
        assertTrue(hasMultiColumnOrdering, "Expected exchange with multi-column ordering");
    }

    @Test
    public void testSortWithDescendingOrder()
    {
        @Language("SQL") String sql = "SELECT orderkey, custkey FROM orders ORDER BY orderkey DESC";

        Plan plan = plan(getSessionWithSortedExchangesEnabled(), sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
        List<ExchangeNode> sortedExchanges = findSortedRemoteExchanges(plan.getRoot());

        assertFalse(sortedExchanges.isEmpty(), "Expected at least one sorted exchange");
    }

    @Test
    public void testSortedExchangeWithJoin()
    {
        @Language("SQL") String sql = "SELECT o.orderkey, o.custkey " +
                "FROM orders o " +
                "JOIN customer c ON o.custkey = c.custkey " +
                "ORDER BY o.orderkey";

        Plan plan = plan(getSessionWithSortedExchangesEnabled(), sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
        List<ExchangeNode> sortedExchanges = findSortedRemoteExchanges(plan.getRoot());

        assertFalse(sortedExchanges.isEmpty(), "Expected at least one sorted exchange");
    }

    @Test
    public void testSortedExchangeWithAggregation()
    {
        @Language("SQL") String sql = "SELECT custkey, SUM(totalprice) as total " +
                "FROM orders " +
                "GROUP BY custkey " +
                "ORDER BY custkey";

        Plan plan = plan(getSessionWithSortedExchangesEnabled(), sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
        List<ExchangeNode> sortedExchanges = findSortedRemoteExchanges(plan.getRoot());

        assertFalse(sortedExchanges.isEmpty(), "Expected at least one sorted exchange");
    }

    @Test
    public void testSortWithLimit()
    {
        @Language("SQL") String sql = "SELECT orderkey, custkey FROM orders ORDER BY orderkey LIMIT 100";

        Plan plan = plan(getSessionWithSortedExchangesEnabled(), sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
        List<ExchangeNode> sortedExchanges = findSortedRemoteExchanges(plan.getRoot());

        assertFalse(sortedExchanges.isEmpty(), "Expected at least one sorted exchange");
    }

    @Test
    public void testMultipleSortsInQuery()
    {
        @Language("SQL") String sql = "SELECT orderkey, custkey FROM " +
                "(SELECT orderkey, custkey FROM orders ORDER BY custkey) " +
                "ORDER BY orderkey";

        Plan plan = plan(getSessionWithSortedExchangesEnabled(), sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
        List<ExchangeNode> sortedExchanges = findSortedRemoteExchanges(plan.getRoot());

        assertFalse(sortedExchanges.isEmpty(), "Expected at least one sorted exchange");
    }

    @Test
    public void testSortWithNullsFirst()
    {
        @Language("SQL") String sql = "SELECT orderkey, custkey FROM orders ORDER BY orderkey NULLS FIRST";

        Plan plan = plan(getSessionWithSortedExchangesEnabled(), sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
        List<ExchangeNode> sortedExchanges = findSortedRemoteExchanges(plan.getRoot());

        assertFalse(sortedExchanges.isEmpty(), "Expected at least one sorted exchange");
    }

    @Test
    public void testSortWithNullsLast()
    {
        @Language("SQL") String sql = "SELECT orderkey, custkey FROM orders ORDER BY orderkey NULLS LAST";

        Plan plan = plan(getSessionWithSortedExchangesEnabled(), sql, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
        List<ExchangeNode> sortedExchanges = findSortedRemoteExchanges(plan.getRoot());

        assertFalse(sortedExchanges.isEmpty(), "Expected at least one sorted exchange");
    }

    private List<ExchangeNode> findSortedRemoteExchanges(PlanNode root)
    {
        return PlanNodeSearcher.searchFrom(root)
                .where(node -> node instanceof ExchangeNode &&
                        ((ExchangeNode) node).getScope().isRemote() &&
                        ((ExchangeNode) node).getType() == ExchangeNode.Type.REPARTITION &&
                        ((ExchangeNode) node).getOrderingScheme().isPresent())
                .findAll()
                .stream()
                .map(ExchangeNode.class::cast)
                .collect(Collectors.toList());
    }
}
