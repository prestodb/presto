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
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.SINGLE_NODE_EXECUTION_ENABLED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Plan tests for {@link AddExchangesForSingleNodeExecution}.
 * Verifies that filter predicates are properly pushed into system table scans
 * and that exchanges are correctly placed in single-node execution mode.
 */
public class TestAddExchangesForSingleNodeExecution
        extends BasePlanTest
{
    private Session singleNodeSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SINGLE_NODE_EXECUTION_ENABLED, "true")
                .build();
    }

    private Plan planSingleNode(String query)
    {
        return plan(singleNodeSession(), query, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false);
    }

    @Test
    public void testShowColumnsHasGatherExchange()
    {
        // SHOW COLUMNS is rewritten to a query against information_schema.columns (system table).
        // In single-node mode, system table scans must have a GATHER exchange.
        Plan plan = planSingleNode("SHOW COLUMNS FROM orders");

        assertTrue(hasGatherExchange(plan),
                "Single-node plan for SHOW COLUMNS should contain a GATHER exchange");
    }

    @Test
    public void testDescribeHasGatherExchange()
    {
        // DESCRIBE is an alias for SHOW COLUMNS
        Plan plan = planSingleNode("DESCRIBE orders");

        assertTrue(hasGatherExchange(plan),
                "Single-node plan for DESCRIBE should contain a GATHER exchange");
    }

    @Test
    public void testShowColumnsFilterNotAboveExchange()
    {
        // After the visitFilter fix, the filter predicate (table_name = 'orders') should be
        // pushed into the TableScanNode, so any remaining FilterNode should be BELOW the
        // GATHER exchange, not above it. This verifies the predicate flows through the
        // native worker path (exchange → scan with pushed predicate).
        Plan plan = planSingleNode("SHOW COLUMNS FROM orders");

        boolean hasFilterAboveExchange = searchFrom(plan.getRoot())
                .where(node -> node instanceof FilterNode &&
                        node.getSources().stream().anyMatch(source -> source instanceof ExchangeNode))
                .findFirst()
                .isPresent();

        assertFalse(hasFilterAboveExchange,
                "FilterNode should not appear above the GATHER exchange; predicate should be pushed into the scan below the exchange");
    }

    @Test
    public void testRegularTableScanNoExchangeAdded()
    {
        // For regular (non-system) tables, no remote exchange should be added
        // by AddExchangesForSingleNodeExecution
        Plan plan = planSingleNode("SELECT nationkey, name FROM nation WHERE nationkey > 5");

        boolean hasRemoteExchange = searchFrom(plan.getRoot())
                .where(node -> node instanceof ExchangeNode &&
                        ((ExchangeNode) node).getScope() == REMOTE_STREAMING)
                .findFirst()
                .isPresent();

        assertFalse(hasRemoteExchange,
                "Regular table scan in single-node mode should not have remote exchanges");
    }

    @Test
    public void testRegularFilterNotAffected()
    {
        // A filter on a regular (non-system) table should remain unchanged
        // (visitFilter falls through to default rewrite)
        Plan plan = planSingleNode("SELECT nationkey FROM nation WHERE nationkey > 10");

        boolean hasTableScan = searchFrom(plan.getRoot())
                .where(node -> node instanceof TableScanNode)
                .findFirst()
                .isPresent();

        assertTrue(hasTableScan,
                "Regular table query should still have a TableScanNode");
    }

    private boolean hasGatherExchange(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof ExchangeNode &&
                        ((ExchangeNode) node).getType() == GATHER &&
                        ((ExchangeNode) node).getScope() == REMOTE_STREAMING)
                .findFirst()
                .isPresent();
    }
}
