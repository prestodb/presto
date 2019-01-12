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
package io.prestosql.sql.planner.optimizations;

import io.prestosql.Session;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import org.testng.annotations.Test;

import static io.prestosql.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.output;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestForceSingleNodeOutput
        extends BasePlanTest
{
    @Test
    public void testSimpleScan()
    {
        // don't force gather
        assertPlanWithSession("SELECT * FROM orders", singleNodeOutput(false), false,
                output(
                        tableScan("orders")));
        // force gather
        assertPlanWithSession("SELECT * FROM orders", singleNodeOutput(true), false,
                output(
                        exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.GATHER,
                                tableScan("orders"))));
    }

    @Test
    public void testGroupBy()
    {
        // don't force gather
        assertPlanWithSession("SELECT orderkey, count(*) FROM orders GROUP BY orderkey", singleNodeOutput(false), false,
                output(
                        node(AggregationNode.class,
                                tableScan("orders"))));
        // force gather
        assertPlanWithSession("SELECT orderkey, count(*) FROM orders GROUP BY orderkey", singleNodeOutput(true), false,
                output(
                        exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.GATHER,
                                node(AggregationNode.class,
                                        tableScan("orders")))));
    }

    @Test
    public void testOrderBy()
    {
        // don't force gather
        assertPlanWithSession("SELECT orderkey FROM orders ORDER BY orderkey", singleNodeOutput(false), false,
                output(
                        exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.GATHER,
                                anyTree(
                                        tableScan("orders")))));
        // force gather, same result
        assertPlanWithSession("SELECT orderkey FROM orders ORDER BY orderkey", singleNodeOutput(true), false,
                output(
                        exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.GATHER,
                                anyTree(
                                        tableScan("orders")))));
    }

    private Session singleNodeOutput(boolean force)
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.toString(force))
                .build();
    }
}
