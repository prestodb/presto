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
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestForceSingleNodeOutput
        extends BasePlanTest
{
    private Session forceSingleNodeSession;

    public TestForceSingleNodeOutput()
    {
        super();
    }

    @BeforeClass
    public void setup()
    {
        this.forceSingleNodeSession = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(FORCE_SINGLE_NODE_OUTPUT, "true")
                .build();
    }

    @Test
    public void testSimpleScan()
    {
        // don't force gather
        assertPlanWithSession("SELECT * FROM orders", getQueryRunner().getDefaultSession(), false,
                output(
                        tableScan("orders")));
        // force gather
        assertPlanWithSession("SELECT * FROM orders", forceSingleNodeSession, false,
                output(
                        exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.GATHER,
                                tableScan("orders"))));
    }

    @Test
    public void testGroupBy()
    {
        // don't force gather
        assertPlanWithSession("SELECT orderkey, count(*) FROM orders GROUP BY orderkey", getQueryRunner().getDefaultSession(), false,
                output(
                        node(AggregationNode.class,
                                tableScan("orders"))));
        // force gather
        assertPlanWithSession("SELECT orderkey, count(*) FROM orders GROUP BY orderkey", forceSingleNodeSession, false,
                output(
                        exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.GATHER,
                                node(AggregationNode.class,
                                        tableScan("orders")))));
    }

    @Test
    public void testOrderBy()
    {
        // don't force gather
        assertPlanWithSession("SELECT orderkey FROM orders ORDER BY orderkey", getQueryRunner().getDefaultSession(), false,
                output(
                        anyTree(
                                exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.GATHER,
                                        tableScan("orders")))));
        // force gather, same result
        assertPlanWithSession("SELECT orderkey FROM orders ORDER BY orderkey", forceSingleNodeSession, false,
                output(
                        anyTree(
                                exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.GATHER,
                                        tableScan("orders")))));
    }
}
