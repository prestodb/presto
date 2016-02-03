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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aliasPair;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestLogicalPlanner
{
    private final LocalQueryRunner queryRunner;

    public TestLogicalPlanner()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(queryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testPlanDsl()
    {
        assertPlan("SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)",
                node(OutputNode.class,
                        node(FilterNode.class,
                                tableScan("orders"))));

        String simpleJoinQuery = "SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey";
        assertPlan(simpleJoinQuery,
                any(
                        any(
                                node(JoinNode.class,
                                        any(
                                                tableScan("orders")),
                                        any(
                                                any(
                                                        tableScan("lineitem")))))));

        assertPlan(simpleJoinQuery,
                anyTree(
                        node(JoinNode.class,
                                anyTree(),
                                anyTree())));

        assertPlan(simpleJoinQuery, anyTree(node(TableScanNode.class)));

        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(ImmutableList.of(aliasPair("X", "Y")),
                                project(
                                        tableScan("orders").withSymbol("orderkey", "X")),
                                project(
                                        node(EnforceSingleRowNode.class,
                                                anyTree(
                                                        tableScan("lineitem").withSymbol("orderkey", "Y")))))));

        assertPlan("SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                anyTree(
                        filter("S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders").withSymbol("orderkey", "X")),
                                                anyTree(
                                                        tableScan("lineitem").withSymbol("orderkey", "Y")))))));

        assertPlan("SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter("NOT S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders").withSymbol("orderkey", "X")),
                                                anyTree(
                                                        tableScan("lineitem").withSymbol("orderkey", "Y")))))));
    }

    private void assertPlan(String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql));
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }
}
