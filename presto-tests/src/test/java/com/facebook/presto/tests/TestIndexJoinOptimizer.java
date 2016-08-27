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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.tpch.IndexedTpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.AbstractTestIndexedQueries.INDEX_SPEC;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

/**
 * Tests related to IndexJoinOptimizer.
 * Note, that this class is not in presto-main because of dependency on TpchIndex
 * which is needed for index join optimization to work.
 */
public class TestIndexJoinOptimizer
{
    private final LocalQueryRunner queryRunner;

    public TestIndexJoinOptimizer()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new IndexedTpchConnectorFactory(queryRunner.getNodeManager(), INDEX_SPEC, 1),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testIndexJoinOptimization()
    {
        String simpleJoinQuery = "SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey";
        assertPlan(simpleJoinQuery,
                anyTree(
                        node(IndexJoinNode.class,
                                anyTree(),
                                anyTree())));
    }

    @Test
    public void testIndexJoinOptimizationDisabled()
    {
        Session modifiedSession = queryRunner.getDefaultSession().withSystemProperty(SystemSessionProperties.INDEX_JOIN, "false");
        String simpleJoinQuery = "SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey";
        assertPlan(modifiedSession, simpleJoinQuery,
                anyTree(
                        node(JoinNode.class,
                                anyTree(),
                                anyTree())));
    }

    private void assertPlan(String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = plan(sql);
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private void assertPlan(Session session, String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = plan(session, sql);
        queryRunner.inTransaction(session, transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private Plan plan(String sql)
    {
        return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql));
    }

    private Plan plan(Session session, String sql)
    {
        return queryRunner.inTransaction(session, transactionSession -> queryRunner.createPlan(transactionSession, sql));
    }
}
