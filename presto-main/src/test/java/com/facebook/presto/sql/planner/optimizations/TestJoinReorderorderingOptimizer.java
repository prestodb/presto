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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aliasPair;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestJoinReorderorderingOptimizer
{
    private final LocalQueryRunner queryRunner;
    private final Session reorderingEnabledSession;
    private final Session reorderingDisabledSession;

    public TestJoinReorderorderingOptimizer()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        reorderingEnabledSession = queryRunner.getDefaultSession().withSystemProperty(SystemSessionProperties.JOIN_REORDERING, "true");
        reorderingDisabledSession = queryRunner.getDefaultSession().withSystemProperty(SystemSessionProperties.JOIN_REORDERING, "false");

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(queryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testNoReorderNeeded()
    {
        @Language("SQL") String sql = "select * from nation join region on nation.regionkey = region.regionkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(aliasPair("X", "Y")),
                                anyTree(tableScan("nation")),
                                anyTree(tableScan("region"))));

        assertPlan(reorderingEnabledSession, sql, pattern);
    }

    @Test
    public void testReorderNeeded()
    {
        @Language("SQL") String sql = "select * from region join nation on nation.regionkey = region.regionkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(aliasPair("X", "Y")),
                                anyTree(tableScan("nation")),
                                anyTree(tableScan("region"))));

        assertPlan(reorderingEnabledSession, sql, pattern);
    }

    @Test
    public void testReorderNeededButDisabled()
    {
        @Language("SQL") String sql = "select * from region join nation on nation.regionkey = region.regionkey";

        PlanMatchPattern pattern =
                anyTree(
                        join(JoinNode.Type.INNER,
                                ImmutableList.of(aliasPair("X", "Y")),
                                anyTree(tableScan("region")),
                                anyTree(tableScan("nation"))));

        assertPlan(reorderingDisabledSession, sql, pattern);
    }

    private void assertPlan(Session session, @Language("SQL") String sql, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }
}
