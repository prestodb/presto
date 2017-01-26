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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.StatsRecorder;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.ITERATIVE_OPTIMIZER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.fail;

public class BasePlanTest
{
    protected final LocalQueryRunner queryRunner;

    public BasePlanTest()
    {
        this(ImmutableMap.of());
    }

    public BasePlanTest(Map<String, String> sessionProperties)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty(ITERATIVE_OPTIMIZER, "true")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        sessionProperties.entrySet().forEach(entry -> sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue()));

        this.queryRunner = new LocalQueryRunner(sessionBuilder.build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    protected LocalQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    protected void assertPlan(String sql, PlanMatchPattern pattern)
    {
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, pattern);
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, stage);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    protected void assertPlanWithOptimizers(String sql, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers, LogicalPlanner.Stage.OPTIMIZED);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    protected void assertMinimallyOptimizedPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(new StatsRecorder(), ImmutableSet.of(new RemoveRedundantIdentityProjections())));

        assertPlanWithOptimizers(sql, pattern, optimizers);
    }

    protected Plan plan(String sql)
    {
        return plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED);
    }

    protected Plan plan(String sql, LogicalPlanner.Stage stage)
    {
        return plan(sql, stage, true);
    }

    protected Plan plan(String sql, LogicalPlanner.Stage stage, boolean forceSingleNode)
    {
        try {
            return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, stage, forceSingleNode));
        }
        catch (RuntimeException ex) {
            fail("Invalid SQL: " + sql, ex);
            return null; // make compiler happy
        }
    }
}
