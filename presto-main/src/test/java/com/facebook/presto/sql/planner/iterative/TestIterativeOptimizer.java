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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestIterativeOptimizer
{
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty("iterative_optimizer_enabled", "true")
                .setSystemProperty("iterative_optimizer_timeout", "1ms");

        queryRunner = new LocalQueryRunner(sessionBuilder.build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test(timeOut = 1000)
    public void optimizerTimeoutsOnNonConvergingPlan()
    {
        PlanOptimizer optimizer = new IterativeOptimizer(
                new RuleStatsRecorder(),
                queryRunner.getStatsCalculator(),
                queryRunner.getCostCalculator(),
                ImmutableSet.of(new NonConvergingRule()));

        try {
            queryRunner.inTransaction(transactionSession -> {
                queryRunner.createPlan(transactionSession, "SELECT * FROM nation", ImmutableList.of(optimizer), WarningCollector.NOOP);
                fail("The optimizer should not converge");
                return null;
            });
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), OPTIMIZER_TIMEOUT.toErrorCode());
        }
    }

    private static class NonConvergingRule
            implements Rule<ProjectNode>
    {
        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        // This rewrite will produce an identity projection node unless one exists.
        // In that case, it will be removed.
        // Thanks to that approach, it never converges and always produces different node.
        @Override
        public Result apply(ProjectNode project, Captures captures, Context context)
        {
            if (isIdentityProjection(project)) {
                return Result.ofPlanNode(project.getSource());
            }
            PlanNode projectNode = new ProjectNode(context.getIdAllocator().getNextId(), project, identityAssignmentsAsSymbolReferences(project.getOutputVariables()));
            return Result.ofPlanNode(projectNode);
        }

        private static boolean isIdentityProjection(ProjectNode project)
        {
            return ImmutableSet.copyOf(project.getOutputVariables()).equals(ImmutableSet.copyOf(project.getSource().getOutputVariables()));
        }
    }
}
