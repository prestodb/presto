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
package io.prestosql.sql.planner.iterative;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.planner.RuleStatsRecorder;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
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
            PlanNode projectNode = new ProjectNode(context.getIdAllocator().getNextId(), project, Assignments.identity(project.getOutputSymbols()));
            return Result.ofPlanNode(projectNode);
        }

        private static boolean isIdentityProjection(ProjectNode project)
        {
            return ImmutableSet.copyOf(project.getOutputSymbols()).equals(ImmutableSet.copyOf(project.getSource().getOutputSymbols()));
        }
    }
}
