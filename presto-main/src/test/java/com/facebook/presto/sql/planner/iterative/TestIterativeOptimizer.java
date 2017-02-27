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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.StatsRecorder;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.OPTIMIZER_TIMEOUT;
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
        }
    }

    @Test(timeOut = 1000)
    public void optimizerTimeoutsOnNonConvergingPlan()
    {
        PlanOptimizer optimizer = new IterativeOptimizer(new StatsRecorder(), ImmutableSet.of(new NonConvergingRule()));

        try {
            queryRunner.inTransaction(transactionSession -> {
                queryRunner.createPlan(transactionSession, "SELECT * FROM nation", ImmutableList.of(optimizer));
                fail("The optimizer should not converge");
                return null;
            });
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), OPTIMIZER_TIMEOUT.toErrorCode());
        }
    }
    private static class NonConvergingRule
            implements Rule
    {
        // This rewrite will produce an identity projection node unless one exists.
        // In that case, it will be removed.
        // Thanks to that approach, it never converges and always produces different node.
        @Override
        public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            if (node instanceof ProjectNode) {
                ProjectNode project = (ProjectNode) node;
                if (isIdentityProjection(project)) {
                    return Optional.of(project.getSource());
                }
            }

            PlanNode projectNode = new ProjectNode(idAllocator.getNextId(), node, Assignments.identity(node.getOutputSymbols()));
            return Optional.of(projectNode);
        }

        private static boolean isIdentityProjection(ProjectNode project)
        {
            return ImmutableSet.copyOf(project.getOutputSymbols()).equals(ImmutableSet.copyOf(project.getSource().getOutputSymbols()));
        }
    }
}
