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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class BasePlanTest
{
    private final LocalQueryRunnerSupplier queryRunnerSupplier;
    private LocalQueryRunner queryRunner;

    public BasePlanTest()
    {
        this(ImmutableMap.of());
    }

    public BasePlanTest(Map<String, String> sessionProperties)
    {
        this.queryRunnerSupplier = () -> createQueryRunner(sessionProperties);
    }

    public BasePlanTest(LocalQueryRunnerSupplier supplier)
    {
        this.queryRunnerSupplier = requireNonNull(supplier, "queryRunnerSupplier is null");
    }

    private static LocalQueryRunner createQueryRunner(Map<String, String> sessionProperties)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        sessionProperties.entrySet().forEach(entry -> sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue()));

        LocalQueryRunner queryRunner = new LocalQueryRunner(sessionBuilder.build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        return queryRunner;
    }

    @BeforeClass
    public final void initPlanTest()
            throws Exception
    {
        queryRunner = queryRunnerSupplier.get();
    }

    @AfterClass(alwaysRun = true)
    public final void destroyPlanTest()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    public ConnectorId getCurrentConnectorId()
    {
        return queryRunner.inTransaction(transactionSession -> queryRunner.getMetadata().getCatalogHandle(transactionSession, transactionSession.getCatalog().get())).get();
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
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true);

        assertPlan(sql, stage, pattern, optimizers);
    }

    protected void assertPlan(String sql, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, Predicate<PlanOptimizer> optimizerPredicate)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true).stream()
                .filter(optimizerPredicate)
                .collect(toList());

        assertPlan(sql, stage, pattern, optimizers);
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers, stage);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    protected void assertDistributedPlan(String sql, PlanMatchPattern pattern)
    {
        assertDistributedPlan(sql, getQueryRunner().getDefaultSession(), pattern);
    }

    protected void assertDistributedPlan(String sql, Session session, PlanMatchPattern pattern)
    {
        assertPlanWithSession(sql, session, false, pattern);
    }

    protected void assertMinimallyOptimizedPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        new RuleStatsRecorder(),
                        queryRunner.getStatsCalculator(),
                        queryRunner.getCostCalculator(),
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())));

        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, forceSingleNode);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern, Consumer<Plan> planValidator)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, forceSingleNode);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            planValidator.accept(actualPlan);
            return null;
        });
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
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }

    public interface LocalQueryRunnerSupplier
    {
        LocalQueryRunner get()
                throws Exception;
    }
}
