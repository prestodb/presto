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
package com.facebook.presto.sql.planner.iterative.connector;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.relation.TableExpression;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class TestPlanNodeTableExpressionRoundTrip
        extends BasePlanTest
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
                .setSystemProperty("iterative_optimizer_timeout", "100s");

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

    protected <T extends PlanNode> void assertRoundTrip(Class<T> planNodeType, @Language("SQL") String sql, BiConsumer<T, T> equalAssertion)
    {
        assertRoundTrip(planNodeType, sql, equalAssertion, ImmutableList.of());
    }

    protected <T extends PlanNode> void assertRoundTrip(Class<T> planNodeType, @Language("SQL") String sql, BiConsumer<T, T> equalAssertion, List<PlanOptimizer> perquisiteOptimizers)
    {
        PlanOptimizer optimizer = new IterativeOptimizer(
                new RuleStatsRecorder(),
                queryRunner.getStatsCalculator(),
                queryRunner.getCostCalculator(),
                ImmutableSet.of(new TranslateRule<>(planNodeType, queryRunner, equalAssertion)));

        queryRunner.inTransaction(transactionSession -> {
            queryRunner.createPlan(
                    transactionSession,
                    sql,
                    new ImmutableList.Builder()
                            .addAll(perquisiteOptimizers)
                            .add(optimizer)
                            .build(),
                    WarningCollector.NOOP);
            return null;
        });
    }

    protected <T extends PlanNode> void assertRoundTripFail(Class<T> planNodeType, @Language("SQL") String sql, BiConsumer<T, T> equalAssertion, List<PlanOptimizer> perquisiteOptimizers)
    {
        PlanOptimizer optimizer = new IterativeOptimizer(
                new RuleStatsRecorder(),
                queryRunner.getStatsCalculator(),
                queryRunner.getCostCalculator(),
                ImmutableSet.of(new TranslateRule<>(planNodeType, queryRunner, equalAssertion)));

        try {
            queryRunner.inTransaction(transactionSession -> {
                queryRunner.createPlan(
                        transactionSession,
                        sql,
                        new ImmutableList.Builder()
                                .addAll(perquisiteOptimizers)
                                .add(optimizer)
                                .build(),
                        WarningCollector.NOOP);
                fail("expected to fail");
                return null;
            });
        }
        catch (AssertionError e) {
            if (e.getMessage().startsWith("expected to fail")) {
                throw e;
            }
        }
    }

    private static class TranslateRule<T extends PlanNode>
            implements Rule<T>
    {
        private final AtomicInteger count = new AtomicInteger(0);
        private final Class<T> type;
        private final LocalQueryRunner queryRunner;
        private final BiConsumer<T, T> nodeEqualAssertion;

        public TranslateRule(Class<T> type, LocalQueryRunner queryRunner, BiConsumer<T, T> nodeEqualAssertion)
        {
            this.type = requireNonNull(type, "type is null");
            this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
            this.nodeEqualAssertion = requireNonNull(nodeEqualAssertion, "nodeEqualAssertion is null");
        }

        @Override
        public Pattern<T> getPattern()
        {
            return Pattern.typeOf(type);
        }

        @Override
        public Result apply(T node, Captures captures, Context context)
        {
            if (count.getAndIncrement() == 0) {
                PlanNodeToTableExpressionTranslator planNodeToTableExpressionTranslator = new PlanNodeToTableExpressionTranslator(
                        queryRunner.getMetadata(),
                        context.getSymbolAllocator().getTypes(),
                        context.getSession(),
                        queryRunner.getSqlParser(),
                        context.getLookup());
                Optional<TableExpression> tableExpression = planNodeToTableExpressionTranslator.translate(node);
                TableExpressionToPlanNodeTranslator tableExpressionToPlanNodeTranslator = new TableExpressionToPlanNodeTranslator(
                        context.getIdAllocator(),
                        context.getSymbolAllocator(),
                        new LiteralEncoder(queryRunner.getMetadata().getBlockEncodingSerde()),
                        queryRunner.getMetadata());

                assertTrue(tableExpression.isPresent(), "planNode cannot be represented by table expression");
                Optional<PlanNode> rewrittenPlanNode = tableExpressionToPlanNodeTranslator.translate(
                        context.getSession(),
                        new ConnectorId("local"),
                        tableExpression.get(), node.getOutputSymbols());
                assertTrue(rewrittenPlanNode.isPresent(), "rewritten planNode is empty");
                assertTrue(type.isAssignableFrom(rewrittenPlanNode.get().getClass()), "PlanNode rewrite into different types");
                nodeEqualAssertion.accept((T) rewrittenPlanNode.get(), node);
                Optional<TableExpression> rewrittenTableExpression = planNodeToTableExpressionTranslator.translate(rewrittenPlanNode.get());
                assertTrue(rewrittenTableExpression.isPresent(), "rewritten planNode cannot be convert back to table expression");
                assertEquals(rewrittenTableExpression.get(), tableExpression.get());
            }
            return Result.empty();
        }
    }
}
