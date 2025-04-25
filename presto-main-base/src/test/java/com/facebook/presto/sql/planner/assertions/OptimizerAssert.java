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
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.expressions.JsonCodecRowExpressionSerde;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyRowExpressions;
import com.facebook.presto.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin;
import com.facebook.presto.sql.planner.iterative.rule.TransformUncorrelatedInPredicateSubqueryToSemiJoin;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert.TestingStatsCalculator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.sql.planner.assertions.PlanAssert.assertPlan;
import static com.facebook.presto.sql.planner.assertions.PlanAssert.assertPlanDoesNotMatch;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.fail;

public class OptimizerAssert
{
    private final Metadata metadata;
    private final TestingStatsCalculator statsCalculator;
    private final PlanOptimizer optimizer;
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final LocalQueryRunner queryRunner;

    private Session session;
    private TypeProvider types;
    private PlanNode plan;

    public OptimizerAssert(Metadata metadata, LocalQueryRunner queryRunner, StatsCalculator statsCalculator, Session session, PlanOptimizer optimizer, TransactionManager transactionManager, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statsCalculator = new TestingStatsCalculator(requireNonNull(statsCalculator, "statsCalculator is null"));
        this.session = requireNonNull(session, "session is null");
        this.optimizer = requireNonNull(optimizer, "optimizer is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "access control is null");
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
    }

    public OptimizerAssert setSystemProperty(String key, String value)
    {
        return withSession(Session.builder(session)
                .setSystemProperty(key, value)
                .build());
    }

    public OptimizerAssert withSession(Session session)
    {
        this.session = session;
        return this;
    }

    public OptimizerAssert on(Function<PlanBuilder, PlanNode> planProvider)
    {
        checkState(plan == null, "plan has already been set");

        PlanBuilder builder = new PlanBuilder(session, idAllocator, metadata);
        plan = planProvider.apply(builder);
        types = builder.getTypes();
        return this;
    }

    public OptimizerAssert on(String sql)
    {
        checkState(plan == null, "plan has already been set");

        //get an initial plan and apply a minimal set of optimizers in preparation for applying the specific rules to be tested
        Plan result = queryRunner.inTransaction(session -> queryRunner.createPlan(session, sql, getMinimalOptimizers(), Optimizer.PlanStage.OPTIMIZED, WarningCollector.NOOP));
        plan = result.getRoot();
        types = result.getTypes();
        return this;
    }

    public void matches(PlanMatchPattern pattern)
    {
        inTransaction(session -> {
            assertPlan(session, metadata, statsCalculator, applyRules(), pattern);
            return null;
        });
    }

    public void doesNotMatch(PlanMatchPattern pattern)
    {
        inTransaction(session -> {
            assertPlanDoesNotMatch(session, metadata, statsCalculator, applyRules(), pattern);
            return null;
        });
    }

    public void validates(Consumer<Plan> planValidator)
    {
        planValidator.accept(applyRules());
    }

    private Plan applyRules()
    {
        PlanNode actual = optimizer.optimize(plan, session, types, new VariableAllocator(), idAllocator, WarningCollector.NOOP).getPlanNode();

        if (!ImmutableSet.copyOf(plan.getOutputVariables()).equals(ImmutableSet.copyOf(actual.getOutputVariables()))) {
            fail(String.format(
                    "%s: output schema of transformed and original plans are not equivalent\n" +
                            "\texpected: %s\n" +
                            "\tactual:   %s",
                    optimizer.getClass().getName(),
                    plan.getOutputVariables(),
                    actual.getOutputVariables()));
        }
        return new Plan(actual, types, StatsAndCosts.empty());
    }

    private List<PlanOptimizer> getMinimalOptimizers()
    {
        ImmutableSet.Builder<Rule<?>> rulesBuilder = ImmutableSet.builder();
        rulesBuilder.add(new TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin());
        rulesBuilder.add(new TransformUncorrelatedInPredicateSubqueryToSemiJoin());
        rulesBuilder.add(new RemoveRedundantIdentityProjections());
        ImmutableSet<Rule<?>> rules = rulesBuilder.build();
        return ImmutableList.of(
                new UnaliasSymbolReferences(queryRunner.getMetadata().getFunctionAndTypeManager()),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        queryRunner.getMetadata(),
                        new RuleStatsRecorder(),
                        queryRunner.getStatsCalculator(),
                        queryRunner.getCostCalculator(),
                        rules),
                new IterativeOptimizer(
                        queryRunner.getMetadata(),
                        new RuleStatsRecorder(),
                        queryRunner.getStatsCalculator(),
                        queryRunner.getCostCalculator(),
                        new SimplifyRowExpressions(
                                metadata,
                                new ExpressionOptimizerManager(
                                        new PluginNodeManager(new InMemoryNodeManager()),
                                        queryRunner.getFunctionAndTypeManager(),
                                        new JsonCodecRowExpressionSerde(jsonCodec(RowExpression.class)))).rules()));
    }

    private <T> void inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .execute(session, session -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
                    return transactionSessionConsumer.apply(session);
                });
    }
}
