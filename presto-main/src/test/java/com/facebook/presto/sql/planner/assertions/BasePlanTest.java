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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.block.TestingBlockJsonSerde;
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.iterative.rule.TranslateExpressions;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.sql.planner.LogicalPlanner.Stage.CREATED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.fail;

public class BasePlanTest
{
    private final LocalQueryRunnerSupplier queryRunnerSupplier;
    private final ObjectMapper objectMapper;

    private LocalQueryRunner queryRunner;

    public BasePlanTest()
    {
        this(ImmutableMap.of());
    }

    public BasePlanTest(Map<String, String> sessionProperties)
    {
        System.out.println("Sourav BasePlanTest");
        this.queryRunnerSupplier = () -> createQueryRunner(sessionProperties);
        this.objectMapper = createObjectMapper();
    }

    public BasePlanTest(LocalQueryRunnerSupplier supplier)
    {
        this.queryRunnerSupplier = requireNonNull(supplier, "queryRunnerSupplier is null");
        this.objectMapper = createObjectMapper();
    }

    public ObjectMapper getObjectMapper()
    {
        return objectMapper;
    }

    private static ObjectMapper createObjectMapper()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
        return new JsonObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)))
                .configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    private static LocalQueryRunner createQueryRunner(Map<String, String> sessionProperties)
    {
        System.out.println("SOurav creating the queryRunner");
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        sessionProperties.entrySet().forEach(entry -> sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue()));

        System.out.println("Sourav just before calling localQueryRunner ctor");
        LocalQueryRunner queryRunner = new LocalQueryRunner(sessionBuilder.build(), new FeaturesConfig(), new NodeSpillConfig(), false, false, createObjectMapper());
        System.out.println("Sourav just before after localQueryRunner ctor");

        //queryRunner.loadFunctionNamespaceManager();
        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
        return queryRunner;
    }

    @BeforeClass
    public final void initPlanTest()
            throws Exception
    {
        System.out.println("Sourav Inside initPlanTest");
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

    protected void assertPlanWithFunctionNameSpaceManagerEnabled(String sql, PlanMatchPattern pattern)
    {
        queryRunner.installPlugin(new FunctionNamespaceManagerPlugin());
        Map<String, String> config = new HashMap<>();
        config.put("path-to-function-definition", "/Users/souravpal/Work/Presto/presto/presto-function-namespace-managers/src/test/resources/functionDefinition.json");
        config.put("supported-function-languages", "CPP");
        config.put("function-implementation-type", "CPP");
        queryRunner.loadFunctionNamespaceManager("filejson", "testunit", config);
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, pattern);
    }

    protected void assertPlan(String sql, Session session, PlanMatchPattern pattern)
    {
        assertPlan(sql, session, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, pattern, queryRunner.getPlanOptimizers(true));
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true);

        assertPlan(sql, queryRunner.getDefaultSession(), stage, pattern, optimizers);
    }

    protected void assertPlan(String sql, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        assertPlan(sql, queryRunner.getDefaultSession(), LogicalPlanner.Stage.OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern, Predicate<PlanOptimizer> optimizerPredicate)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true).stream()
                .filter(optimizerPredicate)
                .collect(toList());

        assertPlan(sql, queryRunner.getDefaultSession(), stage, pattern, optimizers);
    }

    protected void assertPlan(String sql, Session session, LogicalPlanner.Stage stage, PlanMatchPattern pattern, List<PlanOptimizer> optimizers)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(
                    transactionSession,
                    sql,
                    ImmutableList.<PlanOptimizer>builder()
                            .addAll(optimizers)
                            .add(getExpressionTranslator()).build(), // To avoid assert plan failure not printing out plan (#12885)
                    stage,
                    WarningCollector.NOOP);
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
                new UnaliasSymbolReferences(queryRunner.getMetadata().getFunctionAndTypeManager()),
                new PruneUnreferencedOutputs(),
                new IterativeOptimizer(
                        new RuleStatsRecorder(),
                        queryRunner.getStatsCalculator(),
                        queryRunner.getCostCalculator(),
                        ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                getExpressionTranslator()); // To avoid assert plan failure not printing out plan (#12885)

        assertPlan(sql, queryRunner.getDefaultSession(), LogicalPlanner.Stage.OPTIMIZED, pattern, optimizers);
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, forceSingleNode, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            return null;
        });
    }

    protected void assertPlanWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, PlanMatchPattern pattern, Consumer<Plan> planValidator)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, forceSingleNode, WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), queryRunner.getStatsCalculator(), actualPlan, pattern);
            planValidator.accept(actualPlan);
            return null;
        });
    }

    protected void assertPlanValidatorWithSession(@Language("SQL") String sql, Session session, boolean forceSingleNode, Consumer<Plan> planValidator)
    {
        queryRunner.inTransaction(session, transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, forceSingleNode, WarningCollector.NOOP);
            planValidator.accept(actualPlan);
            return null;
        });
    }

    protected void assertPlanFailedWithException(String sql, Session session, @Language("RegExp") String expectedExceptionRegex)
    {
        try {
            queryRunner.inTransaction(session, transactionSession -> queryRunner.createPlan(transactionSession, sql, CREATED, true, WarningCollector.NOOP));
            fail(format("Expected query to fail: %s", sql));
        }
        catch (RuntimeException ex) {
            if (!nullToEmpty(ex.getMessage()).matches(expectedExceptionRegex)) {
                fail(format("Expected exception message '%s' to match '%s' for query: %s", ex.getMessage(), expectedExceptionRegex, sql), ex);
            }
        }
    }

    protected void assertPlanSucceeded(String sql, Session session)
    {
        try {
            queryRunner.inTransaction(session, transactionSession -> queryRunner.createPlan(transactionSession, sql, CREATED, true, WarningCollector.NOOP));
        }
        catch (RuntimeException ex) {
            fail(format("Query %s failed with exception message '%s'", sql, ex.getMessage()), ex);
        }
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
            return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, stage, forceSingleNode, WarningCollector.NOOP));
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }

    protected Plan plan(String sql, LogicalPlanner.Stage stage, Session session)
    {
        return plan(sql, stage, true, session);
    }

    protected Plan plan(String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, Session session)
    {
        try {
            return queryRunner.inTransaction(session, transactionSession -> queryRunner.createPlan(transactionSession, sql, stage, forceSingleNode, WarningCollector.NOOP));
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }

    protected SubPlan subplan(String sql, LogicalPlanner.Stage stage, boolean forceSingleNode)
    {
        return subplan(sql, stage, forceSingleNode, getQueryRunner().getDefaultSession());
    }

    protected SubPlan subplan(String sql, LogicalPlanner.Stage stage, boolean forceSingleNode, Session session)
    {
        try {
            return queryRunner.inTransaction(session, transactionSession -> {
                Plan plan = queryRunner.createPlan(transactionSession, sql, stage, forceSingleNode, WarningCollector.NOOP);
                return queryRunner.createSubPlans(transactionSession, plan, forceSingleNode);
            });
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + sql, e);
        }
    }

    protected Metadata getMetadata()
    {
        return getQueryRunner().getMetadata();
    }

    // Translate all OriginalExpression in planNodes to RowExpression so that we can do plan pattern asserting and printing on RowExpression only.
    protected PlanOptimizer getExpressionTranslator()
    {
        return new IterativeOptimizer(
                new RuleStatsRecorder(),
                getQueryRunner().getStatsCalculator(),
                getQueryRunner().getCostCalculator(),
                ImmutableSet.copyOf(new TranslateExpressions(getMetadata(), getQueryRunner().getSqlParser()).rules()));
    }

    public interface LocalQueryRunnerSupplier
    {
        LocalQueryRunner get()
                throws Exception;
    }
}
