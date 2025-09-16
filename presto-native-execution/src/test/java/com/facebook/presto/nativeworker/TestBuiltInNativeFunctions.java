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

package com.facebook.presto.nativeworker;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.builtin.tools.WorkerFunctionUtil.createSqlInvokedFunction;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.facebook.presto.util.AnalyzerUtil.createParsingOptions;
import static java.util.Collections.emptyList;
import static org.testng.Assert.fail;

public class TestBuiltInNativeFunctions
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createNation(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createRegion(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setBuiltInWorkerFunctionsEnabled(true)
                .build();

        queryRunner.registerNativeFunctions();
        queryRunner.registerWorkerAggregateFunctions(getTestAggregationFunctions());
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());

        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();

        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());

        return queryRunner;
    }

    private List<SqlInvokedFunction> getTestAggregationFunctions()
    {
        JsonBasedUdfFunctionMetadata testFunctionMetadata = new JsonBasedUdfFunctionMetadata(
                "presto.default.test_agg_function",
                FunctionKind.AGGREGATE,
                new TypeSignature("bigint"),
                ImmutableList.of(new TypeSignature("integer"), new TypeSignature("bigint"), new TypeSignature("bigint")),
                "default",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.of(new AggregationFunctionMetadata(new TypeSignature("varbinary"), true)),
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of()),
                Optional.of(ImmutableList.of()),
                Optional.empty());

        return ImmutableList.of(createSqlInvokedFunction("test_agg_function", testFunctionMetadata, "presto"));
    }

    private void assertJsonPlan(@Language("SQL") String query, boolean withBuiltInSidecarEnabled, @Language("RegExp") String jsonPlanRegex, boolean shouldContainRegex)
    {
        QueryRunner queryRunner;
        if (withBuiltInSidecarEnabled) {
            queryRunner = getQueryRunner();
        }
        else {
            queryRunner = (QueryRunner) getExpectedQueryRunner();
        }

        QueryExplainer explainer = getQueryExplainerFromProvidedQueryRunner(queryRunner);
        transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .singleStatement()
                .execute(queryRunner.getDefaultSession(), transactionSession -> {
                    String actualPlan = explainer.getJsonPlan(transactionSession, getSqlParser().createStatement(query, createParsingOptions(transactionSession)), ExplainType.Type.LOGICAL, emptyList(), WarningCollector.NOOP, query);
                    Pattern p = Pattern.compile(jsonPlanRegex, Pattern.MULTILINE);
                    if (shouldContainRegex) {
                        if (!p.matcher(actualPlan).find()) {
                            fail("Query plan text does not contain regex");
                        }
                    }
                    else {
                        if (p.matcher(actualPlan).find()) {
                            fail("Query plan text contains bad pattern");
                        }
                    }

                    return null;
                });
    }

    private QueryExplainer getQueryExplainerFromProvidedQueryRunner(QueryRunner queryRunner)
    {
        Metadata metadata = queryRunner.getMetadata();
        FeaturesConfig featuresConfig = createFeaturesConfig();
        boolean noExchange = queryRunner.getNodeCount() == 1;
        TaskCountEstimator taskCountEstimator = new TaskCountEstimator(queryRunner::getNodeCount);
        CostCalculator costCalculator = new CostCalculatorUsingExchanges(taskCountEstimator);
        List<PlanOptimizer> optimizers = new PlanOptimizers(
                metadata,
                getSqlParser(),
                noExchange,
                new MBeanExporter(new TestingMBeanServer()),
                queryRunner.getSplitManager(),
                queryRunner.getPlanOptimizerManager(),
                queryRunner.getPageSourceManager(),
                queryRunner.getStatsCalculator(),
                costCalculator,
                new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator),
                new CostComparator(featuresConfig),
                taskCountEstimator,
                new PartitioningProviderManager(),
                featuresConfig,
                new ExpressionOptimizerManager(
                        new PluginNodeManager(new InMemoryNodeManager()),
                        queryRunner.getMetadata().getFunctionAndTypeManager()),
                new TaskManagerConfig())
                .getPlanningTimeOptimizers();
        return new QueryExplainer(
                optimizers,
                new PlanFragmenter(metadata, queryRunner.getNodePartitioningManager(), new QueryManagerConfig(), featuresConfig, queryRunner.getPlanCheckerProviderManager()),
                metadata,
                queryRunner.getAccessControl(),
                getSqlParser(),
                queryRunner.getStatsCalculator(),
                costCalculator,
                ImmutableMap.of(),
                new PlanChecker(featuresConfig, false, queryRunner.getPlanCheckerProviderManager()));
    }

    @Test
    public void testUdfQueries()
    {
        assertQuery("SELECT ARRAY['abc']");
        assertQuery("SELECT ARRAY[1, 2, 3]");
        assertQuery("SELECT map_remove_null_values( MAP( ARRAY['a', 'b', 'c'], ARRAY[1, NULL, 3] ) )");
        assertQuery("SELECT presto.default.map_remove_null_values( MAP( ARRAY['a', 'b', 'c'], ARRAY[1, NULL, 3] ) )");
        assertQueryFails("SELECT native.default.map_remove_null_values( MAP( ARRAY['a', 'b', 'c'], ARRAY[1, NULL, 3] ) )", ".*Function native.default.map_remove_null_values not registered.*");
        assertJsonPlan("SELECT map_remove_null_values( MAP( ARRAY['a', 'b', 'c'], ARRAY[1, NULL, 3] ) )", true, "lambda", false);
        assertJsonPlan("SELECT map_remove_null_values( MAP( ARRAY['a', 'b', 'c'], ARRAY[1, NULL, 3] ) )", false, "lambda", true);

        assertPlan("SELECT test_agg_function(cast(orderkey as integer), cast(orderkey as integer), cast(orderkey as bigint)) FROM tpch.tiny.orders", anyTree(any()));
        assertPlan("SELECT test_agg_function(5, cast(orderkey as smallint), orderkey) FROM tpch.tiny.orders", anyTree(any()));
    }
}
