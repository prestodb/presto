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
import com.facebook.presto.sql.planner.StatsRecorder;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.groupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestMixedDistinctAggregationOptimizer
{
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS, "true")
                .build();

        queryRunner = new LocalQueryRunner(defaultSession);
        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    @Test
    public void testMixedDistinctAggregationOptimizer()
    {
        @Language("SQL") String sql = "SELECT custkey, max(totalprice) AS s, count(DISTINCT orderdate) AS d FROM orders GROUP BY custkey";

        String group = "GROUP";

        // Original keys
        String groupBy = "CUSTKEY";
        String aggregate = "TOTALPRICE";
        String distinctAggregation = "ORDERDATE";

        // Second Aggregation data
        List<String> groupByKeysSecond = ImmutableList.of(groupBy);
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregationsSecond = ImmutableMap.of(
                Optional.of("arbitrary"), PlanMatchPattern.functionCall("arbitrary", false, ImmutableList.of(anySymbol())),
                Optional.of("count"), PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol())));

        // First Aggregation data
        List<String> groupByKeysFirst = ImmutableList.of(groupBy, distinctAggregation, group);
        Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregationsFirst = ImmutableMap.of(
                Optional.of("MAX"), functionCall("max", ImmutableList.of("TOTALPRICE")));

        PlanMatchPattern tableScan = tableScan("orders", ImmutableMap.of("TOTALPRICE", "totalprice", "CUSTKEY", "custkey", "ORDERDATE", "orderdate"));

        // GroupingSet symbols
        ImmutableList.Builder<List<String>> groups = ImmutableList.builder();
        groups.add(ImmutableList.of(groupBy, aggregate));
        groups.add(ImmutableList.of(groupBy, distinctAggregation));
        PlanMatchPattern expectedPlanPattern = anyTree(
                aggregation(ImmutableList.of(groupByKeysSecond), aggregationsSecond, ImmutableMap.of(), Optional.empty(),
                        project(
                                aggregation(ImmutableList.of(groupByKeysFirst), aggregationsFirst, ImmutableMap.of(), Optional.empty(),
                                        groupingSet(groups.build(), group,
                                                anyTree(tableScan))))));

        assertUnitPlan(sql, expectedPlanPattern);
    }

    @Test
    public void testNestedType()
    {
        // Second Aggregation data
        Map<String, ExpectedValueProvider<FunctionCall>> aggregationsSecond = ImmutableMap.of(
                "arbitrary", PlanMatchPattern.functionCall("arbitrary", false, ImmutableList.of(anySymbol())),
                "count", PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol())));

        // First Aggregation data
        Map<String, ExpectedValueProvider<FunctionCall>> aggregationsFirst = ImmutableMap.of(
                "max", PlanMatchPattern.functionCall("max", false, ImmutableList.of(anySymbol())));

        assertUnitPlan("SELECT count(DISTINCT a), max(b) FROM (VALUES (ROW(1, 2), 3)) t(a, b)",
                anyTree(
                        aggregation(aggregationsSecond,
                                project(
                                        aggregation(aggregationsFirst,
                                                anyTree(values(ImmutableMap.of()))
                                        )))));
    }

    public void assertUnitPlan(String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new IterativeOptimizer(new StatsRecorder(), ImmutableSet.of(new RemoveRedundantIdentityProjections())),
                new OptimizeMixedDistinctAggregations(queryRunner.getMetadata()),
                new PruneUnreferencedOutputs());

        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }
}
