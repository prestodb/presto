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

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.facebook.presto.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.groupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestOptimizeMixedDistinctAggregations
        extends BasePlanTest
{
    public TestOptimizeMixedDistinctAggregations()
    {
        super(ImmutableMap.of(SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS, "true"));
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
                aggregation(singleGroupingSet(groupByKeysSecond), aggregationsSecond, ImmutableMap.of(), Optional.empty(), SINGLE,
                        project(
                                aggregation(singleGroupingSet(groupByKeysFirst), aggregationsFirst, ImmutableMap.of(), Optional.empty(), SINGLE,
                                        groupingSet(groups.build(), ImmutableMap.of(), group,
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
                                                anyTree(values(ImmutableMap.of())))))));
    }

    private void assertUnitPlan(String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(getMetadata().getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        getMetadata(),
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getEstimatedExchangesCostCalculator(),
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new SingleDistinctAggregationToGroupBy(),
                                new MultipleDistinctAggregationToMarkDistinct())),
                new OptimizeMixedDistinctAggregations(getQueryRunner().getMetadata()),
                new PruneUnreferencedOutputs());
        assertPlan(sql, pattern, optimizers);
    }

    @Test
    public void testIssue27860PercentileVarInGroupIdNodeGroupingSets()
    {
        // Issue #27860: OptimizeMixedDistinctAggregations builds GroupIdNode with two grouping sets:
        //   g0 = {orderstatus, cast_totalprice, percentile_var}
        //   g1 = {orderstatus, custkey}
        // The percentile_var (2nd arg of approx_percentile) is absent from g1, so GroupIdNode
        // NULLs it out for g1 rows. Velox then sees inconsistent percentile values across a batch:
        //   "Percentile argument must be constant for all input rows (0 vs. 0.9)"
        // FIX: percentile_var must appear in g1 as well so it is never NULLed.
        // This test FAILS with the bug present and PASSES after the fix.
        @Language("SQL") String sql =
                "SELECT approx_percentile(CAST(totalprice AS BIGINT), CAST(90 AS DOUBLE) / 100)," +
                "       count(DISTINCT custkey) " +
                "FROM orders " +
                "GROUP BY orderstatus";

        List<PlanOptimizer> optimizers = buildUnitOptimizers();

        getQueryRunner().inTransaction(getQueryRunner().getDefaultSession(), session -> {
            Plan plan = getQueryRunner().createPlan(
                    session,
                    sql,
                    optimizers,
                    Optimizer.PlanStage.OPTIMIZED,
                    WarningCollector.NOOP);

            GroupIdNode groupIdNode = findGroupIdNode(plan.getRoot());
            assertNotNull(groupIdNode, "GroupIdNode must exist after OptimizeMixedDistinctAggregations");

            AggregationNode innerAgg = findAggregationWithGroupIdSource(plan.getRoot(), groupIdNode);
            assertNotNull(innerAgg, "Inner AggregationNode (with GroupIdNode as source) not found");

            // Find the approx_percentile call and extract its 2nd argument (the percentile variable)
            Optional<VariableReferenceExpression> percentileVar = innerAgg.getAggregations().values().stream()
                    .filter(agg -> agg.getCall().getDisplayName().equals("approx_percentile"))
                    .flatMap(agg -> agg.getArguments().stream().skip(1).limit(1))
                    .filter(VariableReferenceExpression.class::isInstance)
                    .map(VariableReferenceExpression.class::cast)
                    .findFirst();

            assertTrue(percentileVar.isPresent(), "approx_percentile with 2+ args not found in inner aggregation");

            List<VariableReferenceExpression> g1 = groupIdNode.getGroupingSets().get(1);

            // This assertion FAILS with the bug: g1 does not contain percentileVar
            // (GroupIdNode NULLs it out for g1 rows, causing the Velox error)
            assertTrue(
                    g1.contains(percentileVar.get()),
                    "Bug #27860: percentile variable '" + percentileVar.get().getName() +
                    "' is absent from g1 grouping set. GroupIdNode NULLs it out for g1 rows, " +
                    "causing Velox: 'Percentile argument must be constant for all input rows'");

            return null;
        });
    }

    private List<PlanOptimizer> buildUnitOptimizers()
    {
        return ImmutableList.of(
                new UnaliasSymbolReferences(getMetadata().getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        getMetadata(),
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getEstimatedExchangesCostCalculator(),
                        ImmutableSet.of(
                                new RemoveRedundantIdentityProjections(),
                                new SingleDistinctAggregationToGroupBy(),
                                new MultipleDistinctAggregationToMarkDistinct())),
                new OptimizeMixedDistinctAggregations(getQueryRunner().getMetadata()),
                new PruneUnreferencedOutputs());
    }

    private static GroupIdNode findGroupIdNode(PlanNode root)
    {
        if (root instanceof GroupIdNode) {
            return (GroupIdNode) root;
        }
        for (PlanNode source : root.getSources()) {
            GroupIdNode found = findGroupIdNode(source);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static AggregationNode findAggregationWithGroupIdSource(PlanNode root, GroupIdNode groupIdNode)
    {
        if (root instanceof AggregationNode && root.getSources().contains(groupIdNode)) {
            return (AggregationNode) root;
        }
        for (PlanNode source : root.getSources()) {
            AggregationNode found = findAggregationWithGroupIdSource(source, groupIdNode);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
