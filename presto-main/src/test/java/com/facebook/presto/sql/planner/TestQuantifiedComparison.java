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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PruneIdentityProjections;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.apply;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestQuantifiedComparison
        extends BasePlanTest
{
    @Test
    public void testQuantifiedComparisonEqualsAny()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey = ANY (VALUES ROW(CAST(5 as BIGINT)), ROW(CAST(3 as BIGINT)))";
        assertUnitPlan(query, anyTree(
                filter("X IN (Y)",
                        apply(ImmutableList.of(),
                                tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                values(ImmutableMap.of("Y", 0))))));
        assertPlan(query, anyTree(
                filter("S",
                        project(
                                semiJoin("X", "Y", "S",
                                        anyTree(tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                        anyTree(values(ImmutableMap.of("Y", 0))))))));
    }

    @Test
    public void testQuantifiedComparisonNotEqualsAll()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey <> ALL (VALUES ROW(CAST(5 as BIGINT)), ROW(CAST(3 as BIGINT)))";
        assertUnitPlan(query, anyTree(
                filter("NOT (X IN (Y))",
                        apply(ImmutableList.of(),
                                tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                values(ImmutableMap.of("Y", 0))))));
        assertPlan(query, anyTree(
                filter("NOT S",
                        project(
                                semiJoin("X", "Y", "S",
                                        anyTree(tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                        anyTree(values(ImmutableMap.of("Y", 0))))))));
    }

    @Test
    public void testQuantifiedComparisonLessAll()
    {
        assertOrderedQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey < ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))",
                "X < MIN", "X", "min", "MIN");
    }

    @Test
    public void testQuantifiedComparisonGreaterEqualAll()
    {
        assertOrderedQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey >= ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))",
                "X >= MAX", "X", "max", "MAX");
    }

    @Test
    public void testQuantifiedComparisonLessSome()
    {
        assertOrderedQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey < SOME (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))",
                "X < MAX", "X", "max", "MAX");
    }

    @Test
    public void testQuantifiedComparisonGreaterEqualAny()
    {
        assertOrderedQuantifiedComparison("SELECT orderkey, custkey FROM orders WHERE orderkey >= ANY (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))",
                "X >= MIN", "X", "min", "MIN");
    }

    @Test
    public void testQuantifiedComparisonEqualAll()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey = ALL (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))";

        assertUnitPlan(query,
                anyTree(
                        project(
                                filter("MIN = MAX AND X = MIN",
                                        apply(ImmutableList.of(),
                                                tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                                node(EnforceSingleRowNode.class,
                                                        aggregation(
                                                                ImmutableMap.of(
                                                                        "MIN", functionCall("min", ImmutableList.of("FIELD")),
                                                                        "MAX", functionCall("max", ImmutableList.of("FIELD"))),
                                                                values(ImmutableMap.of("FIELD", 0)))))))));

        assertPlan(query,
                anyTree(
                node(JoinNode.class,
                        anyTree(
                                tableScan("orders")),
                        anyTree(
                                node(AggregationNode.class,
                                        node(ValuesNode.class))))));
    }

    @Test
    public void testQuantifiedComparisonNotEqualAny()
    {
        String query = "SELECT orderkey, custkey FROM orders WHERE orderkey <> SOME (VALUES CAST(5 as BIGINT), CAST(3 as BIGINT))";

        assertUnitPlan(query,
                anyTree(
                        project(
                                filter("NOT (MIN = MAX AND X = MIN)",
                                        apply(ImmutableList.of(),
                                                tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                                node(EnforceSingleRowNode.class,
                                                        aggregation(
                                                                ImmutableMap.of(
                                                                        "MIN", functionCall("min", ImmutableList.of("FIELD")),
                                                                        "MAX", functionCall("max", ImmutableList.of("FIELD"))),
                                                                values(ImmutableMap.of("FIELD", 0)))))))));
        assertPlan(query, anyTree(
                node(JoinNode.class,
                        tableScan("orders"),
                        anyTree(
                                node(AggregationNode.class,
                                        node(ValuesNode.class))))));
    }

    private void assertOrderedQuantifiedComparison(String query, String filter, String columnMapping, String function, String functionAlias)
    {
        assertUnitPlan(query, anyTree(
                project(
                        filter(filter,
                                apply(ImmutableList.of(),
                                        tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                        node(EnforceSingleRowNode.class,
                                                aggregation(
                                                        ImmutableMap.of(
                                                                functionAlias, functionCall(function, ImmutableList.of("FIELD"))),
                                                        values(ImmutableMap.of("FIELD", 0)))))))));

        assertPlan(query, anyTree(
                project(
                        filter(filter,
                                join(INNER, ImmutableList.of(),
                                        tableScan("orders", ImmutableMap.of(columnMapping, "orderkey")),
                                        node(EnforceSingleRowNode.class,
                                                aggregation(
                                                        ImmutableMap.of(
                                                                functionAlias, functionCall(function, ImmutableList.of("FIELD"))),
                                                        values(ImmutableMap.of("FIELD", 0)))))))));
    }

    private void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        LocalQueryRunner queryRunner = getQueryRunner();
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setDistributedIndexJoinsEnabled(false)
                .setOptimizeHashGeneration(true);
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneIdentityProjections(),
                new PruneUnreferencedOutputs());
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, featuresConfig, optimizers, LogicalPlanner.Stage.OPTIMIZED);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }
}
