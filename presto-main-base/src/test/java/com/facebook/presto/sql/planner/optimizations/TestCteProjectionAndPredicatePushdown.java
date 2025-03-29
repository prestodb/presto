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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.RuleStatsRecorder;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.InlineProjections;
import com.facebook.presto.sql.planner.iterative.rule.PruneProjectColumns;
import com.facebook.presto.sql.planner.iterative.rule.PruneRedundantProjectionAssignments;
import com.facebook.presto.sql.planner.iterative.rule.PruneTableScanColumns;
import com.facebook.presto.sql.planner.iterative.rule.RemoveIdentityProjectionsBelowProjection;
import com.facebook.presto.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED;
import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.cteConsumer;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.cteProducer;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sequence;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.optimizations.TestLogicalCteOptimizer.addQueryScopeDelimiter;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class TestCteProjectionAndPredicatePushdown
        extends BasePlanTest
{
    @Test
    public void testProjectionPushdown()
    {
        assertCtePlan("WITH  temp as (SELECT * FROM ORDERS) " +
                        "select * from (SELECT orderkey FROM temp) t JOIN (select custkey from temp) t2 ON true ",
                anyTree(
                        sequence(cteProducer(addQueryScopeDelimiter("temp", 0), project(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 0))))));
    }

    @Test
    public void testFilterPushdown()
    {
        assertCtePlan("WITH  temp as (SELECT * FROM ORDERS) " +
                        "select * from (SELECT * FROM temp where custkey=2) t JOIN (select * from temp where orderkey=1) t2 ON true ",
                anyTree(
                        sequence(cteProducer(addQueryScopeDelimiter("temp", 0),
                                        filter("custkey = 2 OR orderkey = 1",
                                                tableScan("orders", identityMap("custkey", "orderkey")))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 0))))));
    }

    @Test
    public void testComplexQueryFilterPushdown()
    {
        String testQuery = "WITH  nation_region AS ( " +
                "                SELECT n.name, r.comment AS region_comment, n.comment AS nation_comment" +
                "                FROM region r join nation n on (r.regionkey=n.regionkey)) " +
                "select * from nation_region where name = 'abc' " +
                "union all " +
                "select * from nation_region where name = 'bcd'";

        assertCtePlan(testQuery,
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("nation_region", 0),
                                        project(// no projection pushdown but a project gets added to make sure columns are ordered the right way
                                                filter("name = 'abc' or name = 'bcd'",
                                                        join(tableScan("region"), tableScan("nation", identityMap("name", "comment", "regionkey")))))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("nation_region", 0))))));
    }

    @Test
    public void testNoFilterPushdown()
    {
        // one of the CTE consumers is used without a filter: no filter gets pushed to the producer as all rows are needed
        assertCtePlan("WITH  temp as (SELECT * FROM ORDERS) " +
                        "select * from (SELECT * FROM temp where custkey=2) t JOIN (select * from temp) t2 ON true ",
                anyTree(
                        sequence(cteProducer(addQueryScopeDelimiter("temp", 0),
                                        tableScan("orders")),
                                anyTree(
                                        join(INNER, ImmutableList.of(),
                                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 0))),
                                                cteConsumer(addQueryScopeDelimiter("temp", 0)))))));
    }

    @Test
    public void testNoProjectionPushdown()
    {
        // one of the CTE consumers is used without a projection: no projection gets pushed to the producer as all columns are needed
        assertCtePlan("WITH  temp as (SELECT * FROM ORDERS) " +
                        "select * from (SELECT custkey FROM temp) t JOIN (select * from temp) t2 ON true ",
                anyTree(
                        sequence(cteProducer(addQueryScopeDelimiter("temp", 0),
                                        tableScan("orders")),
                                anyTree(
                                        join(INNER, ImmutableList.of(),
                                                project(cteConsumer(addQueryScopeDelimiter("temp", 0))),
                                                cteConsumer(addQueryScopeDelimiter("temp", 0)))))));
    }

    private void assertCtePlan(String sql, PlanMatchPattern pattern)
    {
        Metadata metadata = getQueryRunner().getMetadata();
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new LogicalCteOptimizer(metadata),
                new PruneUnreferencedOutputs(),
                new UnaliasSymbolReferences(metadata.getFunctionAndTypeManager()),
                new IterativeOptimizer(
                        metadata,
                        new RuleStatsRecorder(),
                        getQueryRunner().getStatsCalculator(),
                        getQueryRunner().getCostCalculator(),
                        ImmutableSet.of(
                                new InlineProjections(metadata.getFunctionAndTypeManager()),
                                new PruneProjectColumns(),
                                new PruneTableScanColumns(),
                                new RemoveRedundantIdentityProjections(),
                                new RemoveIdentityProjectionsBelowProjection(),
                                new PruneRedundantProjectionAssignments())),
                new PruneUnreferencedOutputs(),
                new CteProjectionAndPredicatePushDown(metadata, getQueryRunner().getExpressionManager()));
        assertPlan(sql, getSession(), Optimizer.PlanStage.OPTIMIZED, pattern, optimizers);
    }

    private Session getSession()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .setSystemProperty(CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED, "true")
                .build();
    }

    private static Map<String, String> identityMap(String... values)
    {
        return Arrays.stream(values).collect(toImmutableMap(Functions.identity(), Functions.identity()));
    }
}
