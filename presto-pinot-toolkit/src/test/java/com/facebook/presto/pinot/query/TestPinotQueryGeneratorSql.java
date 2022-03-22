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
package com.facebook.presto.pinot.query;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertFalse;

public class TestPinotQueryGeneratorSql
        extends TestPinotQueryGenerator
{
    public TestPinotQueryGeneratorSql()
    {
        pinotConfig.setUsePinotSqlForBrokerQueries(useSqlSyntax());
    }

    @Override
    public boolean useSqlSyntax()
    {
        return true;
    }

    @Test
    public void assertUsingSqlSyntax()
    {
        Assert.assertEquals(defaultSessionHolder.getConnectorSession().getProperty("use_pinot_sql_for_broker_queries", Boolean.class).booleanValue(), true);
    }

    @Override
    public String getExpectedAggOutput(String expectedAggOutput, String groupByColumns)
    {
        return groupByColumns.isEmpty() ? expectedAggOutput : groupByColumns + ", " + expectedAggOutput;
    }

    @Override
    protected String getGroupByLimitKey()
    {
        return "LIMIT";
    }

    @Override
    @Test
    public void testMultipleAggregatesNotAllowed()
    {
        super.helperTestMultipleAggregatesWithGroupBy(pinotConfig);
    }

    @Override
    @Test
    public void testMultipleAggregateGroupByWithLimitFails()
    {
        super.testMultipleAggregateGroupByWithLimitFails();
    }

    @Test
    public void testAggregationWithGroupBy()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, regionId, city, fare);
        AggregationNode agg = planBuilder.aggregation(
                aggBuilder -> aggBuilder
                        .source(tableScanNode)
                        .singleGroupingSet(variable("city"), variable("regionid"))
                        .addAggregation(planBuilder.variable("sum_fare"), getRowExpression("sum(fare)", defaultSessionHolder))
                        .addAggregation(planBuilder.variable("count_regionid"), getRowExpression("count(regionid)", defaultSessionHolder)));
        testPinotQuery(
                pinotConfig,
                agg,
                ImmutableList.of("SELECT city, regionId, sum(fare), count(regionId) FROM realtimeOnly GROUP BY city, regionId LIMIT 10000", "SELECT city, regionId, count(regionId), sum(fare) FROM realtimeOnly GROUP BY city, regionId LIMIT 10000"),
                defaultSessionHolder,
                ImmutableMap.of());
        ProjectNode proj = planBuilder.project(
                Assignments.builder()
                        .put(variable("count_regionid"), variable("count_regionid"))
                        .put(variable("city"), variable("city"))
                        .put(variable("sum_fare"), variable("sum_fare"))
                        .build(),
                agg);
        testPinotQuery(
                pinotConfig,
                proj,
                "SELECT count(regionId), city, sum(fare) FROM realtimeOnly GROUP BY city, regionId LIMIT 10000",
                defaultSessionHolder,
                ImmutableMap.of());

        proj = planBuilder.project(
                Assignments.builder()
                        .put(variable("sum_fare"), variable("sum_fare"))
                        .put(variable("count_regionid"), variable("count_regionid"))
                        .build(),
                agg);
        testPinotQuery(
                pinotConfig,
                proj,
                "SELECT sum(fare), count(regionId) FROM realtimeOnly GROUP BY city, regionId LIMIT 10000",
                defaultSessionHolder,
                ImmutableMap.of());
    }

    @Override
    @Test
    public void testAggregationWithOrderByPushDownInTopN()
    {
        pinotConfig.setPushdownTopNBrokerQueries(true);
        SessionHolder sessionHolder = new SessionHolder(pinotConfig);
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, city, fare);
        AggregationNode agg = planBuilder.aggregation(
                aggBuilder -> aggBuilder
                        .source(tableScanNode)
                        .singleGroupingSet(variable("city"))
                        .addAggregation(planBuilder.variable("sum_fare"), getRowExpression("sum(fare)", defaultSessionHolder)));
        testPinotQuery(
                pinotConfig,
                agg,
                "SELECT city, sum(fare) FROM realtimeOnly GROUP BY city LIMIT 10000",
                sessionHolder,
                ImmutableMap.of());

        TopNNode topN = new TopNNode(
                planBuilder.getIdAllocator().getNextId(),
                agg,
                50L,
                new OrderingScheme(ImmutableList.of(new Ordering(variable("city"), SortOrder.DESC_NULLS_FIRST))),
                TopNNode.Step.SINGLE);
        testPinotQuery(
                pinotConfig,
                topN,
                "SELECT city, sum(fare) FROM realtimeOnly GROUP BY city ORDER BY city DESC LIMIT 50",
                sessionHolder,
                ImmutableMap.of());

        topN = new TopNNode(
                planBuilder.getIdAllocator().getNextId(),
                agg,
                1000L,
                new OrderingScheme(ImmutableList.of(new Ordering(variable("sum_fare"), SortOrder.ASC_NULLS_FIRST))),
                TopNNode.Step.SINGLE);
        testPinotQuery(
                pinotConfig,
                topN,
                "SELECT city, sum(fare) FROM realtimeOnly GROUP BY city ORDER BY sum(fare) LIMIT 1000",
                sessionHolder,
                ImmutableMap.of());

        topN = new TopNNode(
                planBuilder.getIdAllocator().getNextId(),
                agg,
                1000L,
                new OrderingScheme(ImmutableList.of(new Ordering(variable("sum_fare"), SortOrder.ASC_NULLS_FIRST))),
                TopNNode.Step.SINGLE);
        testPinotQuery(
                pinotConfig,
                topN,
                "SELECT city, sum(fare) FROM realtimeOnly GROUP BY city ORDER BY sum(fare) LIMIT 1000",
                sessionHolder,
                ImmutableMap.of());
    }

    @Test
    public void testDefaultNoTopNPushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, city, fare);
        AggregationNode agg = planBuilder.aggregation(
                aggBuilder -> aggBuilder
                        .source(tableScanNode)
                        .singleGroupingSet(variable("city"))
                        .addAggregation(planBuilder.variable("sum_fare"), getRowExpression("sum(fare)", defaultSessionHolder)));
        pinotConfig.setPushdownTopNBrokerQueries(false);
        TopNNode topN = new TopNNode(planBuilder.getIdAllocator().getNextId(), agg, 1000,
                new OrderingScheme(ImmutableList.of(new Ordering(variable("sum_fare"), SortOrder.ASC_NULLS_FIRST))),
                TopNNode.Step.SINGLE);
        Optional<PinotQueryGenerator.PinotQueryGeneratorResult> generatedQuery =
                new PinotQueryGenerator(pinotConfig, functionAndTypeManager, functionAndTypeManager, standardFunctionResolution)
                        .generate(topN, defaultSessionHolder.getConnectorSession());
        assertFalse(generatedQuery.isPresent());
        SessionHolder sessionHolder = new SessionHolder(pinotConfig);
        testPinotQuery(
                pinotConfig,
                agg,
                "SELECT city, sum(fare) FROM realtimeOnly GROUP BY city LIMIT 10000",
                sessionHolder,
                ImmutableMap.of());
    }

    @Test
    public void testSelectionWithOrderBy()
    {
        pinotConfig.setPushdownTopNBrokerQueries(true);
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, regionId, city, fare);
        SessionHolder sessionHolder = new SessionHolder(pinotConfig);
        testPinotQuery(
                pinotConfig,
                topN(planBuilder, 50L, ImmutableList.of("fare"), ImmutableList.of(false), tableScanNode),
                "SELECT regionId, city, fare FROM realtimeOnly ORDER BY fare DESC LIMIT 50",
                sessionHolder,
                ImmutableMap.of());
        testPinotQuery(
                pinotConfig,
                topN(planBuilder, 50L, ImmutableList.of("fare", "city"), ImmutableList.of(true, false), tableScanNode),
                "SELECT regionId, city, fare FROM realtimeOnly ORDER BY fare, city DESC LIMIT 50",
                sessionHolder,
                ImmutableMap.of());
        testPinotQuery(
                pinotConfig,
                topN(planBuilder, 50L, ImmutableList.of("city", "fare"), ImmutableList.of(false, true), tableScanNode),
                "SELECT regionId, city, fare FROM realtimeOnly ORDER BY city DESC, fare LIMIT 50",
                sessionHolder,
                ImmutableMap.of());

        TopNNode topNNode = topN(planBuilder, 50L, ImmutableList.of("fare", "city"), ImmutableList.of(true, false), tableScanNode);
        testPinotQuery(
                pinotConfig,
                project(planBuilder, topNNode, ImmutableList.of("regionid", "city")),
                "SELECT regionId, city FROM realtimeOnly ORDER BY fare, city DESC LIMIT 50",
                sessionHolder,
                ImmutableMap.of());

        tableScanNode = tableScan(planBuilder, pinotTable, fare, city, regionId);
        testPinotQuery(
                pinotConfig,
                topN(planBuilder, 500L, ImmutableList.of("fare"), ImmutableList.of(false), tableScanNode),
                "SELECT fare, city, regionId FROM realtimeOnly ORDER BY fare DESC LIMIT 500",
                sessionHolder,
                ImmutableMap.of());
        testPinotQuery(
                pinotConfig,
                topN(planBuilder, 5000L, ImmutableList.of("fare", "city"), ImmutableList.of(true, false), tableScanNode),
                "SELECT fare, city, regionId FROM realtimeOnly ORDER BY fare, city DESC LIMIT 5000",
                sessionHolder,
                ImmutableMap.of());
    }

    @Override
    @Test
    public void testDistinctSelection()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare);
        AggregationNode aggregationNode = planBuilder.aggregation(aggBuilder -> aggBuilder.source(tableScanNode).singleGroupingSet(variable("regionid")));
        testPinotQuery(
                pinotConfig,
                aggregationNode,
                "SELECT regionId FROM realtimeOnly GROUP BY regionId LIMIT 10000",
                defaultSessionHolder,
                ImmutableMap.of());
    }

    @Override
    protected String getExpectedDistinctOutput(String groupKeys)
    {
        return groupKeys;
    }
}
