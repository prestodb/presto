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

import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.pinot.TestPinotQueryBase;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestPinotQueryGenerator
        extends TestPinotQueryBase
{
    private static final SessionHolder defaultSessionHolder = new SessionHolder(false);
    private static final PinotTableHandle pinotTable = realtimeOnlyTable;

    private void testPQL(
            PinotConfig givenPinotConfig,
            Function<PlanBuilder, PlanNode> planBuilderConsumer,
            String expectedPQL, SessionHolder sessionHolder,
            Map<String, String> outputVariables)
    {
        PlanNode planNode = planBuilderConsumer.apply(createPlanBuilder(sessionHolder));
        testPQL(givenPinotConfig, planNode, expectedPQL, sessionHolder, outputVariables);
    }

    private void testPQL(
            PinotConfig givenPinotConfig,
            PlanNode planNode,
            String expectedPQL,
            SessionHolder sessionHolder,
            Map<String, String> outputVariables)
    {
        PinotQueryGenerator.PinotQueryGeneratorResult pinotQueryGeneratorResult = new PinotQueryGenerator(givenPinotConfig, typeManager, functionMetadataManager, standardFunctionResolution).generate(planNode, sessionHolder.getConnectorSession()).get();
        if (expectedPQL.contains("__expressions__")) {
            String expressions = planNode.getOutputVariables().stream().map(v -> outputVariables.get(v.getName())).filter(v -> v != null).collect(Collectors.joining(", "));
            expectedPQL = expectedPQL.replace("__expressions__", expressions);
        }
        assertEquals(pinotQueryGeneratorResult.getGeneratedPql().getPql(), expectedPQL);
    }

    private void testPQL(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPQL, SessionHolder sessionHolder, Map<String, String> outputVariables)
    {
        testPQL(pinotConfig, planBuilderConsumer, expectedPQL, sessionHolder, outputVariables);
    }

    private void testPQL(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPQL, SessionHolder sessionHolder)
    {
        testPQL(planBuilderConsumer, expectedPQL, sessionHolder, ImmutableMap.of());
    }

    private void testPQL(PinotConfig givenPinotConfig, Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPQL)
    {
        testPQL(givenPinotConfig, planBuilderConsumer, expectedPQL, defaultSessionHolder, ImmutableMap.of());
    }

    private void testPQL(PinotConfig givenPinotConfig, PlanNode planNode, String expectedPQL)
    {
        testPQL(givenPinotConfig, planNode, expectedPQL, defaultSessionHolder, ImmutableMap.of());
    }

    private void testPQL(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPQL)
    {
        testPQL(planBuilderConsumer, expectedPQL, defaultSessionHolder);
    }

    private PlanNode buildPlan(Function<PlanBuilder, PlanNode> consumer)
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        return consumer.apply(planBuilder);
    }

    private void testUnaryAggregationHelper(BiConsumer<PlanBuilder, PlanBuilder.AggregationBuilder> aggregationFunctionBuilder, String expectedAggOutput)
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode filter = buildPlan(planBuilder -> filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare), getRowExpression("fare > 3", defaultSessionHolder)));
        PlanNode anotherFilter = buildPlan(planBuilder -> filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare), getRowExpression("secondssinceepoch between 200 and 300 and regionid >= 40", defaultSessionHolder)));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).globalGrouping())),
                format("SELECT %s FROM realtimeOnly", expectedAggOutput));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(filter).globalGrouping())),
                format("SELECT %s FROM realtimeOnly WHERE (fare > 3)", expectedAggOutput));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(filter).singleGroupingSet(variable("regionid")))),
                format("SELECT %s FROM realtimeOnly WHERE (fare > 3) GROUP BY regionId TOP 10000", expectedAggOutput));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).singleGroupingSet(variable("regionid")))),
                format("SELECT %s FROM realtimeOnly GROUP BY regionId TOP 10000", expectedAggOutput));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(anotherFilter).singleGroupingSet(variable("regionid"), variable("city")))),
                format("SELECT %s FROM realtimeOnly WHERE ((secondsSinceEpoch BETWEEN 200 AND 300) AND (regionId >= 40)) GROUP BY regionId, city TOP 10000", expectedAggOutput));
    }

    @Test
    public void testSimpleSelectStar()
    {
        testPQL(
                planBuilder -> limit(planBuilder, 50L, tableScan(planBuilder, pinotTable, regionId, city, fare, secondsSinceEpoch)),
                "SELECT regionId, city, fare, secondsSinceEpoch FROM realtimeOnly LIMIT 50");
        testPQL(
                planBuilder -> limit(planBuilder, 50L, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch)),
                "SELECT regionId, secondsSinceEpoch FROM realtimeOnly LIMIT 50");
    }

    @Test
    public void testSimpleSelectWithFilterLimit()
    {
        testPQL(
                planBuilder -> limit(planBuilder, 50L, project(planBuilder, filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, city, fare, secondsSinceEpoch), getRowExpression("secondssinceepoch > 20", defaultSessionHolder)), ImmutableList.of("city", "secondssinceepoch"))),
                "SELECT city, secondsSinceEpoch FROM realtimeOnly WHERE (secondsSinceEpoch > 20) LIMIT 50");
    }

    @Test
    public void testCountStar()
    {
        testUnaryAggregationHelper((planBuilder, aggregationBuilder) -> aggregationBuilder.addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)), "count(*)");
    }

    @Test
    public void testDistinctCountPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode distinctAggregation = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("regionid"))));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(distinctAggregation).globalGrouping().addAggregation(variable("count_regionid"), getRowExpression("count(regionid)", defaultSessionHolder))),
                "SELECT DISTINCTCOUNT(regionId) FROM realtimeOnly");
    }

    @Test
    public void testDistinctCountGroupByPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode distinctAggregation = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city"), variable("regionid"))));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(distinctAggregation).singleGroupingSet(variable("city")).addAggregation(variable("count_regionid"), getRowExpression("count(regionid)", defaultSessionHolder))),
                "SELECT DISTINCTCOUNT(regionId) FROM realtimeOnly GROUP BY city TOP 10000");
    }

    @Test
    public void testDistinctCountWithOtherAggregationPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode markDistinct = buildPlan(planBuilder -> markDistinct(planBuilder, variable("regionid$distinct"), ImmutableList.of(variable("regionid")), justScan));
        PlanNode aggregate = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(markDistinct).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("count(regionid)"), getRowExpression("count(regionid)", defaultSessionHolder), Optional.empty(), Optional.empty(), false, Optional.of(variable("regionid$distinct"))).globalGrouping()));
        String expectedPQL;
        if (aggregate.getOutputVariables().get(0).getName().equalsIgnoreCase("count(regionid)")) {
            expectedPQL = "SELECT DISTINCTCOUNT(regionId), count(*) FROM realtimeOnly";
        }
        else {
            expectedPQL = "SELECT count(*), DISTINCTCOUNT(regionId) FROM realtimeOnly";
        }
        testPQL(new PinotConfig().setAllowMultipleAggregations(true), planBuilder -> planBuilder.limit(10, aggregate), expectedPQL);
    }

    @Test
    public void testDistinctCountWithOtherAggregationGroupByPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode markDistinct = buildPlan(planBuilder -> markDistinct(planBuilder, variable("regionid$distinct"), ImmutableList.of(variable("regionid")), justScan));
        PlanNode aggregate = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(markDistinct).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("count(regionid)"), getRowExpression("count(regionid)", defaultSessionHolder), Optional.empty(), Optional.empty(), false, Optional.of(variable("regionid$distinct")))));
        String expectedPQL;
        if (aggregate.getOutputVariables().get(1).getName().equalsIgnoreCase("count(regionid)")) {
            expectedPQL = "SELECT DISTINCTCOUNT(regionId), count(*) FROM realtimeOnly GROUP BY city TOP 10000";
        }
        else {
            expectedPQL = "SELECT count(*), DISTINCTCOUNT(regionId) FROM realtimeOnly GROUP BY city TOP 10000";
        }
        testPQL(new PinotConfig().setAllowMultipleAggregations(true), aggregate, expectedPQL);
    }

    @Test
    public void testDistinctSelection()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("regionid"))),
                "SELECT count(*) FROM realtimeOnly GROUP BY regionId TOP 10000");
    }

    @Test
    public void testPercentileAggregation()
    {
        testUnaryAggregationHelper((planBuilder, aggregationBuilder) -> aggregationBuilder.addAggregation(planBuilder.variable("agg"), getRowExpression("approx_percentile(fare, 0.10)", defaultSessionHolder)), "PERCENTILEEST10(fare)");
    }

    @Test
    public void testApproxDistinct()
    {
        testUnaryAggregationHelper((planBuilder, aggregationBuilder) -> aggregationBuilder.addAggregation(planBuilder.variable("agg"), getRowExpression("approx_distinct(fare)", defaultSessionHolder)), "DISTINCTCOUNTHLL(fare)");
    }

    @Test
    public void testAggWithUDFInGroupBy()
    {
        LinkedHashMap<String, String> aggProjection = new LinkedHashMap<>();
        aggProjection.put("date", "date_trunc('day', cast(from_unixtime(secondssinceepoch - 50) AS TIMESTAMP))");
        PlanNode justDate = buildPlan(planBuilder -> project(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare), aggProjection, defaultSessionHolder));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justDate).singleGroupingSet(new VariableReferenceExpression("date", TIMESTAMP)).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder))),
                "SELECT count(*) FROM realtimeOnly GROUP BY dateTimeConvert(SUB(secondsSinceEpoch, 50), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS') TOP 10000");
        aggProjection.put("city", "city");
        PlanNode newScanWithCity = buildPlan(planBuilder -> project(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare), aggProjection, defaultSessionHolder));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(newScanWithCity).singleGroupingSet(new VariableReferenceExpression("date", TIMESTAMP), variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder))),
                "SELECT count(*) FROM realtimeOnly GROUP BY dateTimeConvert(SUB(secondsSinceEpoch, 50), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS'), city TOP 10000");
    }

    @Test
    public void testMultipleAggregatesWithOutGroupBy()
    {
        Map<String, String> outputVariables = ImmutableMap.of("agg", "count(*)", "min", "min(fare)");
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).globalGrouping().addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("min"), getRowExpression("min(fare)", defaultSessionHolder))),
                "SELECT __expressions__ FROM realtimeOnly",
                defaultSessionHolder,
                outputVariables);
        testPQL(
                planBuilder -> planBuilder.limit(50L, planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).globalGrouping().addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("min"), getRowExpression("min(fare)", defaultSessionHolder)))),
                "SELECT __expressions__ FROM realtimeOnly",
                defaultSessionHolder,
                outputVariables);
    }

    @Test
    public void testMultipleAggregatesWhenAllowed()
    {
        helperTestMultipleAggregatesWithGroupBy(new PinotConfig().setAllowMultipleAggregations(true));
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testMultipleAggregatesNotAllowed()
    {
        helperTestMultipleAggregatesWithGroupBy(pinotConfig);
    }

    private void helperTestMultipleAggregatesWithGroupBy(PinotConfig givenPinotConfig)
    {
        Map<String, String> outputVariables = ImmutableMap.of("agg", "count(*)", "min", "min(fare)");
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPQL(
                givenPinotConfig,
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("min"), getRowExpression("min(fare)", defaultSessionHolder))),
                "SELECT __expressions__ FROM realtimeOnly GROUP BY city TOP 10000",
                defaultSessionHolder,
                outputVariables);
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testMultipleAggregateGroupByWithLimitFails()
    {
        Map<String, String> outputVariables = ImmutableMap.of("agg", "count(*)", "min", "min(fare)");
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPQL(
                planBuilder -> planBuilder.limit(50L, planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("min"), getRowExpression("min(fare)", defaultSessionHolder)))),
                "SELECT __expressions__ FROM realtimeOnly GROUP BY city TOP 50", defaultSessionHolder, outputVariables);
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testForbiddenProjectionOutsideOfAggregation()
    {
        LinkedHashMap<String, String> projections = new LinkedHashMap<>(ImmutableMap.of("hour", "date_trunc('hour', from_unixtime(secondssinceepoch))", "regionid", "regionid"));
        PlanNode plan = buildPlan(planBuilder -> limit(planBuilder, 10, project(planBuilder, tableScan(planBuilder, pinotTable, secondsSinceEpoch, regionId), projections, defaultSessionHolder)));
        testPQL(pinotConfig, plan, "Should fail", defaultSessionHolder, ImmutableMap.of());
    }

    @Test
    public void testSimpleSelectWithTopN()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, regionId, city, fare);
        TopNNode topNFare = topN(planBuilder, 50L, ImmutableList.of("fare"), ImmutableList.of(false), tableScanNode);
        testPQL(pinotConfig,
                topNFare,
                "SELECT regionId, city, fare FROM realtimeOnly ORDER BY fare DESC LIMIT 50",
                defaultSessionHolder,
                ImmutableMap.of());
        TopNNode topnFareAndCity = topN(planBuilder, 50L, ImmutableList.of("fare", "city"), ImmutableList.of(true, false), tableScanNode);
        testPQL(pinotConfig,
                topnFareAndCity,
                "SELECT regionId, city, fare FROM realtimeOnly ORDER BY fare, city DESC LIMIT 50",
                defaultSessionHolder,
                ImmutableMap.of());
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testAggregationWithOrderByPushDownInTopN()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, city, fare);
        AggregationNode agg = planBuilder.aggregation(aggBuilder -> aggBuilder.source(tableScanNode).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("sum(fare)", defaultSessionHolder)));
        TopNNode topN = new TopNNode(planBuilder.getIdAllocator().getNextId(), agg, 50L, new OrderingScheme(ImmutableList.of(new Ordering(variable("city"), SortOrder.DESC_NULLS_FIRST))), TopNNode.Step.FINAL);
        testPQL(pinotConfig, topN, "", defaultSessionHolder, ImmutableMap.of());
    }
}
