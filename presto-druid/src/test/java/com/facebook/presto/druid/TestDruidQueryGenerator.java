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
package com.facebook.presto.druid;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class TestDruidQueryGenerator
        extends TestDruidQueryBase
{
    private static final SessionHolder defaultSessionHolder = new SessionHolder();
    private static final DruidTableHandle druidTable = realtimeOnlyTable;

    private void testDQL(
            DruidConfig givenDruidConfig,
            Function<PlanBuilder, PlanNode> planBuilderConsumer,
            String expectedDQL, SessionHolder sessionHolder,
            Map<String, String> outputVariables)
    {
        PlanNode planNode = planBuilderConsumer.apply(createPlanBuilder(sessionHolder));
        testDQL(givenDruidConfig, planNode, expectedDQL, sessionHolder, outputVariables);
    }

    private void testDQL(
            DruidConfig givenDruidConfig,
            PlanNode planNode,
            String expectedDQL,
            SessionHolder sessionHolder,
            Map<String, String> outputVariables)
    {
        DruidQueryGenerator.DruidQueryGeneratorResult druidQueryGeneratorResult = new DruidQueryGenerator(typeManager, functionMetadataManager, standardFunctionResolution).generate(planNode, sessionHolder.getConnectorSession()).get();
        if (expectedDQL.contains("__expressions__")) {
            String expressions = planNode.getOutputVariables().stream().map(v -> outputVariables.get(v.getName())).filter(v -> v != null).collect(Collectors.joining(", "));
            expectedDQL = expectedDQL.replace("__expressions__", expressions);
        }
        assertEquals(druidQueryGeneratorResult.getGeneratedDql().getDql(), expectedDQL);
    }

    private void testDQL(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedDQL, SessionHolder sessionHolder, Map<String, String> outputVariables)
    {
        testDQL(druidConfig, planBuilderConsumer, expectedDQL, sessionHolder, outputVariables);
    }

    private void testDQL(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedDQL, SessionHolder sessionHolder)
    {
        testDQL(planBuilderConsumer, expectedDQL, sessionHolder, ImmutableMap.of());
    }

    private void testDQL(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedDQL)
    {
        testDQL(planBuilderConsumer, expectedDQL, defaultSessionHolder);
    }

    private PlanNode buildPlan(Function<PlanBuilder, PlanNode> consumer)
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        return consumer.apply(planBuilder);
    }

    @Test
    public void testSimpleSelectStar()
    {
        testDQL(
                planBuilder -> limit(planBuilder, 50L, tableScan(planBuilder, druidTable, regionId, city, fare, secondsSinceEpoch)),
                "SELECT regionId, city, fare, secondsSinceEpoch FROM realtimeOnly LIMIT 50");
        testDQL(
                planBuilder -> limit(planBuilder, 10L, tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch)),
                "SELECT regionId, secondsSinceEpoch FROM realtimeOnly LIMIT 10");
    }

    @Test
    public void testSimpleSelectWithFilterLimit()
    {
        testDQL(
                planBuilder -> limit(
                                    planBuilder,
                                    30L,
                                    project(
                                            planBuilder,
                                            filter(
                                                    planBuilder,
                                                    tableScan(planBuilder, druidTable, regionId, city, fare, secondsSinceEpoch),
                                                    getRowExpression("secondssinceepoch > 20", defaultSessionHolder)),
                                            ImmutableList.of("city", "secondssinceepoch"))),
                "SELECT city, secondsSinceEpoch FROM realtimeOnly WHERE (secondsSinceEpoch > 20) LIMIT 30");
    }

    @Test
    public void testCountStar()
    {
        BiConsumer<PlanBuilder, PlanBuilder.AggregationBuilder> aggregationFunctionBuilder = (planBuilder, aggregationBuilder) -> aggregationBuilder.addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder));
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode filter = buildPlan(planBuilder -> filter(planBuilder, tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare), getRowExpression("fare > 3", defaultSessionHolder)));
        PlanNode anotherFilter = buildPlan(planBuilder -> filter(planBuilder, tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare), getRowExpression("secondssinceepoch between 200 and 300 and regionid >= 40", defaultSessionHolder)));
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).globalGrouping())),
                "SELECT count(*) FROM realtimeOnly");
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(filter).globalGrouping())),
                "SELECT count(*) FROM realtimeOnly WHERE (fare > 3)");
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(filter).singleGroupingSet(variable("regionid")))),
                "SELECT regionId, count(*) FROM realtimeOnly WHERE (fare > 3) GROUP BY regionId");
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).singleGroupingSet(variable("regionid")))),
                "SELECT regionId, count(*) FROM realtimeOnly GROUP BY regionId");
        testDQL(
                planBuilder -> limit(planBuilder, 5L, planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).singleGroupingSet(variable("regionid"))))),
                "SELECT regionId, count(*) FROM realtimeOnly GROUP BY regionId LIMIT 5");
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(anotherFilter).singleGroupingSet(variable("regionid"), variable("city")))),
                "SELECT regionId, city, count(*) FROM realtimeOnly WHERE ((secondsSinceEpoch BETWEEN 200 AND 300) AND (regionId >= 40)) GROUP BY regionId, city");
    }

    @Test
    public void testDistinctSelection()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare));
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("regionid"))),
                "SELECT regionId, count(*) FROM realtimeOnly GROUP BY regionId");
    }

    @Test
    public void testDistinctCountPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode distinctAggregation = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("regionid"))));
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(distinctAggregation).globalGrouping().addAggregation(variable("regionid"), getRowExpression("count(regionid)", defaultSessionHolder))),
                "SELECT count ( distinct regionId) FROM realtimeOnly");
    }

    @Test
    public void testDistinctCountGroupByPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode distinctAggregation = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city"), variable("regionid"))));
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(distinctAggregation).singleGroupingSet(variable("city")).addAggregation(variable("regionid"), getRowExpression("count(regionid)", defaultSessionHolder))),
                "SELECT city, count ( distinct regionId) FROM realtimeOnly GROUP BY city");
    }
}
