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
        DruidQueryGenerator.DruidQueryGeneratorResult druidQueryGeneratorResult = new DruidQueryGenerator(functionAndTypeManager, functionAndTypeManager, standardFunctionResolution).generate(planNode, sessionHolder.getConnectorSession()).get();
        if (expectedDQL.contains("__expressions__")) {
            String expressions = planNode.getOutputVariables().stream().map(v -> outputVariables.get(v.getName())).filter(v -> v != null).collect(Collectors.joining(", "));
            expectedDQL = expectedDQL.replace("__expressions__", expressions);
        }
        String generateDQL = druidQueryGeneratorResult.getGeneratedDql().getDql();
        generateDQL = generateDQL.replaceAll("\\\\\"", "");
        assertEquals(generateDQL, expectedDQL);
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
                "SELECT \"region.Id\", \"city\", \"fare\", \"secondsSinceEpoch\" FROM \"realtimeOnly\" LIMIT 50");
        testDQL(
                planBuilder -> limit(planBuilder, 10L, tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch)),
                "SELECT \"region.Id\", \"secondsSinceEpoch\" FROM \"realtimeOnly\" LIMIT 10");
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
                "SELECT \"city\", \"secondsSinceEpoch\" FROM \"realtimeOnly\" WHERE (\"secondsSinceEpoch\" > 20) LIMIT 30");
    }

    @Test
    public void testCountStar()
    {
        BiConsumer<PlanBuilder, PlanBuilder.AggregationBuilder> aggregationFunctionBuilder = (planBuilder, aggregationBuilder) -> aggregationBuilder.addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder));
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode filter = buildPlan(planBuilder -> filter(planBuilder, tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare), getRowExpression("fare > 3", defaultSessionHolder)));
        PlanNode anotherFilter = buildPlan(planBuilder ->
                filter(planBuilder,
                        tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare),
                        getRowExpression("secondssinceepoch between 200 and 300 and \"region.id\" >= 40", defaultSessionHolder)));
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).globalGrouping())),
                "SELECT count(*) FROM \"realtimeOnly\"");
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(filter).globalGrouping())),
                "SELECT count(*) FROM \"realtimeOnly\" WHERE (\"fare\" > 3)");
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(filter).singleGroupingSet(variable("region.id")))),
                "SELECT \"region.Id\", count(*) FROM \"realtimeOnly\" WHERE (\"fare\" > 3) GROUP BY \"region.Id\"");
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).singleGroupingSet(variable("region.id")))),
                "SELECT \"region.Id\", count(*) FROM \"realtimeOnly\" GROUP BY \"region.Id\"");
        testDQL(
                planBuilder -> limit(planBuilder, 5L, planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).singleGroupingSet(variable("region.id"))))),
                "SELECT \"region.Id\", count(*) FROM \"realtimeOnly\" GROUP BY \"region.Id\" LIMIT 5");
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(anotherFilter).singleGroupingSet(variable("region.id"), variable("city")))),
                "SELECT \"region.Id\", \"city\", count(*) FROM \"realtimeOnly\" WHERE ((\"secondsSinceEpoch\" BETWEEN 200 AND 300) AND (\"region.Id\" >= 40)) GROUP BY \"region.Id\", \"city\"");
    }

    @Test
    public void testDistinctSelection()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare));
        testDQL(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("region.id"))),
                "SELECT \"region.Id\", count(*) FROM \"realtimeOnly\" GROUP BY \"region.Id\"");
    }

    @Test
    public void testDistinctCountPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode distinctAggregation = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("region.id"))));
        testDQL(
                planBuilder -> planBuilder.aggregation(
                        aggBuilder -> aggBuilder.source(distinctAggregation).globalGrouping().addAggregation(variable("region.id"),
                                getRowExpression("count(\"region.id\")", defaultSessionHolder))),
                "SELECT count ( distinct \"region.Id\") FROM \"realtimeOnly\"");
    }

    @Test
    public void testGroupByPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare));
        testDQL(
                planBuilder -> planBuilder.aggregation(
                        aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city"), variable("region.id"), variable("secondssinceepoch"))
                                .addAggregation(variable("totalfare"), getRowExpression("sum(\"fare\")", defaultSessionHolder))),
                "SELECT \"city\", \"region.Id\", \"secondsSinceEpoch\", sum(fare) FROM \"realtimeOnly\" GROUP BY \"city\", \"region.Id\", \"secondsSinceEpoch\"");
    }

    @Test
    public void testDistinctCountGroupByPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode distinctAggregation = buildPlan(planBuilder -> planBuilder.aggregation(
                aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city"), variable("region.id"))));
        testDQL(
                planBuilder -> planBuilder.aggregation(
                        aggBuilder -> aggBuilder.source(distinctAggregation).singleGroupingSet(variable("city"))
                                .addAggregation(variable("region.id"), getRowExpression("count(\"region.id\")", defaultSessionHolder))),
                "SELECT \"city\", count ( distinct \"region.Id\") FROM \"realtimeOnly\" GROUP BY \"city\"");
    }

    @Test
    public void testTimestampLiteralPushdown()
    {
        //the timezone of the session is Pacific/Apia UTC+13
        //the timezone of the connector session is UTC
        //so the time needs to be adjust for 13 hours if the timezone not specified
        testDQL(
                planBuilder -> project(
                        planBuilder,
                        filter(
                                planBuilder,
                                tableScan(planBuilder, druidTable, regionId, city, fare, datetime),
                                getRowExpression("datetime = timestamp '2016-06-26 19:00:00.000'", defaultSessionHolder)),
                        ImmutableList.of("city", "datetime")),
                "SELECT \"city\", \"datetime\" FROM \"realtimeOnly\" WHERE (\"datetime\" = TIMESTAMP '2016-06-26 06:00:00.000')");
        //test timestamp with timezone
        testDQL(
                planBuilder -> project(
                        planBuilder,
                        filter(
                                planBuilder,
                                tableScan(planBuilder, druidTable, regionId, city, fare, datetime),
                                getRowExpression("datetime > timestamp '2016-06-26 19:00:00.000 UTC'", defaultSessionHolder)),
                        ImmutableList.of("city", "datetime")),
                "SELECT \"city\", \"datetime\" FROM \"realtimeOnly\" WHERE (\"datetime\" > TIMESTAMP '2016-06-26 19:00:00.000')");
    }
}
