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
        DruidQueryGenerator.DruidQueryGeneratorResult druidQueryGeneratorResult = new DruidQueryGenerator(givenDruidConfig, typeManager, functionMetadataManager, standardFunctionResolution).generate(planNode, sessionHolder.getConnectorSession()).get();
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
        testDQL(planBuilder -> tableScan(planBuilder, druidTable, regionId, city, fare, secondsSinceEpoch),
                "SELECT regionId, city, fare, secondsSinceEpoch FROM realtimeOnly");
        testDQL(planBuilder -> tableScan(planBuilder, druidTable, regionId, secondsSinceEpoch),
                "SELECT regionId, secondsSinceEpoch FROM realtimeOnly");
    }

    @Test
    public void testSimpleSelectWithFilter()
    {
        testDQL(planBuilder -> project(planBuilder, filter(planBuilder, tableScan(planBuilder, druidTable, regionId, city, fare, secondsSinceEpoch), getRowExpression("secondssinceepoch > 20", defaultSessionHolder)), ImmutableList.of("city", "secondssinceepoch")),
                "SELECT city, secondsSinceEpoch FROM realtimeOnly WHERE (secondsSinceEpoch > 20)");
    }
}
