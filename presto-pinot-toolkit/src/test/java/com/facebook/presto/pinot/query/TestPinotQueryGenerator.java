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
import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.pinot.TestPinotQueryBase;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPinotQueryGenerator
        extends TestPinotQueryBase
{
    protected static final PinotTableHandle pinotTable = realtimeOnlyTable;

    protected SessionHolder defaultSessionHolder = getDefaultSessionHolder();

    public SessionHolder getDefaultSessionHolder()
    {
        return new SessionHolder(false, useSqlSyntax());
    }

    public boolean useSqlSyntax()
    {
        return false;
    }

    private void testPinotQuery(
            PinotConfig givenPinotConfig,
            Function<PlanBuilder, PlanNode> planBuilderConsumer,
            String expectedPinotQuery, SessionHolder sessionHolder,
            Map<String, String> outputVariables)
    {
        PlanNode planNode = planBuilderConsumer.apply(createPlanBuilder(sessionHolder));
        testPinotQuery(givenPinotConfig, planNode, expectedPinotQuery, sessionHolder, outputVariables);
    }

    protected void testPinotQuery(
            PinotConfig givenPinotConfig,
            PlanNode planNode,
            String expectedPinotQuery,
            SessionHolder sessionHolder,
            Map<String, String> outputVariables)
    {
        testPinotQuery(givenPinotConfig, planNode, ImmutableList.of(expectedPinotQuery), sessionHolder, outputVariables);
    }

    protected void testPinotQuery(
            PinotConfig givenPinotConfig,
            PlanNode planNode,
            List<String> expectedPinotQueries,
            SessionHolder sessionHolder,
            Map<String, String> outputVariables)
    {
        PinotQueryGenerator.PinotQueryGeneratorResult pinotQueryGeneratorResult = new PinotQueryGenerator(givenPinotConfig, functionAndTypeManager, functionAndTypeManager, standardFunctionResolution).generate(planNode, sessionHolder.getConnectorSession()).get();
        String pinotQuery = pinotQueryGeneratorResult.getGeneratedPinotQuery().getQuery();
        Set<String> expectedPinotQuerySet = new HashSet<>();
        for (String expectedPinotQuery : expectedPinotQueries) {
            if (expectedPinotQuery.contains("__expressions__")) {
                String expressions = planNode.getOutputVariables().stream().map(v -> outputVariables.get(v.getName())).filter(v -> v != null).collect(Collectors.joining(", "));
                expectedPinotQuery = expectedPinotQuery.replace("__expressions__", expressions);
            }
            expectedPinotQuerySet.add(expectedPinotQuery);
        }
        if (expectedPinotQuerySet.size() == 1) {
            assertEquals(pinotQuery, expectedPinotQuerySet.iterator().next());
        }
        assertTrue(expectedPinotQuerySet.contains(pinotQuery), String.format("Expected Generated PinotQuery: %s in the set: [%s]", pinotQuery, Arrays.toString(expectedPinotQuerySet.toArray(new String[0]))));
    }

    private void testPinotQuery(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPinotQuery, SessionHolder sessionHolder, Map<String, String> outputVariables)
    {
        testPinotQuery(pinotConfig, planBuilderConsumer, expectedPinotQuery, sessionHolder, outputVariables);
    }

    private void testPinotQuery(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPinotQuery, SessionHolder sessionHolder)
    {
        testPinotQuery(planBuilderConsumer, expectedPinotQuery, sessionHolder, ImmutableMap.of());
    }

    private void testPinotQuery(PinotConfig givenPinotConfig, Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPinotQuery)
    {
        testPinotQuery(givenPinotConfig, planBuilderConsumer, expectedPinotQuery, defaultSessionHolder, ImmutableMap.of());
    }

    private void testPinotQuery(PinotConfig givenPinotConfig, PlanNode planNode, String expectedPinotQuery)
    {
        testPinotQuery(givenPinotConfig, planNode, expectedPinotQuery, defaultSessionHolder, ImmutableMap.of());
    }

    private void testPinotQuery(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPinotQuery)
    {
        testPinotQuery(planBuilderConsumer, expectedPinotQuery, defaultSessionHolder);
    }

    protected PlanNode buildPlan(Function<PlanBuilder, PlanNode> consumer)
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        return consumer.apply(planBuilder);
    }

    private void testUnaryAggregationHelper(BiConsumer<PlanBuilder, PlanBuilder.AggregationBuilder> aggregationFunctionBuilder, String expectedAggOutput)
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode filter = buildPlan(planBuilder -> filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare), getRowExpression("fare > 3", defaultSessionHolder)));
        PlanNode anotherFilter = buildPlan(planBuilder -> filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare), getRowExpression("secondssinceepoch between 200 and 300 and regionid >= 40", defaultSessionHolder)));
        PlanNode filterWithMultiValue = buildPlan(planBuilder -> filter(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare, scores), getRowExpression("contains(scores, 100) OR contains(scores, 200)", defaultSessionHolder)));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).globalGrouping())),
                format("SELECT %s FROM realtimeOnly", getExpectedAggOutput(expectedAggOutput, "")));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(filter).globalGrouping())),
                format("SELECT %s FROM realtimeOnly WHERE (fare > 3)", getExpectedAggOutput(expectedAggOutput, "")));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(filter).singleGroupingSet(variable("regionid")))),
                format("SELECT %s FROM realtimeOnly WHERE (fare > 3) GROUP BY regionId %s 10000", getExpectedAggOutput(expectedAggOutput, "regionId"), getGroupByLimitKey()));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(justScan).singleGroupingSet(variable("regionid")))),
                format("SELECT %s FROM realtimeOnly GROUP BY regionId %s 10000", getExpectedAggOutput(expectedAggOutput, "regionId"), getGroupByLimitKey()));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(anotherFilter).singleGroupingSet(variable("regionid"), variable("city")))),
                format("SELECT %s FROM realtimeOnly WHERE ((secondsSinceEpoch BETWEEN 200 AND 300) AND (regionId >= 40)) GROUP BY regionId, city %s 10000", getExpectedAggOutput(expectedAggOutput, "regionId, city"), getGroupByLimitKey()));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(planBuilder, aggBuilder.source(filterWithMultiValue).singleGroupingSet(variable("regionid"), variable("city")))),
                format("SELECT %s FROM realtimeOnly WHERE ((scores = 100) OR (scores = 200)) GROUP BY regionId, city %s 10000", getExpectedAggOutput(expectedAggOutput, "regionId, city"), getGroupByLimitKey()));
    }

    protected String getGroupByLimitKey()
    {
        return "TOP";
    }

    protected String getExpectedAggOutput(String expectedAggOutput, String groupByColumns)
    {
        return expectedAggOutput;
    }

    @Test
    public void testSimpleSelectStar()
    {
        testPinotQuery(
                planBuilder -> limit(planBuilder, 50L, tableScan(planBuilder, pinotTable, regionId, city, fare, secondsSinceEpoch)),
                "SELECT regionId, city, fare, secondsSinceEpoch FROM realtimeOnly LIMIT 50");
        testPinotQuery(
                planBuilder -> limit(planBuilder, 50L, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch)),
                "SELECT regionId, secondsSinceEpoch FROM realtimeOnly LIMIT 50");
    }

    @Test
    public void testSimpleSelectWithFilterLimit()
    {
        testPinotQuery(
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
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(distinctAggregation).globalGrouping().addAggregation(variable("count_regionid"), getRowExpression("count(regionid)", defaultSessionHolder))),
                "SELECT DISTINCTCOUNT(regionId) FROM realtimeOnly");
    }

    @Test
    public void testDistinctCountPushdownWithVariableSuffix()
    {
        Map<VariableReferenceExpression, PinotColumnHandle> columnHandleMap = ImmutableMap.of(new VariableReferenceExpression("regionid_33", regionId.getDataType()), regionId);
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, columnHandleMap));
        PlanNode markDistinct = buildPlan(planBuilder -> markDistinct(planBuilder, variable("regionid$distinct_62"), ImmutableList.of(variable("regionid")), justScan));
        PlanNode aggregate = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(markDistinct).addAggregation(planBuilder.variable("count(regionid_33)"), getRowExpression("count(regionid_33)", defaultSessionHolder), Optional.empty(), Optional.empty(), false, Optional.of(variable("regionid$distinct_62"))).globalGrouping()));
        testPinotQuery(new PinotConfig().setAllowMultipleAggregations(true), planBuilder -> planBuilder.limit(10, aggregate), "SELECT DISTINCTCOUNT(regionId) FROM realtimeOnly");
    }

    @Test
    public void testDistinctCountGroupByPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode distinctAggregation = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city"), variable("regionid"))));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(distinctAggregation).singleGroupingSet(variable("city")).addAggregation(variable("count_regionid"), getRowExpression("count(regionid)", defaultSessionHolder))),
                String.format("SELECT %s FROM realtimeOnly GROUP BY city %s 10000", getExpectedAggOutput("DISTINCTCOUNT(regionId)", "city"), getGroupByLimitKey()));
    }

    @Test
    public void testDistinctCountWithOtherAggregationPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode markDistinct = buildPlan(planBuilder -> markDistinct(planBuilder, variable("regionid$distinct"), ImmutableList.of(variable("regionid")), justScan));
        PlanNode aggregate = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(markDistinct).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("count(regionid)"), getRowExpression("count(regionid)", defaultSessionHolder), Optional.empty(), Optional.empty(), false, Optional.of(variable("regionid$distinct"))).globalGrouping()));
        String expectedPinotQuery;
        if (aggregate.getOutputVariables().get(0).getName().equalsIgnoreCase("count(regionid)")) {
            expectedPinotQuery = "SELECT DISTINCTCOUNT(regionId), count(*) FROM realtimeOnly";
        }
        else {
            expectedPinotQuery = "SELECT count(*), DISTINCTCOUNT(regionId) FROM realtimeOnly";
        }
        testPinotQuery(new PinotConfig().setAllowMultipleAggregations(true), planBuilder -> planBuilder.limit(10, aggregate), expectedPinotQuery);
    }

    @Test
    public void testDistinctCountWithOtherAggregationGroupByPushdown()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode markDistinct = buildPlan(planBuilder -> markDistinct(planBuilder, variable("regionid$distinct"), ImmutableList.of(variable("regionid")), justScan));
        PlanNode aggregate = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(markDistinct).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("count(regionid)"), getRowExpression("count(regionid)", defaultSessionHolder), Optional.empty(), Optional.empty(), false, Optional.of(variable("regionid$distinct")))));
        String expectedPinotQuery;
        if (aggregate.getOutputVariables().get(1).getName().equalsIgnoreCase("count(regionid)")) {
            expectedPinotQuery = String.format("SELECT %s FROM realtimeOnly GROUP BY city %s 10000", getExpectedAggOutput("DISTINCTCOUNT(regionId), count(*)", "city"), getGroupByLimitKey());
        }
        else {
            expectedPinotQuery = String.format("SELECT %s FROM realtimeOnly GROUP BY city %s 10000", getExpectedAggOutput("count(*), DISTINCTCOUNT(regionId)", "city"), getGroupByLimitKey());
        }
        testPinotQuery(new PinotConfig().setAllowMultipleAggregations(true), aggregate, expectedPinotQuery);
    }

    @Test
    public void testDistinctSelection()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPinotQuery(
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
        testUnaryAggregationHelper((planBuilder, aggregationBuilder) -> aggregationBuilder.addAggregation(planBuilder.variable("agg"), getRowExpression("approx_distinct(fare, 0.1)", defaultSessionHolder)), "DISTINCTCOUNTHLL(fare, 6)");
        testUnaryAggregationHelper((planBuilder, aggregationBuilder) -> aggregationBuilder.addAggregation(planBuilder.variable("agg"), getRowExpression("approx_distinct(fare, 0.02)", defaultSessionHolder)), "DISTINCTCOUNTHLL(fare, 11)");
        testUnaryAggregationHelper((planBuilder, aggregationBuilder) -> aggregationBuilder.addAggregation(planBuilder.variable("agg"), getRowExpression("approx_distinct(fare, 0.01)", defaultSessionHolder)), "DISTINCTCOUNTHLL(fare, 13)");
        testUnaryAggregationHelper((planBuilder, aggregationBuilder) -> aggregationBuilder.addAggregation(planBuilder.variable("agg"), getRowExpression("approx_distinct(fare, 0.005)", defaultSessionHolder)), "DISTINCTCOUNTHLL(fare, 15)");
    }

    @Test
    public void testApproxDistinctWithInvalidParameters()
    {
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode approxPlanNode = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("approx_distinct(fare, 0)", defaultSessionHolder))));
        Optional<PinotQueryGenerator.PinotQueryGeneratorResult> generatedQuery =
                new PinotQueryGenerator(pinotConfig, functionAndTypeManager, functionAndTypeManager, standardFunctionResolution)
                        .generate(approxPlanNode, defaultSessionHolder.getConnectorSession());
        assertFalse(generatedQuery.isPresent());
        approxPlanNode = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("approx_distinct(fare, 0.004)", defaultSessionHolder))));
        generatedQuery =
                new PinotQueryGenerator(pinotConfig, functionAndTypeManager, functionAndTypeManager, standardFunctionResolution)
                        .generate(approxPlanNode, defaultSessionHolder.getConnectorSession());
        assertFalse(generatedQuery.isPresent());
        approxPlanNode = buildPlan(planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("approx_distinct(fare, 1)", defaultSessionHolder))));
        generatedQuery =
                new PinotQueryGenerator(pinotConfig, functionAndTypeManager, functionAndTypeManager, standardFunctionResolution)
                        .generate(approxPlanNode, defaultSessionHolder.getConnectorSession());
        assertFalse(generatedQuery.isPresent());
    }
    @Test
    public void testAggWithUDFInGroupBy()
    {
        LinkedHashMap<String, String> aggProjection = new LinkedHashMap<>();
        aggProjection.put("date", "date_trunc('day', cast(from_unixtime(secondssinceepoch - 50) AS TIMESTAMP))");
        PlanNode justDate = buildPlan(planBuilder -> project(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare), aggProjection, defaultSessionHolder));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justDate).singleGroupingSet(new VariableReferenceExpression("date", TIMESTAMP)).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder))),
                String.format("SELECT %s FROM realtimeOnly GROUP BY dateTimeConvert(SUB(secondsSinceEpoch, 50), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS') %s 10000", getExpectedAggOutput("count(*)", "dateTimeConvert(SUB(secondsSinceEpoch, 50), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS')"), getGroupByLimitKey()));
        aggProjection.put("city", "city");
        PlanNode newScanWithCity = buildPlan(planBuilder -> project(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare), aggProjection, defaultSessionHolder));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(newScanWithCity).singleGroupingSet(new VariableReferenceExpression("date", TIMESTAMP), variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder))),
                String.format("SELECT %s FROM realtimeOnly GROUP BY dateTimeConvert(SUB(secondsSinceEpoch, 50), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS'), city %s 10000", getExpectedAggOutput("count(*)", "dateTimeConvert(SUB(secondsSinceEpoch, 50), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS'), city"), getGroupByLimitKey()));
    }

    @Test
    public void testAggWithArrayFunctionsInGroupBy()
    {
        LinkedHashMap<String, String> aggProjection = new LinkedHashMap<>();
        aggProjection.put("array_max_0", "array_max(scores)");
        PlanNode justMaxScores = buildPlan(planBuilder -> project(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare, scores), aggProjection, defaultSessionHolder));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justMaxScores).singleGroupingSet(new VariableReferenceExpression("array_max_0", DOUBLE)).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder))),
                String.format("SELECT %s FROM realtimeOnly GROUP BY arrayMax(scores) %s 10000", getExpectedAggOutput("count(*)", "arrayMax(scores)"), getGroupByLimitKey()));
        aggProjection.put("city", "city");
        PlanNode newScanWithCity = buildPlan(planBuilder -> project(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare, scores), aggProjection, defaultSessionHolder));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(newScanWithCity).singleGroupingSet(new VariableReferenceExpression("array_max_0", DOUBLE), variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder))),
                String.format("SELECT %s FROM realtimeOnly GROUP BY arrayMax(scores), city %s 10000", getExpectedAggOutput("count(*)", "arrayMax(scores), city"), getGroupByLimitKey()));
    }

    private void testAggWithArrayFunction(String functionVariable, String prestoFunctionExpression, String pinotFunctionExpression)
    {
        LinkedHashMap<String, String> aggProjection = new LinkedHashMap<>();
        aggProjection.put("city", "city");
        aggProjection.put(functionVariable, prestoFunctionExpression);
        PlanNode aggregationPlanNode = buildPlan(planBuilder -> project(planBuilder, tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare, scores), aggProjection, defaultSessionHolder));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(aggregationPlanNode).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression(String.format("sum(%s)", functionVariable), defaultSessionHolder))),
                String.format("SELECT %s FROM realtimeOnly GROUP BY city %s 10000", getExpectedAggOutput(String.format("sum(%s)", pinotFunctionExpression), "city"), getGroupByLimitKey()));
    }

    @Test
    public void testAggWithArrayFunctions()
    {
        testAggWithArrayFunction("array_min_0", "array_min(scores)", "arrayMin(scores)");
        testAggWithArrayFunction("array_max_0", "array_max(scores)", "arrayMax(scores)");
        testAggWithArrayFunction("array_sum_0", "reduce(scores, cast(0 as double), (s, x) -> s + x, s -> s)", "arraySum(scores)");
        testAggWithArrayFunction("array_average_0", "reduce(scores, CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), (s,x) -> CAST(ROW(x + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), s -> IF(s.count = 0, NULL, s.sum / s.count))", "arrayAverage(scores)");
    }

    @Test
    public void testMultipleAggregatesWithOutGroupBy()
    {
        Map<String, String> outputVariables = ImmutableMap.of("agg", "count(*)", "min", "min(fare)");
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPinotQuery(
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).globalGrouping().addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("min"), getRowExpression("min(fare)", defaultSessionHolder))),
                "SELECT __expressions__ FROM realtimeOnly",
                defaultSessionHolder,
                outputVariables);
        testPinotQuery(
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
        helperTestMultipleAggregatesWithGroupBy(new PinotConfig().setAllowMultipleAggregations(false));
    }

    protected void helperTestMultipleAggregatesWithGroupBy(PinotConfig givenPinotConfig)
    {
        Map<String, String> outputVariables = ImmutableMap.of("agg", "count(*)", "min", "min(fare)");
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPinotQuery(
                givenPinotConfig,
                planBuilder -> planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("min"), getRowExpression("min(fare)", defaultSessionHolder))),
                String.format("SELECT %s FROM realtimeOnly GROUP BY city %s 10000", getExpectedAggOutput("__expressions__", "city"), getGroupByLimitKey()),
                defaultSessionHolder,
                outputVariables);
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testMultipleAggregateGroupByWithLimitFails()
    {
        Map<String, String> outputVariables = ImmutableMap.of("agg", "count(*)", "min", "min(fare)");
        PlanNode justScan = buildPlan(planBuilder -> tableScan(planBuilder, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPinotQuery(
                planBuilder -> planBuilder.limit(50L, planBuilder.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(planBuilder.variable("min"), getRowExpression("min(fare)", defaultSessionHolder)))),
                String.format("SELECT %s FROM realtimeOnly GROUP BY city %s 50", getExpectedAggOutput("__expressions__", "city"), getGroupByLimitKey()),
                defaultSessionHolder,
                outputVariables);
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testForbiddenProjectionOutsideOfAggregation()
    {
        LinkedHashMap<String, String> projections = new LinkedHashMap<>(ImmutableMap.of("hour", "date_trunc('hour', from_unixtime(secondssinceepoch))", "regionid", "regionid"));
        PlanNode plan = buildPlan(planBuilder -> limit(planBuilder, 10, project(planBuilder, tableScan(planBuilder, pinotTable, secondsSinceEpoch, regionId), projections, defaultSessionHolder)));
        testPinotQuery(pinotConfig, plan, "Should fail", defaultSessionHolder, ImmutableMap.of());
    }

    @Test
    public void testSimpleSelectWithTopN()
    {
        pinotConfig.setPushdownTopNBrokerQueries(true);
        SessionHolder sessionHolder = new SessionHolder(pinotConfig);
        PlanBuilder planBuilder = createPlanBuilder(new SessionHolder(pinotConfig));
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, regionId, city, fare);
        TopNNode topNFare = topN(planBuilder, 50L, ImmutableList.of("fare"), ImmutableList.of(false), tableScanNode);
        testPinotQuery(
                pinotConfig,
                topNFare,
                "SELECT regionId, city, fare FROM realtimeOnly ORDER BY fare DESC LIMIT 50",
                sessionHolder,
                ImmutableMap.of());
        TopNNode topnFareAndCity = topN(planBuilder, 50L, ImmutableList.of("fare", "city"), ImmutableList.of(true, false), tableScanNode);
        testPinotQuery(
                pinotConfig,
                topnFareAndCity,
                "SELECT regionId, city, fare FROM realtimeOnly ORDER BY fare, city DESC LIMIT 50",
                sessionHolder,
                ImmutableMap.of());
        ProjectNode projectNode = project(planBuilder, topnFareAndCity, ImmutableList.of("regionid", "city"));
        testPinotQuery(pinotConfig,
                projectNode,
                "SELECT regionId, city FROM realtimeOnly ORDER BY fare, city DESC LIMIT 50",
                sessionHolder,
                ImmutableMap.of());
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testAggregationWithOrderByPushDownInTopN()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(planBuilder, pinotTable, city, fare);
        AggregationNode agg = planBuilder.aggregation(aggBuilder -> aggBuilder.source(tableScanNode).singleGroupingSet(variable("city")).addAggregation(planBuilder.variable("agg"), getRowExpression("sum(fare)", defaultSessionHolder)));
        TopNNode topN = new TopNNode(planBuilder.getIdAllocator().getNextId(), agg, 50L, new OrderingScheme(ImmutableList.of(new Ordering(variable("city"), SortOrder.DESC_NULLS_FIRST))), TopNNode.Step.FINAL);
        testPinotQuery(pinotConfig, topN, "", defaultSessionHolder, ImmutableMap.of());
    }

    @Test
    public void testDistinctLimitPushdown()
    {
        PlanBuilder planBuilder = createPlanBuilder(defaultSessionHolder);
        DistinctLimitNode distinctLimitNode = distinctLimit(
                planBuilder,
                ImmutableList.of(new VariableReferenceExpression("regionid", BIGINT)),
                50L,
                tableScan(planBuilder, pinotTable, regionId));
        testPinotQuery(
                pinotConfig,
                distinctLimitNode,
                String.format("SELECT %s FROM realtimeOnly GROUP BY regionId %s 50", getExpectedDistinctOutput("regionId"), getGroupByLimitKey()),
                defaultSessionHolder,
                ImmutableMap.of());

        planBuilder = createPlanBuilder(defaultSessionHolder);
        distinctLimitNode = distinctLimit(
                planBuilder,
                ImmutableList.of(
                        new VariableReferenceExpression("regionid", BIGINT),
                        new VariableReferenceExpression("city", VARCHAR)),
                50L,
                tableScan(planBuilder, pinotTable, regionId, city));
        testPinotQuery(
                pinotConfig,
                distinctLimitNode,
                String.format("SELECT %s FROM realtimeOnly GROUP BY regionId, city %s 50", getExpectedDistinctOutput("regionId, city"), getGroupByLimitKey()),
                defaultSessionHolder,
                ImmutableMap.of());
    }

    protected String getExpectedDistinctOutput(String groupKeys)
    {
        return "count(*)";
    }
}
