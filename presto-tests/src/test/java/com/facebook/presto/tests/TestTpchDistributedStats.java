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
package com.facebook.presto.tests;

import com.facebook.presto.execution.QueryPlan;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.tests.statistics.Metric;
import com.facebook.presto.tests.statistics.MetricComparator;
import com.facebook.presto.tests.statistics.MetricComparison;
import com.facebook.presto.tpch.ColumnNaming;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.tests.statistics.MetricComparison.Result.DIFFER;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.MATCH;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_BASELINE;
import static com.facebook.presto.tests.statistics.MetricComparison.Result.NO_ESTIMATE;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunnerWithoutCatalogs;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.groupingBy;
import static org.testng.Assert.assertEquals;

public class TestTpchDistributedStats
{
    public static final int NUMBER_OF_TPCH_QUERIES = 22;

    DistributedQueryRunner runner;

    public TestTpchDistributedStats()
            throws Exception
    {
        runner = createQueryRunnerWithoutCatalogs(emptyMap(), emptyMap());
        runner.createCatalog("tpch", "tpch", ImmutableMap.of(
                "tpch.column-naming", ColumnNaming.STANDARD.name()
        ));
    }

    @Test
    void testEstimateForSimpleQuery()
    {
        String queryId = executeQuery("SELECT * FROM NATION");

        QueryPlan queryPlan = getQueryPlan(queryId);

        MetricComparison rootOutputRowCountComparison = getRootOutputRowCountComparison(queryId, queryPlan);
        assertEquals(rootOutputRowCountComparison.result(), MATCH);
    }

    private MetricComparison getRootOutputRowCountComparison(String queryId, QueryPlan queryPlan)
    {
        List<MetricComparison> comparisons = new MetricComparator().getMetricComparisons(queryPlan, getOutputStageInfo(queryId));
        return comparisons.stream()
                .filter(comparison -> comparison.getMetric().equals(Metric.OUTPUT_ROW_COUNT))
                .filter(comparison -> comparison.getPlanNode().equals(queryPlan.getPlan().getRoot()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No comparison for root node found"));
    }

    /**
     * This is a development tool for manual inspection of differences between
     * cost estimates and actual execution costs. Its outputs need to be inspected
     * manually because at this point no sensible assertions can be formulated
     * for the entirety of TPCH queries.
     */
    @Test
    void testCostEstimatesVsRealityDifferences()
    {
        IntStream.rangeClosed(1, NUMBER_OF_TPCH_QUERIES)
                .filter(i -> i != 15) //query 15 creates a view, which TPCH connector does not support.
                .forEach(i -> summarizeQuery(i, getTpchQuery(i)));
    }

    private String getTpchQuery(int i)
    {
        try {
            String queryClassPath = "/io/airlift/tpch/queries/q" + i + ".sql";
            return Resources.toString(getClass().getResource(queryClassPath), Charset.defaultCharset());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private QueryPlan getQueryPlan(String queryId)
    {
        return runner.getQueryPlan(new QueryId(queryId));
    }

    private void summarizeQuery(int queryNumber, String query)
    {
        String queryId = executeQuery(query);
        QueryPlan queryPlan = getQueryPlan(queryId);

        List<PlanNode> allPlanNodes = new PlanNodeSearcher(queryPlan.getPlan().getRoot()).findAll();

        System.out.println(format("Query TPCH [%s] produces [%s] plan nodes.\n", queryNumber, allPlanNodes.size()));

        List<MetricComparison> comparisons = new MetricComparator().getMetricComparisons(queryPlan, getOutputStageInfo(queryId));

        Map<Metric, Map<MetricComparison.Result, List<MetricComparison>>> metricSummaries =
                comparisons.stream()
                        .collect(groupingBy(MetricComparison::getMetric, groupingBy(MetricComparison::result)));

        metricSummaries.forEach((metricName, resultSummaries) -> {
            System.out.println(format("Summary for metric [%s]", metricName));
            outputSummary(resultSummaries, NO_ESTIMATE);
            outputSummary(resultSummaries, NO_BASELINE);
            outputSummary(resultSummaries, DIFFER);
            outputSummary(resultSummaries, MATCH);
            System.out.println();
        });

        System.out.println("Detailed results:\n");

        comparisons.forEach(System.out::println);
    }

    private String executeQuery(String query)
    {
        return runner.executeWithQueryId(runner.getDefaultSession(), query).getQueryId();
    }

    private StageInfo getOutputStageInfo(String queryId)
    {
        return runner.getQueryInfo(new QueryId(queryId)).getOutputStage().get();
    }

    private void outputSummary(Map<MetricComparison.Result, List<MetricComparison>> resultSummaries, MetricComparison.Result result)
    {
        System.out.println(format("[%s]\t-\t[%s]", result, resultSummaries.getOrDefault(result, emptyList()).size()));
    }
}
