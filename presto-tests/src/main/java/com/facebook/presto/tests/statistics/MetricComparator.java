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
package com.facebook.presto.tests.statistics;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class MetricComparator
{
    public Set<MetricComparison<?>> getMetricComparisons(String query, QueryRunner runner, Set<Metric<?>> metrics)
    {
        if (runner instanceof DistributedQueryRunner) {
            return getMetricComparisonsDistributed(query, (DistributedQueryRunner) runner, metrics);
        }
        else if (runner instanceof LocalQueryRunner) {
            return getMetricComparisonsLocal(query, (LocalQueryRunner) runner, metrics);
        }
        else {
            throw new IllegalArgumentException("only local and distributed runner supported");
        }
    }

    private Set<MetricComparison<?>> getMetricComparisonsDistributed(String query, DistributedQueryRunner runner, Set<Metric<?>> metrics)
    {
        String queryId = runner.executeWithQueryId(runner.getDefaultSession(), query).getQueryId();
        Plan queryPlan = runner.getQueryPlan(new QueryId(queryId));
        StageInfo stageInfo = runner.getQueryInfo(new QueryId(queryId)).getOutputStage().get();
        OutputNode outputNode = (OutputNode) stageInfo.getPlan().getRoot();

        return getMetricComparisons(query, runner, queryPlan, outputNode, metrics);
    }

    private Set<MetricComparison<?>> getMetricComparisonsLocal(String query, LocalQueryRunner runner, Set<Metric<?>> metrics)
    {
        Plan queryPlan = inTransaction(runner, (session) -> runner.createPlan(session, query));
        OutputNode outputNode = (OutputNode) queryPlan.getRoot();
        return getMetricComparisons(query, runner, queryPlan, outputNode, metrics);
    }

    private <T> T inTransaction(QueryRunner runner, Function<Session, T> transactionSessionConsumer)
    {
        return transaction(runner.getTransactionManager(), runner.getAccessControl())
                .singleStatement()
                .execute(runner.getDefaultSession(), session -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    session.getCatalog().ifPresent(catalog -> runner.getMetadata().getCatalogHandle(session, catalog));
                    return transactionSessionConsumer.apply(session);
                });
    }

    private Set<MetricComparison<?>> getMetricComparisons(String query, QueryRunner runner, Plan queryPlan, OutputNode outputNode, Set<Metric<?>> metrics)
    {
        StatsContext statsContext = buildStatsContext(queryPlan, outputNode);
        List<Metric<?>> metricsList = ImmutableList.copyOf(metrics);
        List<Optional<?>> actualValues = getActualValues(metricsList, query, runner, statsContext);
        List<Optional<?>> estimatedValues = getEstimatedValues(metricsList, queryPlan.getPlanNodeStats().get(outputNode.getId()), statsContext);

        ImmutableSet.Builder<MetricComparison<?>> metricComparisons = ImmutableSet.builder();
        for (int i = 0; i < metricsList.size(); ++i) {
            metricComparisons.add(new MetricComparison(
                    outputNode,
                    metricsList.get(i),
                    estimatedValues.get(i),
                    actualValues.get(i)));
        }
        return metricComparisons.build();
    }

    private StatsContext buildStatsContext(Plan queryPlan, OutputNode outputNode)
    {
        ImmutableMap.Builder<String, Symbol> columnSymbols = ImmutableMap.builder();
        for (int columnId = 0; columnId < outputNode.getColumnNames().size(); ++columnId) {
            columnSymbols.put(outputNode.getColumnNames().get(columnId), outputNode.getOutputSymbols().get(columnId));
        }
        return new StatsContext(columnSymbols.build(), queryPlan.getTypes());
    }

    private List<Optional<?>> getActualValues(List<Metric<?>> metrics, String query, QueryRunner runner, StatsContext statsContext)
    {
        String statsQuery = "SELECT "
                + metrics.stream().map(Metric::getComputingAggregationSql).collect(joining(","))
                + " FROM (" + query + ")";

        MaterializedRow actualValuesRow = getOnlyElement(runner.execute(statsQuery).getMaterializedRows());

        ImmutableList.Builder<Optional<?>> actualValues = ImmutableList.builder();
        for (int i = 0; i < metrics.size(); ++i) {
            actualValues.add(metrics.get(i).getValueFromAggregationQuery(actualValuesRow, i, statsContext));
        }
        return actualValues.build();
    }

    private List<Optional<?>> getEstimatedValues(List<Metric<?>> metrics, PlanNodeStatsEstimate outputNodeStatisticsEstimates, StatsContext statsContext)
    {
        return metrics.stream()
                .map(metric -> metric.getValueFromPlanNodeEstimate(outputNodeStatisticsEstimates, statsContext))
                .collect(toList());
    }
}
