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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.OptionalDouble;

import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

final class MetricComparator
{
    private MetricComparator() {}

    static List<MetricComparison> getMetricComparisons(String query, QueryRunner runner, List<Metric> metrics)
    {
        List<OptionalDouble> estimatedValues = getEstimatedValues(metrics, query, runner);
        List<OptionalDouble> actualValues = getActualValues(metrics, query, runner);

        ImmutableList.Builder<MetricComparison> metricComparisons = ImmutableList.builder();
        for (int i = 0; i < metrics.size(); ++i) {
            //noinspection unchecked
            metricComparisons.add(new MetricComparison(
                    metrics.get(i),
                    estimatedValues.get(i),
                    actualValues.get(i)));
        }
        return metricComparisons.build();
    }

    private static List<OptionalDouble> getEstimatedValues(List<Metric> metrics, String query, QueryRunner runner)
    {
        return transaction(runner.getTransactionManager(), runner.getAccessControl())
                .singleStatement()
                .execute(runner.getDefaultSession(), (Session session) -> getEstimatedValuesInternal(metrics, query, runner, session));
    }

    private static List<OptionalDouble> getEstimatedValuesInternal(List<Metric> metrics, String query, QueryRunner runner, Session session)
    // TODO inline back this method
    {
        Plan queryPlan = runner.createPlan(session, query, WarningCollector.NOOP);
        OutputNode outputNode = (OutputNode) queryPlan.getRoot();
        PlanNodeStatsEstimate outputNodeStats = queryPlan.getStatsAndCosts().getStats().getOrDefault(queryPlan.getRoot().getId(), PlanNodeStatsEstimate.unknown());
        StatsContext statsContext = buildStatsContext(queryPlan, outputNode);
        return getEstimatedValues(metrics, outputNodeStats, statsContext);
    }

    private static StatsContext buildStatsContext(Plan queryPlan, OutputNode outputNode)
    {
        ImmutableMap.Builder<String, VariableReferenceExpression> columnVariables = ImmutableMap.builder();
        for (int columnId = 0; columnId < outputNode.getColumnNames().size(); ++columnId) {
            columnVariables.put(outputNode.getColumnNames().get(columnId), outputNode.getOutputVariables().get(columnId));
        }
        return new StatsContext(columnVariables.build());
    }

    private static List<OptionalDouble> getActualValues(List<Metric> metrics, String query, QueryRunner runner)
    {
        String statsQuery = "SELECT "
                + metrics.stream().map(Metric::getComputingAggregationSql).collect(joining(","))
                + " FROM (" + query + ")";
        try {
            MaterializedRow actualValuesRow = getOnlyElement(runner.execute(statsQuery).getMaterializedRows());

            ImmutableList.Builder<OptionalDouble> actualValues = ImmutableList.builder();
            for (int i = 0; i < metrics.size(); ++i) {
                actualValues.add(metrics.get(i).getValueFromAggregationQueryResult(actualValuesRow.getField(i)));
            }
            return actualValues.build();
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to execute query to compute actual values: %s", statsQuery), e);
        }
    }

    private static List<OptionalDouble> getEstimatedValues(List<Metric> metrics, PlanNodeStatsEstimate outputNodeStatisticsEstimates, StatsContext statsContext)
    {
        return metrics.stream()
                .map(metric -> metric.getValueFromPlanNodeEstimate(outputNodeStatisticsEstimates, statsContext))
                .collect(toImmutableList());
    }
}
