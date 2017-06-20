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

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStats;
import com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.util.MoreMaps.mergeMaps;
import static com.google.common.collect.Maps.transformValues;
import static java.util.Arrays.asList;

public class MetricComparator
{
    private final List<Metric> metrics = asList(Metric.values());
    private final double tolerance = 0.1;

    public List<MetricComparison> getMetricComparisons(Plan queryPlan, StageInfo outputStageInfo)
    {
        return metrics.stream().flatMap(metric -> {
            Map<PlanNodeId, PlanNodeStatsEstimate> estimates = queryPlan.getPlanNodeStatsEstimates();
            Map<PlanNodeId, PlanNodeStatsEstimate> actuals = extractActualCosts(outputStageInfo);
            return estimates.entrySet().stream().map(entry -> {
                // todo refactor to stay in PlanNodeId domain ????
                PlanNode node = planNodeForId(queryPlan, entry.getKey());
                PlanNodeStatsEstimate estimate = entry.getValue();
                Optional<PlanNodeStatsEstimate> execution = Optional.ofNullable(actuals.get(node.getId()));
                return createMetricComparison(metric, node, estimate, execution);
            });
        }).collect(Collectors.toList());
    }

    private PlanNode planNodeForId(Plan queryPlan, PlanNodeId id)
    {
        return searchFrom(queryPlan.getRoot())
                .where(node -> node.getId().equals(id))
                .findOnlyElement();
    }

    private Map<PlanNodeId, PlanNodeStatsEstimate> extractActualCosts(StageInfo outputStageInfo)
    {
        Stream<Map<PlanNodeId, PlanNodeStats>> stagesStatsStream =
                getAllStages(Optional.of(outputStageInfo)).stream()
                        .map(PlanNodeStatsSummarizer::aggregatePlanNodeStats);

        Map<PlanNodeId, PlanNodeStats> mergedStats = mergeStats(stagesStatsStream);
        return transformValues(mergedStats, this::toPlanNodeStatsEstimate);
    }

    private Map<PlanNodeId, PlanNodeStats> mergeStats(Stream<Map<PlanNodeId, PlanNodeStats>> stagesStatsStream)
    {
        BinaryOperator<PlanNodeStats> allowNoDuplicates = (a, b) -> {
            throw new IllegalArgumentException("PlanNodeIds must be unique");
        };
        return mergeMaps(stagesStatsStream, allowNoDuplicates);
    }

    private PlanNodeStatsEstimate toPlanNodeStatsEstimate(PlanNodeStats operatorStats)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(operatorStats.getPlanNodeOutputPositions())
                .setOutputSizeInBytes(operatorStats.getPlanNodeOutputDataSize().toBytes())
                .build();
    }

    private MetricComparison createMetricComparison(Metric metric, PlanNode node, PlanNodeStatsEstimate estimate, Optional<PlanNodeStatsEstimate> execution)
    {
        Optional<Double> estimatedCost = asOptional(metric.getValue(estimate));
        Optional<Double> executionCost = execution.flatMap(e -> asOptional(metric.getValue(e)));
        return new MetricComparison(node, metric, estimatedCost, executionCost);
    }

    private Optional<Double> asOptional(double value)
    {
        return Double.isNaN(value) ? Optional.empty() : Optional.of(value);
    }
}
