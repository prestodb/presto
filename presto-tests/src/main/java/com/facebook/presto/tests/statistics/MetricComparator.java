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

public final class MetricComparator
{
    private MetricComparator() {}

    public static List<MetricComparison> createMetricComparisons(Plan queryPlan, StageInfo outputStageInfo)
    {
        return Stream.of(Metric.values()).flatMap(metric -> {
            Map<PlanNodeId, PlanNodeStatsEstimate> estimates = queryPlan.getPlanNodeStats();
            Map<PlanNodeId, PlanNodeStatsEstimate> actuals = extractActualStats(outputStageInfo);
            return estimates.entrySet().stream().map(entry -> {
                // todo refactor to stay in PlanNodeId domain ????
                PlanNode node = planNodeForId(queryPlan, entry.getKey());
                PlanNodeStatsEstimate estimate = entry.getValue();
                Optional<PlanNodeStatsEstimate> execution = Optional.ofNullable(actuals.get(node.getId()));
                return createMetricComparison(metric, node, estimate, execution);
            });
        }).collect(Collectors.toList());
    }

    private static PlanNode planNodeForId(Plan queryPlan, PlanNodeId id)
    {
        return searchFrom(queryPlan.getRoot())
                .where(node -> node.getId().equals(id))
                .findOnlyElement();
    }

    private static Map<PlanNodeId, PlanNodeStatsEstimate> extractActualStats(StageInfo outputStageInfo)
    {
        Stream<Map<PlanNodeId, PlanNodeStats>> stagesStatsStream =
                getAllStages(Optional.of(outputStageInfo)).stream()
                        .map(PlanNodeStatsSummarizer::aggregatePlanNodeStats);

        Map<PlanNodeId, PlanNodeStats> mergedStats = mergeStats(stagesStatsStream);
        return transformValues(mergedStats, MetricComparator::toPlanNodeStats);
    }

    private static Map<PlanNodeId, PlanNodeStats> mergeStats(Stream<Map<PlanNodeId, PlanNodeStats>> stagesStatsStream)
    {
        BinaryOperator<PlanNodeStats> allowNoDuplicates = (a, b) -> {
            throw new IllegalArgumentException("PlanNodeIds must be unique");
        };
        return mergeMaps(stagesStatsStream, allowNoDuplicates);
    }

    private static PlanNodeStatsEstimate toPlanNodeStats(PlanNodeStats operatorStats)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(operatorStats.getPlanNodeOutputPositions())
                .build();
        // TODO think if we want to compare estimated data size with actual data size
        //      hacky way to do it is to have single symbol with single range with data_size set to
        //      new Estimate(operatorStats.getPlanNodeOutputDataSize().toBytes())
    }

    private static MetricComparison createMetricComparison(Metric metric, PlanNode node, PlanNodeStatsEstimate estimate, Optional<PlanNodeStatsEstimate> execution)
    {
        Optional<Double> estimatedStats = asOptional(metric.getValue(estimate));
        Optional<Double> executionStats = execution.flatMap(e -> asOptional(metric.getValue(e)));
        return new MetricComparison(node, metric, estimatedStats, executionStats);
    }

    private static Optional<Double> asOptional(double value)
    {
        return Double.isNaN(value) ? Optional.empty() : Optional.of(value);
    }
}
