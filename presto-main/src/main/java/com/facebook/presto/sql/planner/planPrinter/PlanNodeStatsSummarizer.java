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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.HashCollisionsInfo;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.util.MoreMaps.mergeMaps;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Lists.reverse;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctDataSize;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class PlanNodeStatsSummarizer
{
    private PlanNodeStatsSummarizer() {}

    static Map<PlanNodeId, PlanNodeStats> aggregatePlanNodeStats(StageInfo stageInfo)
    {
        Map<PlanNodeId, PlanNodeStats> aggregatedStats = new HashMap<>();
        List<PlanNodeStats> planNodeStats = stageInfo.getTasks().stream()
                .map(TaskInfo::getStats)
                .flatMap(taskStats -> getPlanNodeStats(taskStats).stream())
                .collect(toList());
        for (PlanNodeStats stats : planNodeStats) {
            aggregatedStats.merge(stats.getPlanNodeId(), stats, PlanNodeStats::merge);
        }
        return aggregatedStats;
    }

    private static List<PlanNodeStats> getPlanNodeStats(TaskStats taskStats)
    {
        // Best effort to reconstruct the plan nodes from operators.
        // Because stats are collected separately from query execution,
        // it's possible that some or all of them are missing or out of date.
        // For example, a LIMIT clause can cause a query to finish before stats
        // are collected from the leaf stages.
        Map<PlanNodeId, Long> planNodeInputPositions = new HashMap<>();
        Map<PlanNodeId, Long> planNodeInputBytes = new HashMap<>();
        Map<PlanNodeId, Long> planNodeOutputPositions = new HashMap<>();
        Map<PlanNodeId, Long> planNodeOutputBytes = new HashMap<>();
        Map<PlanNodeId, Long> planNodeWallMillis = new HashMap<>();

        Map<PlanNodeId, Map<String, OperatorInputStats>> operatorInputStats = new HashMap<>();
        Map<PlanNodeId, Map<String, OperatorHashCollisionsStats>> operatorHashCollisionsStats = new HashMap<>();

        for (PipelineStats pipelineStats : taskStats.getPipelines()) {
            // Due to eventual consistently collected stats, these could be empty
            if (pipelineStats.getOperatorSummaries().isEmpty()) {
                continue;
            }

            Set<PlanNodeId> processedNodes = new HashSet<>();
            PlanNodeId inputPlanNode = pipelineStats.getOperatorSummaries().iterator().next().getPlanNodeId();
            PlanNodeId outputPlanNode = getLast(pipelineStats.getOperatorSummaries()).getPlanNodeId();

            // Gather input statistics
            for (OperatorStats operatorStats : pipelineStats.getOperatorSummaries()) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();

                long wall = operatorStats.getAddInputWall().toMillis() + operatorStats.getGetOutputWall().toMillis() + operatorStats.getFinishWall().toMillis();
                planNodeWallMillis.merge(planNodeId, wall, Long::sum);

                // A pipeline like hash build before join might link to another "internal" pipelines which provide actual input for this plan node
                if (operatorStats.getPlanNodeId().equals(inputPlanNode) && !pipelineStats.isInputPipeline()) {
                    continue;
                }
                if (processedNodes.contains(planNodeId)) {
                    continue;
                }

                operatorInputStats.merge(planNodeId,
                        ImmutableMap.of(
                                operatorStats.getOperatorType(),
                                new OperatorInputStats(
                                        operatorStats.getTotalDrivers(),
                                        operatorStats.getInputPositions(),
                                        operatorStats.getSumSquaredInputPositions())),
                        (map1, map2) -> mergeMaps(map1, map2, OperatorInputStats::merge));

                if (operatorStats.getInfo() instanceof HashCollisionsInfo) {
                    HashCollisionsInfo hashCollisionsInfo = (HashCollisionsInfo) operatorStats.getInfo();
                    operatorHashCollisionsStats.merge(planNodeId,
                            ImmutableMap.of(
                                    operatorStats.getOperatorType(),
                                    new OperatorHashCollisionsStats(
                                            hashCollisionsInfo.getWeightedHashCollisions(),
                                            hashCollisionsInfo.getWeightedSumSquaredHashCollisions(),
                                            hashCollisionsInfo.getWeightedExpectedHashCollisions())),
                            (map1, map2) -> mergeMaps(map1, map2, OperatorHashCollisionsStats::merge));
                }

                planNodeInputPositions.merge(planNodeId, operatorStats.getInputPositions(), Long::sum);
                planNodeInputBytes.merge(planNodeId, operatorStats.getInputDataSize().toBytes(), Long::sum);
                processedNodes.add(planNodeId);
            }

            // Gather output statistics
            processedNodes.clear();
            for (OperatorStats operatorStats : reverse(pipelineStats.getOperatorSummaries())) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();

                // An "internal" pipeline like a hash build, links to another pipeline which is the actual output for this plan node
                if (operatorStats.getPlanNodeId().equals(outputPlanNode) && !pipelineStats.isOutputPipeline()) {
                    continue;
                }
                if (processedNodes.contains(planNodeId)) {
                    continue;
                }

                planNodeOutputPositions.merge(planNodeId, operatorStats.getOutputPositions(), Long::sum);
                planNodeOutputBytes.merge(planNodeId, operatorStats.getOutputDataSize().toBytes(), Long::sum);
                processedNodes.add(planNodeId);
            }
        }

        List<PlanNodeStats> stats = new ArrayList<>();
        for (Map.Entry<PlanNodeId, Long> entry : planNodeWallMillis.entrySet()) {
            PlanNodeId planNodeId = entry.getKey();
            stats.add(new PlanNodeStats(
                    planNodeId,
                    new Duration(planNodeWallMillis.get(planNodeId), MILLISECONDS),
                    planNodeInputPositions.get(planNodeId),
                    succinctDataSize(planNodeInputBytes.get(planNodeId), BYTE),
                    // It's possible there will be no output stats because all the pipelines that we observed were non-output.
                    // For example in a query like SELECT * FROM a JOIN b ON c = d LIMIT 1
                    // It's possible to observe stats after the build starts, but before the probe does
                    // and therefore only have wall time, but no output stats
                    planNodeOutputPositions.getOrDefault(planNodeId, 0L),
                    succinctDataSize(planNodeOutputBytes.getOrDefault(planNodeId, 0L), BYTE),
                    operatorInputStats.get(planNodeId),
                    // Only some operators emit hash collisions statistics
                    operatorHashCollisionsStats.getOrDefault(planNodeId, emptyMap())));
        }
        return stats;
    }
}
