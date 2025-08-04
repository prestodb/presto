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
package com.facebook.presto.util;

import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.slice.XxHash64;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.Long.toHexString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class QueryInfoUtils
{
    private QueryInfoUtils() {}

    public static String computeQueryHash(String query)
    {
        requireNonNull(query, "query is null");

        if (query.isEmpty()) {
            return "";
        }

        byte[] queryBytes = query.getBytes(UTF_8);
        long queryHash = new XxHash64().update(queryBytes).hash();
        return toHexString(queryHash);
    }

    public static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

        Set<String> globalUniqueNodes = new HashSet<>();
        StageStats rootStageStats = toStageStats(outputStage, globalUniqueNodes);

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setWaitingForPrerequisites(queryInfo.getState() == QueryState.WAITING_FOR_PREREQUISITES)
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setScheduled(queryInfo.isScheduled())
                .setNodes(globalUniqueNodes.size())
                .setTotalSplits(queryStats.getTotalDrivers())
                .setQueuedSplits(queryStats.getQueuedDrivers())
                .setRunningSplits(queryStats.getRunningDrivers() + queryStats.getBlockedDrivers())
                .setCompletedSplits(queryStats.getCompletedDrivers())
                .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                .setWaitingForPrerequisitesTimeMillis(queryStats.getWaitingForPrerequisitesTime().toMillis())
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .setProcessedRows(queryStats.getRawInputPositions())
                .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                .setPeakMemoryBytes(queryStats.getPeakUserMemoryReservation().toBytes())
                .setPeakTotalMemoryBytes(queryStats.getPeakTotalMemoryReservation().toBytes())
                .setPeakTaskTotalMemoryBytes(queryStats.getPeakTaskTotalMemory().toBytes())
                .setSpilledBytes(queryStats.getSpilledDataSize().toBytes())
                .setRootStage(rootStageStats)
                .setRuntimeStats(queryStats.getRuntimeStats())
                .build();
    }

    private static StageStats toStageStats(StageInfo stageInfo, Set<String> globalUniqueNodeIds)
    {
        if (stageInfo == null) {
            return null;
        }

        StageExecutionInfo currentStageExecutionInfo = stageInfo.getLatestAttemptExecutionInfo();
        StageExecutionStats stageExecutionStats = currentStageExecutionInfo.getStats();

        // Store current stage details into a builder
        StageStats.Builder builder = StageStats.builder()
                .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                .setState(currentStageExecutionInfo.getState().toString())
                .setDone(currentStageExecutionInfo.getState().isDone())
                .setTotalSplits(stageExecutionStats.getTotalDrivers())
                .setQueuedSplits(stageExecutionStats.getQueuedDrivers())
                .setRunningSplits(stageExecutionStats.getRunningDrivers() + stageExecutionStats.getBlockedDrivers())
                .setCompletedSplits(stageExecutionStats.getCompletedDrivers())
                .setCpuTimeMillis(stageExecutionStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(stageExecutionStats.getTotalScheduledTime().toMillis())
                .setProcessedRows(stageExecutionStats.getRawInputPositions())
                .setProcessedBytes(stageExecutionStats.getRawInputDataSizeInBytes())
                .setNodes(countStageAndAddGlobalUniqueNodes(currentStageExecutionInfo.getTasks(), globalUniqueNodeIds));

        // Recurse into child stages to create their StageStats
        List<StageInfo> subStages = stageInfo.getSubStages();
        if (subStages.isEmpty()) {
            builder.setSubStages(ImmutableList.of());
        }
        else {
            ImmutableList.Builder<StageStats> subStagesBuilder = ImmutableList.builderWithExpectedSize(subStages.size());
            for (StageInfo subStage : subStages) {
                subStagesBuilder.add(toStageStats(subStage, globalUniqueNodeIds));
            }
            builder.setSubStages(subStagesBuilder.build());
        }

        return builder.build();
    }

    private static int countStageAndAddGlobalUniqueNodes(List<TaskInfo> tasks, Set<String> globalUniqueNodes)
    {
        Set<String> stageUniqueNodes = Sets.newHashSetWithExpectedSize(tasks.size());
        for (TaskInfo task : tasks) {
            String nodeId = task.getNodeId();
            stageUniqueNodes.add(nodeId);
            globalUniqueNodes.add(nodeId);
        }
        return stageUniqueNodes.size();
    }
}
