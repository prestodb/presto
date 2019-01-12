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
package io.prestosql.jdbc;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class StageStats
{
    private final String stageId;
    private final String state;
    private final boolean done;
    private final int nodes;
    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;
    private final long cpuTimeMillis;
    private final long wallTimeMillis;
    private final long processedRows;
    private final long processedBytes;
    private final List<StageStats> subStages;

    public StageStats(
            String stageId,
            String state,
            boolean done,
            int nodes,
            int totalSplits,
            int queuedSplits,
            int runningSplits,
            int completedSplits,
            long cpuTimeMillis,
            long wallTimeMillis,
            long processedRows,
            long processedBytes,
            List<StageStats> subStages)
    {
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.state = requireNonNull(state, "state is null");
        this.done = done;
        this.nodes = nodes;
        this.totalSplits = totalSplits;
        this.queuedSplits = queuedSplits;
        this.runningSplits = runningSplits;
        this.completedSplits = completedSplits;
        this.cpuTimeMillis = cpuTimeMillis;
        this.wallTimeMillis = wallTimeMillis;
        this.processedRows = processedRows;
        this.processedBytes = processedBytes;
        this.subStages = ImmutableList.copyOf(requireNonNull(subStages, "subStages is null"));
    }

    static StageStats create(io.prestosql.client.StageStats stats)
    {
        return new StageStats(
                stats.getStageId(),
                stats.getState(),
                stats.isDone(),
                stats.getNodes(),
                stats.getTotalSplits(),
                stats.getQueuedSplits(),
                stats.getRunningSplits(),
                stats.getCompletedSplits(),
                stats.getCpuTimeMillis(),
                stats.getWallTimeMillis(),
                stats.getProcessedRows(),
                stats.getProcessedBytes(),
                stats.getSubStages().stream()
                        .map(StageStats::create)
                        .collect(toList()));
    }

    public String getStageId()
    {
        return stageId;
    }

    public String getState()
    {
        return state;
    }

    public boolean isDone()
    {
        return done;
    }

    public int getNodes()
    {
        return nodes;
    }

    public int getTotalSplits()
    {
        return totalSplits;
    }

    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    public int getRunningSplits()
    {
        return runningSplits;
    }

    public int getCompletedSplits()
    {
        return completedSplits;
    }

    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    public long getWallTimeMillis()
    {
        return wallTimeMillis;
    }

    public long getProcessedRows()
    {
        return processedRows;
    }

    public long getProcessedBytes()
    {
        return processedBytes;
    }

    public List<StageStats> getSubStages()
    {
        return subStages;
    }
}
