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
package com.facebook.presto.jdbc;

import com.facebook.presto.client.StatementStats;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class QueryStats
{
    private final String queryId;
    private final String state;
    private final boolean queued;
    private final boolean scheduled;
    private final int nodes;
    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;
    private final long userTimeMillis;
    private final long cpuTimeMillis;
    private final long wallTimeMillis;
    private final long processedRows;
    private final long processedBytes;
    private final Optional<StageStats> rootStage;

    public QueryStats(
            String queryId,
            String state,
            boolean queued,
            boolean scheduled,
            int nodes,
            int totalSplits,
            int queuedSplits,
            int runningSplits,
            int completedSplits,
            long userTimeMillis,
            long cpuTimeMillis,
            long wallTimeMillis,
            long processedRows,
            long processedBytes,
            Optional<StageStats> rootStage)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.state = requireNonNull(state, "state is null");
        this.queued = queued;
        this.scheduled = scheduled;
        this.nodes = nodes;
        this.totalSplits = totalSplits;
        this.queuedSplits = queuedSplits;
        this.runningSplits = runningSplits;
        this.completedSplits = completedSplits;
        this.userTimeMillis = userTimeMillis;
        this.cpuTimeMillis = cpuTimeMillis;
        this.wallTimeMillis = wallTimeMillis;
        this.processedRows = processedRows;
        this.processedBytes = processedBytes;
        this.rootStage = requireNonNull(rootStage, "rootStage is null");
    }

    static QueryStats create(String queryId, StatementStats stats)
    {
        return new QueryStats(
                queryId,
                stats.getState(),
                stats.isQueued(),
                stats.isScheduled(),
                stats.getNodes(),
                stats.getTotalSplits(),
                stats.getQueuedSplits(),
                stats.getRunningSplits(),
                stats.getCompletedSplits(),
                stats.getUserTimeMillis(),
                stats.getCpuTimeMillis(),
                stats.getWallTimeMillis(),
                stats.getProcessedRows(),
                stats.getProcessedBytes(),
                Optional.ofNullable(stats.getRootStage()).map(StageStats::create));
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getState()
    {
        return state;
    }

    public boolean isQueued()
    {
        return queued;
    }

    public boolean isScheduled()
    {
        return scheduled;
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

    public long getUserTimeMillis()
    {
        return userTimeMillis;
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

    public Optional<StageStats> getRootStage()
    {
        return rootStage;
    }
}
