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

import io.prestosql.client.StatementStats;

import java.util.Optional;
import java.util.OptionalDouble;

import static java.lang.Math.min;
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
    private final long cpuTimeMillis;
    private final long wallTimeMillis;
    private final long queuedTimeMillis;
    private final long elapsedTimeMillis;
    private final long processedRows;
    private final long processedBytes;
    private final long peakMemoryBytes;
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
            long cpuTimeMillis,
            long wallTimeMillis,
            long queuedTimeMillis,
            long elapsedTimeMillis,
            long processedRows,
            long processedBytes,
            long peakMemoryBytes,
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
        this.cpuTimeMillis = cpuTimeMillis;
        this.wallTimeMillis = wallTimeMillis;
        this.queuedTimeMillis = queuedTimeMillis;
        this.elapsedTimeMillis = elapsedTimeMillis;
        this.processedRows = processedRows;
        this.processedBytes = processedBytes;
        this.peakMemoryBytes = peakMemoryBytes;
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
                stats.getCpuTimeMillis(),
                stats.getWallTimeMillis(),
                stats.getQueuedTimeMillis(),
                stats.getElapsedTimeMillis(),
                stats.getProcessedRows(),
                stats.getProcessedBytes(),
                stats.getPeakMemoryBytes(),
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

    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    public long getWallTimeMillis()
    {
        return wallTimeMillis;
    }

    public long getQueuedTimeMillis()
    {
        return queuedTimeMillis;
    }

    public long getElapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    public long getProcessedRows()
    {
        return processedRows;
    }

    public long getProcessedBytes()
    {
        return processedBytes;
    }

    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    public Optional<StageStats> getRootStage()
    {
        return rootStage;
    }

    public OptionalDouble getProgressPercentage()
    {
        if (!scheduled || totalSplits == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(min(100, (completedSplits * 100.0) / totalSplits));
    }
}
