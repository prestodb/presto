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
package com.facebook.presto.verifier.event;

import com.facebook.airlift.event.client.EventField;
import com.facebook.airlift.event.client.EventType;
import com.facebook.presto.jdbc.QueryStats;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
@EventType("QueryStatsEvent")
public class QueryStatsEvent
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
    private final long peakTotalMemoryBytes;
    private final long peakTaskTotalMemoryBytes;

    public QueryStatsEvent(QueryStats stats)
    {
        this(
                stats.getQueryId(),
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
                stats.getPeakTotalMemoryBytes(),
                stats.getPeakTaskTotalMemoryBytes());
    }

    public QueryStatsEvent(
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
            long peakTotalMemoryBytes,
            long peakTaskTotalMemoryBytes)
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
        this.peakTotalMemoryBytes = peakTotalMemoryBytes;
        this.peakTaskTotalMemoryBytes = peakTaskTotalMemoryBytes;
    }

    @EventField
    public String getQueryId()
    {
        return queryId;
    }

    @EventField
    public String getState()
    {
        return state;
    }

    @EventField
    public boolean isQueued()
    {
        return queued;
    }

    @EventField
    public boolean isScheduled()
    {
        return scheduled;
    }

    @EventField
    public int getNodes()
    {
        return nodes;
    }

    @EventField
    public int getTotalSplits()
    {
        return totalSplits;
    }

    @EventField
    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    @EventField
    public int getRunningSplits()
    {
        return runningSplits;
    }

    @EventField
    public int getCompletedSplits()
    {
        return completedSplits;
    }

    @EventField
    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    @EventField
    public long getWallTimeMillis()
    {
        return wallTimeMillis;
    }

    @EventField
    public long getQueuedTimeMillis()
    {
        return queuedTimeMillis;
    }

    @EventField
    public long getElapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    @EventField
    public long getProcessedRows()
    {
        return processedRows;
    }

    @EventField
    public long getProcessedBytes()
    {
        return processedBytes;
    }

    @EventField
    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    @EventField
    public long getPeakTotalMemoryBytes()
    {
        return peakTotalMemoryBytes;
    }

    @EventField
    public long getPeakTaskTotalMemoryBytes()
    {
        return peakTaskTotalMemoryBytes;
    }
}
