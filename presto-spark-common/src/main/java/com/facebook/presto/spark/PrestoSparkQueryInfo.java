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
package com.facebook.presto.spark;

import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import static com.facebook.presto.server.protocol.QueryResourceUtil.globalUniqueNodes;
import static java.util.Objects.requireNonNull;

@Immutable
public class PrestoSparkQueryInfo
{
    // the following fields are based on com.facebook.presto.jdbc.QueryStats,
    // with the following fields removed:
    //  - queued
    //  - queuedSplits
    //  - runningSplits
    //  - completedSplits: duplciate of totalSplits in Presto-on-Spark
    //  - queuedTimeMillis
    //  - rootStage: it can be too verbose
    private final String queryId;
    private final String state;
    private final int nodes;
    private final int totalSplits;
    private final long cpuTimeMillis;
    private final long wallTimeMillis;
    private final long elapsedTimeMillis;
    private final long processedRows;
    private final long processedBytes;
    private final long peakMemoryBytes;
    private final long peakTotalMemoryBytes;
    private final long peakTaskTotalMemoryBytes;

    private final Optional<ExecutionFailureInfo> failureInfo;

    @JsonCreator
    public PrestoSparkQueryInfo(
            String queryId,
            String state,
            int nodes,
            int totalSplits,
            long cpuTimeMillis,
            long wallTimeMillis,
            long elapsedTimeMillis,
            long processedRows,
            long processedBytes,
            long peakMemoryBytes,
            long peakTotalMemoryBytes,
            long peakTaskTotalMemoryBytes,
            Optional<ExecutionFailureInfo> failureInfo)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.state = requireNonNull(state, "state is null");
        this.nodes = nodes;
        this.totalSplits = totalSplits;
        this.cpuTimeMillis = cpuTimeMillis;
        this.wallTimeMillis = wallTimeMillis;
        this.elapsedTimeMillis = elapsedTimeMillis;
        this.processedRows = processedRows;
        this.processedBytes = processedBytes;
        this.peakMemoryBytes = peakMemoryBytes;
        this.peakTotalMemoryBytes = peakTotalMemoryBytes;
        this.peakTaskTotalMemoryBytes = peakTaskTotalMemoryBytes;

        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public int getNodes()
    {
        return nodes;
    }

    @JsonProperty
    public int getTotalSplits()
    {
        return totalSplits;
    }

    @JsonProperty
    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    @JsonProperty
    public long getWallTimeMillis()
    {
        return wallTimeMillis;
    }

    @JsonProperty
    public long getElapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    @JsonProperty
    public long getProcessedRows()
    {
        return processedRows;
    }

    @JsonProperty
    public long getProcessedBytes()
    {
        return processedBytes;
    }

    @JsonProperty
    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    @JsonProperty
    public long getPeakTotalMemoryBytes()
    {
        return peakTotalMemoryBytes;
    }

    @JsonProperty
    public long getPeakTaskTotalMemoryBytes()
    {
        return peakTaskTotalMemoryBytes;
    }

    @JsonProperty
    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    public static PrestoSparkQueryInfo createFromQueryInfo(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

        return new PrestoSparkQueryInfo(
                queryInfo.getQueryId().getId(),
                queryInfo.getState().name(),
                globalUniqueNodes(outputStage).size(),
                queryStats.getTotalDrivers(),
                queryStats.getTotalCpuTime().toMillis(),
                queryStats.getTotalScheduledTime().toMillis(),
                queryStats.getElapsedTime().toMillis(),
                queryStats.getRawInputPositions(),
                queryStats.getRawInputDataSize().toBytes(),
                queryStats.getPeakUserMemoryReservation().toBytes(),
                queryStats.getPeakTotalMemoryReservation().toBytes(),
                queryStats.getPeakTaskTotalMemory().toBytes(),
                Optional.ofNullable(queryInfo.getFailureInfo()));
    }
}
