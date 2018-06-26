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
package com.facebook.presto.spi.eventlistener;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryStatistics
{
    private final Duration cpuTime;
    private final Duration wallTime;
    private final Duration queuedTime;
    private final Optional<Duration> analysisTime;
    private final Optional<Duration> distributedPlanningTime;

    private final long peakUserMemoryBytes;
    // peak of user + system memory
    private final long peakTotalNonRevocableMemoryBytes;
    private final long peakTaskTotalMemory;
    private final long totalBytes;
    private final long totalRows;
    private final long outputBytes;
    private final long outputRows;
    private final long writtenBytes;
    private final long writtenRows;

    private final double cumulativeMemory;

    private final List<StageGcStatistics> stageGcStatistics;

    private final int completedSplits;
    private final boolean complete;

    private final List<StageCpuDistribution> cpuTimeDistribution;

    private final List<String> operatorSummaries;

    public QueryStatistics(
            Duration cpuTime,
            Duration wallTime,
            Duration queuedTime,
            Optional<Duration> analysisTime,
            Optional<Duration> distributedPlanningTime,
            long peakUserMemoryBytes,
            long peakTotalNonRevocableMemoryBytes,
            long peakTaskTotalMemory,
            long totalBytes,
            long totalRows,
            long outputBytes,
            long outputRows,
            long writtenBytes,
            long writtenRows,
            double cumulativeMemory,
            List<StageGcStatistics> stageGcStatistics,
            int completedSplits,
            boolean complete,
            List<StageCpuDistribution> cpuTimeDistribution,
            List<String> operatorSummaries)
    {
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.wallTime = requireNonNull(wallTime, "wallTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.analysisTime = requireNonNull(analysisTime, "analysisTime is null");
        this.distributedPlanningTime = requireNonNull(distributedPlanningTime, "distributedPlanningTime is null");
        this.peakUserMemoryBytes = peakUserMemoryBytes;
        this.peakTotalNonRevocableMemoryBytes = peakTotalNonRevocableMemoryBytes;
        this.peakTaskTotalMemory = peakTaskTotalMemory;
        this.totalBytes = totalBytes;
        this.totalRows = totalRows;
        this.outputBytes = outputBytes;
        this.outputRows = outputRows;
        this.writtenBytes = writtenBytes;
        this.writtenRows = writtenRows;
        this.cumulativeMemory = cumulativeMemory;
        this.stageGcStatistics = requireNonNull(stageGcStatistics, "stageGcStatistics is null");
        this.completedSplits = completedSplits;
        this.complete = complete;
        this.cpuTimeDistribution = requireNonNull(cpuTimeDistribution, "cpuTimeDistribution is null");
        this.operatorSummaries = requireNonNull(operatorSummaries, "operatorSummaries is null");
    }

    public Duration getCpuTime()
    {
        return cpuTime;
    }

    public Duration getWallTime()
    {
        return wallTime;
    }

    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    public Optional<Duration> getAnalysisTime()
    {
        return analysisTime;
    }

    public Optional<Duration> getDistributedPlanningTime()
    {
        return distributedPlanningTime;
    }

    public long getPeakUserMemoryBytes()
    {
        return peakUserMemoryBytes;
    }

    public long getPeakTotalNonRevocableMemoryBytes()
    {
        return peakTotalNonRevocableMemoryBytes;
    }

    public long peakTaskTotalMemory()
    {
        return peakTaskTotalMemory;
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getTotalRows()
    {
        return totalRows;
    }

    public long getOutputBytes()
    {
        return outputBytes;
    }

    public long getOutputRows()
    {
        return outputRows;
    }

    public long getWrittenBytes()
    {
        return writtenBytes;
    }

    public long getWrittenRows()
    {
        return writtenRows;
    }

    public double getCumulativeMemory()
    {
        return cumulativeMemory;
    }

    public List<StageGcStatistics> getStageGcStatistics()
    {
        return stageGcStatistics;
    }

    public int getCompletedSplits()
    {
        return completedSplits;
    }

    public boolean isComplete()
    {
        return complete;
    }

    public List<StageCpuDistribution> getCpuTimeDistribution()
    {
        return cpuTimeDistribution;
    }

    public List<String> getOperatorSummaries()
    {
        return operatorSummaries;
    }
}
