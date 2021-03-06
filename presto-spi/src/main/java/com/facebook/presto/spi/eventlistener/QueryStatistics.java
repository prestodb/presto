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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryStatistics
{
    private final Duration cpuTime;
    private final Duration retriedCpuTime;
    private final Duration wallTime;
    private final Duration queuedTime;
    private final Optional<Duration> analysisTime;

    private final int peakRunningTasks;
    private final long peakUserMemoryBytes;
    // peak of user + system memory
    private final long peakTotalNonRevocableMemoryBytes;
    private final long peakTaskUserMemory;
    private final long peakTaskTotalMemory;
    private final long peakNodeTotalMemory;
    private final long totalBytes;
    private final long totalRows;
    private final long outputBytes;
    private final long outputRows;
    private final long writtenOutputBytes;
    private final long writtenOutputRows;
    private final long writtenIntermediateBytes;
    private final long spilledBytes;

    private final double cumulativeMemory;

    private final int completedSplits;
    private final boolean complete;

    public QueryStatistics(
            Duration cpuTime,
            Duration retriedCpuTime,
            Duration wallTime,
            Duration queuedTime,
            Optional<Duration> analysisTime,
            int peakRunningTasks,
            long peakUserMemoryBytes,
            long peakTotalNonRevocableMemoryBytes,
            long peakTaskUserMemory,
            long peakTaskTotalMemory,
            long peakNodeTotalMemory,
            long totalBytes,
            long totalRows,
            long outputBytes,
            long outputRows,
            long writtenOutputBytes,
            long writtenOutputRows,
            long writtenIntermediateBytes,
            long spilledBytes,
            double cumulativeMemory,
            int completedSplits,
            boolean complete)
    {
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.retriedCpuTime = requireNonNull(retriedCpuTime, "retriedCpuTime is null");
        this.wallTime = requireNonNull(wallTime, "wallTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.analysisTime = requireNonNull(analysisTime, "analysisTime is null");
        this.peakRunningTasks = peakRunningTasks;
        this.peakUserMemoryBytes = peakUserMemoryBytes;
        this.peakTotalNonRevocableMemoryBytes = peakTotalNonRevocableMemoryBytes;
        this.peakTaskUserMemory = peakTaskUserMemory;
        this.peakTaskTotalMemory = peakTaskTotalMemory;
        this.peakNodeTotalMemory = peakNodeTotalMemory;
        this.totalBytes = totalBytes;
        this.totalRows = totalRows;
        this.outputBytes = outputBytes;
        this.outputRows = outputRows;
        this.writtenOutputBytes = writtenOutputBytes;
        this.writtenOutputRows = writtenOutputRows;
        this.writtenIntermediateBytes = writtenIntermediateBytes;
        this.spilledBytes = spilledBytes;
        this.cumulativeMemory = cumulativeMemory;
        this.completedSplits = completedSplits;
        this.complete = complete;
    }

    public Duration getCpuTime()
    {
        return cpuTime;
    }

    public Duration getRetriedCpuTime()
    {
        return retriedCpuTime;
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

    public int getPeakRunningTasks()
    {
        return peakRunningTasks;
    }

    public long getPeakUserMemoryBytes()
    {
        return peakUserMemoryBytes;
    }

    public long getPeakTotalNonRevocableMemoryBytes()
    {
        return peakTotalNonRevocableMemoryBytes;
    }

    public long getPeakTaskTotalMemory()
    {
        return peakTaskTotalMemory;
    }

    public long getPeakTaskUserMemory()
    {
        return peakTaskUserMemory;
    }

    public long getPeakNodeTotalMemory()
    {
        return peakNodeTotalMemory;
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

    public long getWrittenOutputBytes()
    {
        return writtenOutputBytes;
    }

    public long getWrittenOutputRows()
    {
        return writtenOutputRows;
    }

    public long getWrittenIntermediateBytes()
    {
        return writtenIntermediateBytes;
    }

    public long getSpilledBytes()
    {
        return spilledBytes;
    }

    public double getCumulativeMemory()
    {
        return cumulativeMemory;
    }

    public int getCompletedSplits()
    {
        return completedSplits;
    }

    public boolean isComplete()
    {
        return complete;
    }
}
