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
    private final Duration wallTime;
    private final Duration queuedTime;
    private final Optional<Duration> analysisTime;
    private final Optional<Duration> distributedPlanningTime;

    private final long peakMemoryBytes;
    private final long totalBytes;
    private final long totalRows;

    private final int completedSplits;

    public QueryStatistics(
            Duration cpuTime,
            Duration wallTime,
            Duration queuedTime,
            Optional<Duration> analysisTime,
            Optional<Duration> distributedPlanningTime,
            long peakMemoryBytes,
            long totalBytes,
            long totalRows,
            int completedSplits)
    {
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.wallTime = requireNonNull(wallTime, "wallTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.analysisTime = requireNonNull(analysisTime, "analysisTime is null");
        this.distributedPlanningTime = requireNonNull(distributedPlanningTime, "distributedPlanningTime is null");
        this.peakMemoryBytes = requireNonNull(peakMemoryBytes, "peakMemoryBytes is null");
        this.totalBytes = requireNonNull(totalBytes, "totalBytes is null");
        this.totalRows = requireNonNull(totalRows, "totalRows is null");
        this.completedSplits = requireNonNull(completedSplits, "completedSplits is null");
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

    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getTotalRows()
    {
        return totalRows;
    }

    public int getCompletedSplits()
    {
        return completedSplits;
    }
}
