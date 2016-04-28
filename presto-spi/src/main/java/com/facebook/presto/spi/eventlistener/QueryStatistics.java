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

public class QueryStatistics
{
    private final Long queuedTimeMs;
    private final Long analysisTimeMs;
    private final Long distributedPlanningTimeMs;
    private final Long totalSplitWallTimeMs;
    private final Long totalSplitCpuTimeMs;

    private final Long peakMemoryBytes;
    private final Long totalBytes;
    private final Long totalRows;

    private final Integer splits;

    public QueryStatistics(Long queuedTimeMs, Long analysisTimeMs, Long distributedPlanningTimeMs, Long totalSplitWallTimeMs, Long totalSplitCpuTimeMs, Long peakMemoryBytes, Long totalBytes, Long totalRows, Integer splits)
    {
        this.queuedTimeMs = queuedTimeMs;
        this.analysisTimeMs = analysisTimeMs;
        this.distributedPlanningTimeMs = distributedPlanningTimeMs;
        this.totalSplitWallTimeMs = totalSplitWallTimeMs;
        this.totalSplitCpuTimeMs = totalSplitCpuTimeMs;
        this.peakMemoryBytes = peakMemoryBytes;
        this.totalBytes = totalBytes;
        this.totalRows = totalRows;
        this.splits = splits;
    }

    public Long getQueuedTimeMs()
    {
        return queuedTimeMs;
    }

    public Long getAnalysisTimeMs()
    {
        return analysisTimeMs;
    }

    public Long getDistributedPlanningTimeMs()
    {
        return distributedPlanningTimeMs;
    }

    public Long getTotalSplitWallTimeMs()
    {
        return totalSplitWallTimeMs;
    }

    public Long getTotalSplitCpuTimeMs()
    {
        return totalSplitCpuTimeMs;
    }

    public Long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    public Long getTotalBytes()
    {
        return totalBytes;
    }

    public Long getTotalRows()
    {
        return totalRows;
    }

    public Integer getSplits()
    {
        return splits;
    }
}
