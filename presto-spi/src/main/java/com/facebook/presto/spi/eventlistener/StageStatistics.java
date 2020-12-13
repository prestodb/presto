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

import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;

public class StageStatistics
{
    private final int stageId;
    private final int stageExecutionId;
    private final int tasks;

    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration retriedCpuTime;
    private final Duration totalBlockedTime;

    private final DataSize rawInputDataSize;
    private final DataSize processedInputDataSize;
    private final DataSize physicalWrittenDataSize;

    private final StageGcStatistics gcStatistics;
    private final ResourceDistribution cpuDistribution;
    private final ResourceDistribution memoryDistribution;

    public StageStatistics(
            int stageId,
            int stageExecutionId,
            int tasks,
            Duration totalScheduledTime,
            Duration totalCpuTime,
            Duration retriedCpuTime,
            Duration totalBlockedTime,
            DataSize rawInputDataSize,
            DataSize processedInputDataSize,
            DataSize physicalWrittenDataSize,
            StageGcStatistics gcStatistics,
            ResourceDistribution cpuDistribution,
            ResourceDistribution memoryDistribution)
    {
        this.stageId = stageId;
        this.stageExecutionId = stageExecutionId;
        this.tasks = tasks;
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.retriedCpuTime = requireNonNull(retriedCpuTime, "retriedCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawDataInputSize is null");
        this.processedInputDataSize = requireNonNull(processedInputDataSize, "processedInputDataSize is null");
        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "physicalWrittenDataSize is null");
        this.gcStatistics = requireNonNull(gcStatistics, "gcStatistics is null");
        this.cpuDistribution = requireNonNull(cpuDistribution, "cpuDistribution is null");
        this.memoryDistribution = requireNonNull(memoryDistribution, "memoryDistribution is null");
    }

    public int getStageId()
    {
        return stageId;
    }

    public int getStageExecutionId()
    {
        return stageExecutionId;
    }

    public int getTasks()
    {
        return tasks;
    }

    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    public Duration getRetriedCpuTime()
    {
        return retriedCpuTime;
    }

    public Duration getTotalBlockedTime()
    {
        return totalBlockedTime;
    }

    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    public DataSize getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    public StageGcStatistics getGcStatistics()
    {
        return gcStatistics;
    }

    public ResourceDistribution getCpuDistribution()
    {
        return cpuDistribution;
    }

    public ResourceDistribution getMemoryDistribution()
    {
        return memoryDistribution;
    }
}
