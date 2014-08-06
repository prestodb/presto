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
package com.facebook.presto.execution;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class TaskManagerConfig
{
    private boolean taskCpuTimerEnabled = true;
    private DataSize maxTaskMemoryUsage = new DataSize(256, Unit.MEGABYTE);
    private DataSize operatorPreAllocatedMemory = new DataSize(16, Unit.MEGABYTE);
    private DataSize maxTaskIndexMemoryUsage = new DataSize(64, Unit.MEGABYTE);
    private int maxShardProcessorThreads = Runtime.getRuntime().availableProcessors() * 4;

    private DataSize sinkMaxBufferSize = new DataSize(32, Unit.MEGABYTE);

    private Duration clientTimeout = new Duration(5, TimeUnit.MINUTES);
    private Duration infoMaxAge = new Duration(15, TimeUnit.MINUTES);

    public boolean isTaskCpuTimerEnabled()
    {
        return taskCpuTimerEnabled;
    }

    @Config("task.cpu-timer-enabled")
    public TaskManagerConfig setTaskCpuTimerEnabled(boolean taskCpuTimerEnabled)
    {
        this.taskCpuTimerEnabled = taskCpuTimerEnabled;
        return this;
    }

    @NotNull
    public DataSize getMaxTaskMemoryUsage()
    {
        return maxTaskMemoryUsage;
    }

    @Config("task.max-memory")
    public TaskManagerConfig setMaxTaskMemoryUsage(DataSize maxTaskMemoryUsage)
    {
        this.maxTaskMemoryUsage = maxTaskMemoryUsage;
        return this;
    }

    @NotNull
    public DataSize getOperatorPreAllocatedMemory()
    {
        return operatorPreAllocatedMemory;
    }

    @Config("task.operator-pre-allocated-memory")
    public TaskManagerConfig setOperatorPreAllocatedMemory(DataSize operatorPreAllocatedMemory)
    {
        this.operatorPreAllocatedMemory = operatorPreAllocatedMemory;
        return this;
    }

    @NotNull
    public DataSize getMaxTaskIndexMemoryUsage()
    {
        return maxTaskIndexMemoryUsage;
    }

    @Config("task.max-index-memory")
    public TaskManagerConfig setMaxTaskIndexMemoryUsage(DataSize maxTaskIndexMemoryUsage)
    {
        this.maxTaskIndexMemoryUsage = maxTaskIndexMemoryUsage;
        return this;
    }

    @Min(1)
    public int getMaxShardProcessorThreads()
    {
        return maxShardProcessorThreads;
    }

    @Config("task.shard.max-threads")
    public TaskManagerConfig setMaxShardProcessorThreads(int maxShardProcessorThreads)
    {
        this.maxShardProcessorThreads = maxShardProcessorThreads;
        return this;
    }

    @NotNull
    public DataSize getSinkMaxBufferSize()
    {
        return sinkMaxBufferSize;
    }

    @Config("sink.max-buffer-size")
    public TaskManagerConfig setSinkMaxBufferSize(DataSize sinkMaxBufferSize)
    {
        this.sinkMaxBufferSize = sinkMaxBufferSize;
        return this;
    }

    @MinDuration("5s")
    @NotNull
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    @Config("task.client.timeout")
    public TaskManagerConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @NotNull
    public Duration getInfoMaxAge()
    {
        return infoMaxAge;
    }

    @Config("task.info.max-age")
    public TaskManagerConfig setInfoMaxAge(Duration infoMaxAge)
    {
        this.infoMaxAge = infoMaxAge;
        return this;
    }
}
