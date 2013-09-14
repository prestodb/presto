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
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class QueryManagerConfig
{
    private boolean coordinator = true;
    private DataSize maxTaskMemoryUsage = new DataSize(256, Unit.MEGABYTE);
    private DataSize operatorPreAllocatedMemory = new DataSize(16, Unit.MEGABYTE);
    private int maxPendingSplitsPerNode = 100;
    private int maxShardProcessorThreads = Runtime.getRuntime().availableProcessors() * 4;
    private DataSize sinkMaxBufferSize = new DataSize(32, Unit.MEGABYTE);
    private Duration maxQueryAge = new Duration(15, TimeUnit.MINUTES);
    private Duration clientTimeout = new Duration(5, TimeUnit.MINUTES);
    private Duration infoMaxAge = new Duration(15, TimeUnit.MINUTES);

    private DataSize exchangeMaxBufferSize = new DataSize(32, Unit.MEGABYTE);
    private int exchangeConcurrentRequestMultiplier = 3;

    private int queryManagerExecutorPoolSize = 5;

    private int remoteTaskMaxConsecutiveErrorCount = 10;
    private Duration remoteTaskMinErrorDuration = new Duration(2, TimeUnit.MINUTES);

    public boolean isCoordinator()
    {
        return coordinator;
    }

    @Config("coordinator")
    public QueryManagerConfig setCoordinator(boolean coordinator)
    {
        this.coordinator = coordinator;
        return this;
    }

    @Min(1)
    public int getMaxPendingSplitsPerNode()
    {
        return maxPendingSplitsPerNode;
    }

    @Config("query.max-pending-splits-per-node")
    public QueryManagerConfig setMaxPendingSplitsPerNode(int maxPendingSplitsPerNode)
    {
        this.maxPendingSplitsPerNode = maxPendingSplitsPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxTaskMemoryUsage()
    {
        return maxTaskMemoryUsage;
    }

    @Config("task.max-memory")
    @LegacyConfig("query.operator.max-memory")
    public QueryManagerConfig setMaxTaskMemoryUsage(DataSize maxTaskMemoryUsage)
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
    public QueryManagerConfig setOperatorPreAllocatedMemory(DataSize operatorPreAllocatedMemory)
    {
        this.operatorPreAllocatedMemory = operatorPreAllocatedMemory;
        return this;
    }

    @Min(1)
    public int getMaxShardProcessorThreads()
    {
        return maxShardProcessorThreads;
    }

    @Config("query.shard.max-threads")
    public QueryManagerConfig setMaxShardProcessorThreads(int maxShardProcessorThreads)
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
    public QueryManagerConfig setSinkMaxBufferSize(DataSize sinkMaxBufferSize)
    {
        this.sinkMaxBufferSize = sinkMaxBufferSize;
        return this;
    }

    @NotNull
    public Duration getMaxQueryAge()
    {
        return maxQueryAge;
    }

    @Config("query.max-age")
    public QueryManagerConfig setMaxQueryAge(Duration maxQueryAge)
    {
        this.maxQueryAge = maxQueryAge;
        return this;
    }

    @MinDuration("5s")
    @NotNull
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    @Config("query.client.timeout")
    public QueryManagerConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @NotNull
    public Duration getInfoMaxAge()
    {
        return infoMaxAge;
    }

    @Config("query.info.max-age")
    public QueryManagerConfig setInfoMaxAge(Duration infoMaxAge)
    {
        this.infoMaxAge = infoMaxAge;
        return this;
    }

    @NotNull
    public DataSize getExchangeMaxBufferSize()
    {
        return exchangeMaxBufferSize;
    }

    @Config("exchange.max-buffer-size")
    public QueryManagerConfig setExchangeMaxBufferSize(DataSize exchangeMaxBufferSize)
    {
        this.exchangeMaxBufferSize = exchangeMaxBufferSize;
        return this;
    }

    @Min(1)
    public int getExchangeConcurrentRequestMultiplier()
    {
        return exchangeConcurrentRequestMultiplier;
    }

    @Config("exchange.concurrent-request-multiplier")
    public QueryManagerConfig setExchangeConcurrentRequestMultiplier(int exchangeConcurrentRequestMultiplier)
    {
        this.exchangeConcurrentRequestMultiplier = exchangeConcurrentRequestMultiplier;
        return this;
    }

    @Min(1)
    public int getQueryManagerExecutorPoolSize()
    {
        return queryManagerExecutorPoolSize;
    }

    @Config("query.manager-executor-pool-size")
    public QueryManagerConfig setQueryManagerExecutorPoolSize(int queryManagerExecutorPoolSize)
    {
        this.queryManagerExecutorPoolSize = queryManagerExecutorPoolSize;
        return this;
    }

    @Min(0)
    public int getRemoteTaskMaxConsecutiveErrorCount()
    {
        return remoteTaskMaxConsecutiveErrorCount;
    }

    @Config("query.remote-task.max-consecutive-error-count")
    public QueryManagerConfig setRemoteTaskMaxConsecutiveErrorCount(int remoteTaskMaxConsecutiveErrorCount)
    {
        this.remoteTaskMaxConsecutiveErrorCount = remoteTaskMaxConsecutiveErrorCount;
        return this;
    }

    @NotNull
    public Duration getRemoteTaskMinErrorDuration()
    {
        return remoteTaskMinErrorDuration;
    }

    @Config("query.remote-task.min-error-duration")
    public QueryManagerConfig setRemoteTaskMinErrorDuration(Duration remoteTaskMinErrorDuration)
    {
        this.remoteTaskMinErrorDuration = remoteTaskMinErrorDuration;
        return this;
    }
}
