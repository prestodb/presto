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
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class QueryManagerConfig
{
    private int maxPendingSplitsPerNode = 100;

    private int initialHashPartitions = 8;
    private Duration maxQueryAge = new Duration(15, TimeUnit.MINUTES);
    private int maxQueryHistory = 100;
    private Duration clientTimeout = new Duration(5, TimeUnit.MINUTES);

    private int queryManagerExecutorPoolSize = 5;

    private int remoteTaskMaxConsecutiveErrorCount = 10;
    private Duration remoteTaskMinErrorDuration = new Duration(2, TimeUnit.MINUTES);

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

    @Min(1)
    public int getInitialHashPartitions()
    {
        return initialHashPartitions;
    }

    @Config("query.initial-hash-partitions")
    public QueryManagerConfig setInitialHashPartitions(int initialHashPartitions)
    {
        this.initialHashPartitions = initialHashPartitions;
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

    @Min(0)
    public int getMaxQueryHistory()
    {
        return maxQueryHistory;
    }

    @Config("query.max-history")
    public QueryManagerConfig setMaxQueryHistory(int maxQueryHistory)
    {
        this.maxQueryHistory = maxQueryHistory;
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
