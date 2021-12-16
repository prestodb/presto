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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ResourceManagerConfig
{
    private Duration queryExpirationTimeout = new Duration(10, SECONDS);
    private Duration completedQueryExpirationTimeout = new Duration(10, MINUTES);
    private int maxCompletedQueries = 100;
    private Duration nodeStatusTimeout = new Duration(30, SECONDS);
    private Duration memoryPoolInfoRefreshDuration = new Duration(1, SECONDS);
    private Duration queryHeartbeatInterval = new Duration(1, SECONDS);
    private Duration nodeHeartbeatInterval = new Duration(1, SECONDS);
    private int heartbeatThreads = 3;
    private int heartbeatConcurrency = 3;
    private int resourceManagerExecutorThreads = 1000;
    private Duration proxyAsyncTimeout = new Duration(60, SECONDS);
    private Duration memoryPoolFetchInterval = new Duration(1, SECONDS);
    private boolean resourceGroupServiceCacheEnabled;
    private Duration resourceGroupServiceCacheExpireInterval = new Duration(10, SECONDS);
    private Duration resourceGroupServiceCacheRefreshInterval = new Duration(1, SECONDS);

    @MinDuration("1ms")
    public Duration getQueryExpirationTimeout()
    {
        return queryExpirationTimeout;
    }

    @Config("resource-manager.query-expiration-timeout")
    public ResourceManagerConfig setQueryExpirationTimeout(Duration queryExpirationTimeout)
    {
        this.queryExpirationTimeout = queryExpirationTimeout;
        return this;
    }

    @MinDuration("1ms")
    public Duration getCompletedQueryExpirationTimeout()
    {
        return completedQueryExpirationTimeout;
    }

    @Config("resource-manager.completed-query-expiration-timeout")
    public ResourceManagerConfig setCompletedQueryExpirationTimeout(Duration completedQueryExpirationTimeout)
    {
        this.completedQueryExpirationTimeout = completedQueryExpirationTimeout;
        return this;
    }

    @Min(1)
    public int getMaxCompletedQueries()
    {
        return maxCompletedQueries;
    }

    @Config("resource-manager.max-completed-queries")
    public ResourceManagerConfig setMaxCompletedQueries(int maxCompletedQueries)
    {
        this.maxCompletedQueries = maxCompletedQueries;
        return this;
    }

    @MinDuration("1ms")
    public Duration getNodeStatusTimeout()
    {
        return nodeStatusTimeout;
    }

    @Config("resource-manager.node-status-timeout")
    public ResourceManagerConfig setNodeStatusTimeout(Duration nodeStatusTimeout)
    {
        this.nodeStatusTimeout = nodeStatusTimeout;
        return this;
    }

    @MinDuration("1ms")
    public Duration getMemoryPoolInfoRefreshDuration()
    {
        return memoryPoolInfoRefreshDuration;
    }

    @Config("resource-manager.memory-pool-info-refresh-duration")
    public ResourceManagerConfig setMemoryPoolInfoRefreshDuration(Duration memoryPoolInfoRefreshDuration)
    {
        this.memoryPoolInfoRefreshDuration = memoryPoolInfoRefreshDuration;
        return this;
    }

    @MinDuration("1ms")
    public Duration getQueryHeartbeatInterval()
    {
        return queryHeartbeatInterval;
    }

    @Config("resource-manager.query-heartbeat-interval")
    public ResourceManagerConfig setQueryHeartbeatInterval(Duration queryHeartbeatInterval)
    {
        this.queryHeartbeatInterval = queryHeartbeatInterval;
        return this;
    }

    @MinDuration("1ms")
    public Duration getNodeHeartbeatInterval()
    {
        return nodeHeartbeatInterval;
    }

    @Config("resource-manager.node-heartbeat-interval")
    public ResourceManagerConfig setNodeHeartbeatInterval(Duration nodeHeartbeatInterval)
    {
        this.nodeHeartbeatInterval = nodeHeartbeatInterval;
        return this;
    }

    @Min(1)
    public int getHeartbeatThreads()
    {
        return heartbeatThreads;
    }

    @Config("resource-manager.heartbeat-threads")
    @ConfigDescription("Total number of timeout threads across all timeout thread pools")
    public ResourceManagerConfig setHeartbeatThreads(int heartbeatThreads)
    {
        this.heartbeatThreads = heartbeatThreads;
        return this;
    }

    @Min(1)
    public int getHeartbeatConcurrency()
    {
        return heartbeatConcurrency;
    }

    @Config("resource-manager.heartbeat-concurrency")
    @ConfigDescription("Number of thread pools to handle timeouts. Threads per pool is calculated by http-timeout-threads / http-timeout-concurrency")
    public ResourceManagerConfig setHeartbeatConcurrency(int heartbeatConcurrency)
    {
        this.heartbeatConcurrency = heartbeatConcurrency;
        return this;
    }

    @Min(1)
    public int getResourceManagerExecutorThreads()
    {
        return resourceManagerExecutorThreads;
    }

    @Config("resource-manager.executor-threads")
    public ResourceManagerConfig setResourceManagerExecutorThreads(int resourceManagerExecutorThreads)
    {
        this.resourceManagerExecutorThreads = resourceManagerExecutorThreads;
        return this;
    }

    @MinDuration("1ms")
    public Duration getProxyAsyncTimeout()
    {
        return proxyAsyncTimeout;
    }

    @Config("resource-manager.proxy-async-timeout")
    public ResourceManagerConfig setProxyAsyncTimeout(Duration proxyAsyncTimeout)
    {
        this.proxyAsyncTimeout = proxyAsyncTimeout;
        return this;
    }

    @MinDuration("1ms")
    public Duration getMemoryPoolFetchInterval()
    {
        return memoryPoolFetchInterval;
    }

    @Config("resource-manager.memory-pool-fetch-interval")
    public ResourceManagerConfig setMemoryPoolFetchInterval(Duration memoryPoolFetchInterval)
    {
        this.memoryPoolFetchInterval = memoryPoolFetchInterval;
        return this;
    }

    public boolean getResourceGroupServiceCacheEnabled()
    {
        return resourceGroupServiceCacheEnabled;
    }

    @Config("resource-manager.resource-group-service-cache-enabled")
    public ResourceManagerConfig setResourceGroupServiceCacheEnabled(Boolean resourceGroupServiceCacheEnabled)
    {
        this.resourceGroupServiceCacheEnabled = resourceGroupServiceCacheEnabled;
        return this;
    }

    @MinDuration("1ms")
    public Duration getResourceGroupServiceCacheExpireInterval()
    {
        return resourceGroupServiceCacheExpireInterval;
    }

    @Config("resource-manager.resource-group-service-cache-expire-interval")
    public ResourceManagerConfig setResourceGroupServiceCacheExpireInterval(Duration resourceGroupServiceCacheExpireInterval)
    {
        this.resourceGroupServiceCacheExpireInterval = resourceGroupServiceCacheExpireInterval;
        return this;
    }

    public Duration getResourceGroupServiceCacheRefreshInterval()
    {
        return resourceGroupServiceCacheRefreshInterval;
    }

    @Config("resource-manager.resource-group-service-cache-refresh-interval")
    public ResourceManagerConfig setResourceGroupServiceCacheRefreshInterval(Duration resourceGroupServiceCacheRefreshInterval)
    {
        this.resourceGroupServiceCacheRefreshInterval = resourceGroupServiceCacheRefreshInterval;
        return this;
    }
}
