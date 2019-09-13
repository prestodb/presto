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
package com.facebook.presto.pinot;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class PinotConfig
{
    private static final Duration DEFAULT_IDLE_TIMEOUT = new Duration(5, TimeUnit.MINUTES);
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = new Duration(1, TimeUnit.MINUTES);
    private static final int DEFAULT_MIN_CONNECTIONS_PER_SERVER = 10;
    private static final int DEFAULT_MAX_CONNECTIONS_PER_SERVER = 30;
    private static final int DEFAULT_MAX_BACKLOG_PER_SERVER = 30;
    private static final int DEFAULT_THREAD_POOL_SIZE = 64;
    private static final int DEFAULT_CORE_POOL_SIZE = 50;

    private static final long DEFAULT_LIMIT_ALL = 2147483647;

    private static final int DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN = 20;

    private String zookeeperUrl;
    private String pinotCluster;
    private String controllerUrl;

    private long limitAll = DEFAULT_LIMIT_ALL;

    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private Duration connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    // Maximum number of threads actively processing tasks.
    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

    // The number of threads to keep in the pool when connecting with Pinot, even if they are idle
    private int corePoolSize = DEFAULT_CORE_POOL_SIZE;

    // Minimum number of live connections for each server
    private int minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;

    // Maximum number of live connections for each server
    private int maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;

    // Maximum number of pending checkout requests before requests starts getting rejected
    private int maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;

    // Estimated size for non-numeric columns, used when processing Pages. Use heuristics to save calculating actual bytes at query time.
    private int estimatedSizeInBytesForNonNumericColumn = DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN;

    @NotNull
    public String getZookeeperUrl()
    {
        return zookeeperUrl;
    }

    @Config("zookeeper-uri")
    public PinotConfig setZookeeperUrl(String zookeeperUrl)
    {
        if (zookeeperUrl != null && zookeeperUrl.endsWith("/")) {
            zookeeperUrl = zookeeperUrl.substring(0, zookeeperUrl.length() - 1);
        }
        this.zookeeperUrl = zookeeperUrl;
        return this;
    }

    @NotNull
    public String getPinotCluster()
    {
        return pinotCluster;
    }

    @Config("pinot-cluster")
    public PinotConfig setPinotCluster(String pinotCluster)
    {
        this.pinotCluster = pinotCluster;
        return this;
    }

    @NotNull
    public String getControllerUrl()
    {
        return controllerUrl;
    }

    @Config("controller-url")
    public PinotConfig setControllerUrl(String controllerUrl)
    {
        this.controllerUrl = controllerUrl;
        return this;
    }

    @NotNull
    public long getLimitAll()
    {
        return limitAll;
    }

    @Config("limit-all")
    public PinotConfig setLimitAll(String limitAll)
    {
        try {
            this.limitAll = Long.valueOf(limitAll);
        }
        catch (Exception e) {
            this.limitAll = DEFAULT_LIMIT_ALL;
        }
        return this;
    }

    @NotNull
    public int getThreadPoolSize()
    {
        return threadPoolSize;
    }

    @Config("thread-pool-size")
    public PinotConfig setThreadPoolSize(String threadPoolSize)
    {
        try {
            this.threadPoolSize = Integer.valueOf(threadPoolSize);
        }
        catch (Exception e) {
            this.threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        }
        return this;
    }

    @NotNull
    public int getCorePoolSize()
    {
        return corePoolSize;
    }

    @Config("core-pool-size")
    public PinotConfig setCorePoolSize(String corePoolSize)
    {
        try {
            this.corePoolSize = Integer.valueOf(corePoolSize);
        }
        catch (Exception e) {
            this.corePoolSize = DEFAULT_CORE_POOL_SIZE;
        }
        return this;
    }

    @NotNull
    public int getMinConnectionsPerServer()
    {
        return minConnectionsPerServer;
    }

    @Config("min-connections-per-server")
    public PinotConfig setMinConnectionsPerServer(String minConnectionsPerServer)
    {
        try {
            this.minConnectionsPerServer = Integer.valueOf(minConnectionsPerServer);
        }
        catch (Exception e) {
            this.minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;
        }
        return this;
    }

    @NotNull
    public int getMaxConnectionsPerServer()
    {
        return maxConnectionsPerServer;
    }

    @Config("max-connections-per-server")
    public PinotConfig setMaxConnectionsPerServer(String maxConnectionsPerServer)
    {
        try {
            this.maxConnectionsPerServer = Integer.valueOf(maxConnectionsPerServer);
        }
        catch (Exception e) {
            this.maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;
        }
        return this;
    }

    @NotNull
    public int getMaxBacklogPerServer()
    {
        return maxBacklogPerServer;
    }

    @Config("max-backlog-per-server")
    public PinotConfig setMaxBacklogPerServer(String maxBacklogPerServer)
    {
        try {
            this.maxBacklogPerServer = Integer.valueOf(maxBacklogPerServer);
        }
        catch (Exception e) {
            this.maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;
        }
        return this;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getIdleTimeout()
    {
        return idleTimeout;
    }

    @Config("idle-timeout")
    public PinotConfig setIdleTimeout(Duration idleTimeout)
    {
        this.idleTimeout = idleTimeout;
        return this;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("connection-timeout")
    public PinotConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @NotNull
    public int getEstimatedSizeInBytesForNonNumericColumn()
    {
        return estimatedSizeInBytesForNonNumericColumn;
    }

    @Config("estimated-size-in-bytes-for-non-numeric-column")
    public PinotConfig setEstimatedSizeInBytesForNonNumericColumn(int estimatedSizeInBytesForNonNumericColumn)
    {
        try {
            this.estimatedSizeInBytesForNonNumericColumn = Integer.valueOf(estimatedSizeInBytesForNonNumericColumn);
        }
        catch (Exception e) {
            this.estimatedSizeInBytesForNonNumericColumn = DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN;
        }
        return this;
    }
}
