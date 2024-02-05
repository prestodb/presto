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
package com.facebook.presto.statistic;

import com.facebook.airlift.configuration.Config;

public class RedisProviderConfig
{
    // Constants
    public static final String COORDINATOR_PROPERTY_NAME = "coordinator";
    public static final String SERVICE_NAME_PROPERTY_NAME = "hbo.redis-provider.config-name";
    public static final String SERVER_URI_PROPERTY_NAME = "hbo.redis-provider.server_uri";
    public static final String CLUSTER_PASSWORD_PROPERTY_NAME = "hbo.redis-provider.cluster-password";
    public static final String TOTAL_FETCH_TIMEOUT_PROPERTY_NAME = "hbo.redis-provider.total-fetch-timeoutms";
    public static final String DEFAULT_TTL_PROPERTY_NAME = "hbo.redis-provider.default-ttl-seconds";
    public static final String HBO_PROVIDER_ENABLED_NAME = "hbo.redis-provider.enabled";
    public static final String TOTAL_SET_TIMEOUT_PROPERTY_NAME = "hbo.redis-provider.total-set-timeoutms";
    public static final String REDIS_CREDENTIALS_PATH = "credentials-path";
    public static final String CLUSTER_MODE_ENABLED = "hbo.redis-provider.cluster-mode-enabled";
    public static final String REDIS_PROPERTIES_PATH = "etc/redis-provider.properties";
    private String coordinator;
    private String serviceName;
    private String serverUri;
    private String clusterPassword;
    private long totalFetchTimeoutMs;

    private long defaultTtlSeconds;
    private boolean hboProviderEnabled;

    private boolean clusterModeEnabled;
    private long totalSetTimeoutMs;
    private String credentialsPath;
    private String redisPropertiesPath;

    @Config("hbo.redis-provider.cluster-mode-enabled")
    public RedisProviderConfig setClusterModeEnabled(boolean value)
    {
        this.clusterModeEnabled = value;
        return this;
    }

    public boolean getClusterModeEnabled()
    {
        return clusterModeEnabled;
    }

    @Config("coordinator")
    public RedisProviderConfig setCoordinator(String value)
    {
        this.coordinator = value;
        return this;
    }

    public String getCoordinator()
    {
        return coordinator;
    }

    @Config("hbo.redis-provider.config-name")
    public RedisProviderConfig setServiceName(String value)
    {
        this.serviceName = value;
        return this;
    }

    public String getServiceName()
    {
        return serviceName;
    }

    @Config("hbo.redis-provider.server_uri")
    public RedisProviderConfig setServerUri(String value)
    {
        this.serverUri = value;
        return this;
    }

    public String getServerUri()
    {
        return serverUri;
    }

    @Config("hbo.redis-provider.cluster-password")
    public RedisProviderConfig setClusterPassword(String value)
    {
        this.clusterPassword = value;
        return this;
    }

    public String getClusterPassword()
    {
        return clusterPassword;
    }

    @Config("hbo.redis-provider.total-fetch-timeoutms")
    public RedisProviderConfig setTotalFetchTimeoutMs(long value)
    {
        this.totalFetchTimeoutMs = value;
        return this;
    }

    public long getTotalFetchTimeoutMs()
    {
        return totalFetchTimeoutMs;
    }

    @Config("hbo.redis-provider.default-ttl-seconds")
    public RedisProviderConfig setDefaultTtlSeconds(long value)
    {
        this.defaultTtlSeconds = value;
        return this;
    }

    public long getDefaultTtlSeconds()
    {
        return defaultTtlSeconds;
    }

    @Config("hbo.redis-provider.enabled")
    public RedisProviderConfig setHboProviderEnabled(boolean value)
    {
        this.hboProviderEnabled = value;
        return this;
    }

    public boolean getHboProviderEnabled()
    {
        return hboProviderEnabled;
    }

    @Config("hbo.redis-provider.total-set-timeoutms")
    public RedisProviderConfig setTotalSetTimeoutMs(long value)
    {
        this.totalSetTimeoutMs = value;
        return this;
    }

    public long getTotalSetTimeoutMs()
    {
        return totalSetTimeoutMs;
    }

    @Config("credentials-path")
    public RedisProviderConfig setCredentialsPath(String value)
    {
        this.credentialsPath = value;
        return this;
    }

    public String getCredentialsPath()
    {
        return credentialsPath;
    }
}
