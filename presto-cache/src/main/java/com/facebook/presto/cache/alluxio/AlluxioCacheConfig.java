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
package com.facebook.presto.cache.alluxio;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AlluxioCacheConfig
{
    private boolean metricsCollectionEnabled = true;
    private boolean asyncWriteEnabled;
    private String jmxClass = "alluxio.metrics.sink.JmxSink";
    private String metricsDomain = "com.facebook.alluxio";
    private DataSize maxCacheSize = new DataSize(2, GIGABYTE);
    private boolean configValidationEnabled;
    private boolean timeoutEnabled = true;
    private boolean cacheQuotaEnabled;
    private Duration timeoutDuration = new Duration(60, SECONDS);
    private int timeoutThreads = 64;
    private int evictionRetries = 10;
    private EvictionPolicy evictionPolicy = EvictionPolicy.LRU;
    private boolean shadowCacheEnabled;
    private Duration shadowCacheWindow = new Duration(7, DAYS);

    public boolean isMetricsCollectionEnabled()
    {
        return metricsCollectionEnabled;
    }

    @Config("cache.alluxio.metrics-enabled")
    @ConfigDescription("If metrics for alluxio caching are enabled")
    public AlluxioCacheConfig setMetricsCollectionEnabled(boolean metricsCollectionEnabled)
    {
        this.metricsCollectionEnabled = metricsCollectionEnabled;
        return this;
    }

    public String getJmxClass()
    {
        return jmxClass;
    }

    @Config("cache.alluxio.jmx-class")
    @ConfigDescription("JMX class name used by the alluxio caching for metrics")
    public AlluxioCacheConfig setJmxClass(String jmxClass)
    {
        this.jmxClass = jmxClass;
        return this;
    }

    public String getMetricsDomain()
    {
        return metricsDomain;
    }

    @Config("cache.alluxio.metrics-domain")
    @ConfigDescription("Metrics domain name used by the alluxio caching")
    public AlluxioCacheConfig setMetricsDomain(String metricsDomain)
    {
        this.metricsDomain = metricsDomain;
        return this;
    }

    public boolean isAsyncWriteEnabled()
    {
        return asyncWriteEnabled;
    }

    @Config("cache.alluxio.async-write-enabled")
    @ConfigDescription("If alluxio caching should write to cache asynchronously")
    public AlluxioCacheConfig setAsyncWriteEnabled(boolean asyncWriteEnabled)
    {
        this.asyncWriteEnabled = asyncWriteEnabled;
        return this;
    }

    public DataSize getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config("cache.alluxio.max-cache-size")
    @ConfigDescription("The maximum cache size available for alluxio cache")
    public AlluxioCacheConfig setMaxCacheSize(DataSize maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    @Config("cache.alluxio.config-validation-enabled")
    @ConfigDescription("If the alluxio caching should validate the provided configuration")
    public AlluxioCacheConfig setConfigValidationEnabled(boolean configValidationEnabled)
    {
        this.configValidationEnabled = configValidationEnabled;
        return this;
    }

    public boolean isConfigValidationEnabled()
    {
        return configValidationEnabled;
    }

    public int getTimeoutThreads()
    {
        return timeoutThreads;
    }

    @Config("cache.alluxio.timeout-threads")
    @ConfigDescription("Number of threads used to handle timeouts in alluxio caching")
    public AlluxioCacheConfig setTimeoutThreads(int timeoutThreads)
    {
        this.timeoutThreads = timeoutThreads;
        return this;
    }

    public Duration getTimeoutDuration()
    {
        return timeoutDuration;
    }

    @Config("cache.alluxio.timeout-duration")
    @ConfigDescription("Timeout duration for alluxio caching operations")
    public AlluxioCacheConfig setTimeoutDuration(Duration timeoutDuration)
    {
        this.timeoutDuration = timeoutDuration;
        return this;
    }

    public boolean getTimeoutEnabled()
    {
        return timeoutEnabled;
    }

    @Config("cache.alluxio.timeout-enabled")
    @ConfigDescription("Whether to enable timeout for alluxio caching operations")
    public AlluxioCacheConfig setTimeoutEnabled(boolean timeoutEnabled)
    {
        this.timeoutEnabled = timeoutEnabled;
        return this;
    }

    public int getEvictionRetries()
    {
        return evictionRetries;
    }

    @Config("cache.alluxio.eviction-retries")
    @ConfigDescription("The maximum number of eviction retries")
    public AlluxioCacheConfig setEvictionRetries(int evictionRetries)
    {
        this.evictionRetries = evictionRetries;
        return this;
    }

    public EvictionPolicy getEvictionPolicy()
    {
        return evictionPolicy;
    }

    @Config("cache.alluxio.eviction-policy")
    @ConfigDescription("The cache eviction policy")
    public AlluxioCacheConfig setEvictionPolicy(EvictionPolicy evictionPolicy)
    {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public boolean isCacheQuotaEnabled()
    {
        return cacheQuotaEnabled;
    }

    @Config("cache.alluxio.quota-enabled")
    @ConfigDescription("Whether to enable quota for alluxio caching operations")
    public AlluxioCacheConfig setCacheQuotaEnabled(boolean cacheQuotaEnabled)
    {
        this.cacheQuotaEnabled = cacheQuotaEnabled;
        return this;
    }

    public boolean isShadowCacheEnabled()
    {
        return shadowCacheEnabled;
    }

    @Config("cache.alluxio.shadow-cache-enabled")
    @ConfigDescription("Whether to enable alluxio shadow cache")
    public AlluxioCacheConfig setShadowCacheEnabled(boolean shadowCacheEnabled)
    {
        this.shadowCacheEnabled = shadowCacheEnabled;
        return this;
    }

    public Duration getShadowCacheWindow()
    {
        return shadowCacheWindow;
    }

    @Config("cache.alluxio.shadow-cache-window")
    @ConfigDescription("The time window of alluxio shadow cache for working set calculation")
    public AlluxioCacheConfig setShadowCacheWindow(Duration shadowCacheWindow)
    {
        this.shadowCacheWindow = shadowCacheWindow;
        return this;
    }
}
