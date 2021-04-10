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

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.presto.cache.CacheType.ALLUXIO;

public class AlluxioCachingConfigurationProvider
        implements DynamicConfigurationProvider
{
    private final CacheConfig cacheConfig;
    private final AlluxioCacheConfig alluxioCacheConfig;

    @Inject
    public AlluxioCachingConfigurationProvider(CacheConfig cacheConfig, AlluxioCacheConfig alluxioCacheConfig)
    {
        this.cacheConfig = cacheConfig;
        this.alluxioCacheConfig = alluxioCacheConfig;
    }

    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        if (cacheConfig.isCachingEnabled() && cacheConfig.getCacheType() == ALLUXIO) {
            configuration.set("alluxio.user.local.cache.enabled", String.valueOf(cacheConfig.isCachingEnabled()));
            if (cacheConfig.getBaseDirectory() != null) {
                configuration.set("alluxio.user.client.cache.dir", cacheConfig.getBaseDirectory().getPath());
            }
            configuration.set("alluxio.user.client.cache.size", alluxioCacheConfig.getMaxCacheSize().toString());
            configuration.set("alluxio.user.client.cache.async.write.enabled", String.valueOf(alluxioCacheConfig.isAsyncWriteEnabled()));
            configuration.set("alluxio.user.metrics.collection.enabled", String.valueOf(alluxioCacheConfig.isMetricsCollectionEnabled()));
            configuration.set("alluxio.user.client.cache.eviction.retries", String.valueOf(alluxioCacheConfig.getEvictionRetries()));
            configuration.set("alluxio.user.client.cache.evictor.class", alluxioCacheConfig.getEvictionPolicy().getClassName());
            configuration.set("alluxio.user.client.cache.quota.enabled", String.valueOf(alluxioCacheConfig.isCacheQuotaEnabled()));
            configuration.set("sink.jmx.class", alluxioCacheConfig.getJmxClass());
            configuration.set("sink.jmx.domain", alluxioCacheConfig.getMetricsDomain());
            configuration.set("alluxio.conf.validation.enabled", String.valueOf(alluxioCacheConfig.isConfigValidationEnabled()));
            if (alluxioCacheConfig.getTimeoutEnabled()) {
                configuration.set("alluxio.user.client.cache.timeout.duration", String.valueOf(alluxioCacheConfig.getTimeoutDuration().toMillis()));
                configuration.set("alluxio.user.client.cache.timeout.threads", String.valueOf(alluxioCacheConfig.getTimeoutThreads()));
            }
            else {
                configuration.set("alluxio.user.client.cache.timeout.duration", "-1");
            }
        }
    }
}
