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
package com.facebook.presto.hive.cache.alluxio;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.alluxio.AlluxioCacheConfig;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsEnvironment;
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
    public void updateConfiguration(Configuration configuration, HdfsEnvironment.HdfsContext context, URI uri)
    {
        if (cacheConfig.isCachingEnabled() && cacheConfig.getCacheType() == ALLUXIO) {
            configuration.set("alluxio.user.local.cache.enabled", String.valueOf(cacheConfig.isCachingEnabled()));
            configuration.set("alluxio.user.client.cache.size", cacheConfig.getMaxInMemoryCacheSize().toString());
            configuration.set("alluxio.user.client.cache.dir", cacheConfig.getBaseDirectory().getPath());

            configuration.set("alluxio.user.metrics.collection.enabled", String.valueOf(alluxioCacheConfig.isMetricsCollectionEnabled()));
            configuration.set("sink.jmx.class", alluxioCacheConfig.getJmxClass());
            configuration.set("sink.jmx.domain", alluxioCacheConfig.getMetricsDomain());
        }
    }
}
