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
package com.facebook.presto.cache;

import com.facebook.presto.cache.alluxio.AlluxioCacheConfig;
import com.facebook.presto.cache.alluxio.AlluxioCachingConfigurationProvider;
import com.facebook.presto.cache.filemerge.FileMergeCacheConfig;
import com.facebook.presto.cache.filemerge.FileMergeCacheManager;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.cache.CacheType.FILE_MERGE;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CachingModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(CacheStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CacheStats.class).withGeneratedName();
        configBinder(binder).bindConfig(CacheConfig.class);
        configBinder(binder).bindConfig(FileMergeCacheConfig.class);
        configBinder(binder).bindConfig(AlluxioCacheConfig.class);

        newSetBinder(binder, DynamicConfigurationProvider.class).addBinding().to(AlluxioCachingConfigurationProvider.class).in(Scopes.SINGLETON);

        binder.bind(CacheFactory.class).in(Scopes.SINGLETON);
    }

    //TODO: how to inject something with having constructor with parameter.
    @Singleton
    @Provides
    public CacheManager createCacheManager(CacheConfig cacheConfig, FileMergeCacheConfig fileMergeCacheConfig, CacheStats cacheStats)
    {
        if (cacheConfig.isCachingEnabled() && cacheConfig.getCacheType() == FILE_MERGE) {
            return new FileMergeCacheManager(
                    cacheConfig,
                    fileMergeCacheConfig,
                    cacheStats,
                    newScheduledThreadPool(5, daemonThreadsNamed("hive-cache-flusher-%s")),
                    newScheduledThreadPool(1, daemonThreadsNamed("hive-cache-remover-%s")),
                    newScheduledThreadPool(1, daemonThreadsNamed("hive-cache-size-calculator-%s")));
        }
        return new NoOpCacheManager();
    }
}
