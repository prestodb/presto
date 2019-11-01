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
package com.facebook.presto.raptor.filesystem;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheManager;
import com.facebook.presto.cache.CacheStats;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.cache.ForCachingFileSystem;
import com.facebook.presto.cache.LocalRangeCacheManager;
import com.facebook.presto.cache.NoOpCacheManager;
import com.facebook.presto.hadoop.FileSystemFactory;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.function.BiFunction;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class RaptorCachingHdfsConfiguration
        implements RaptorHdfsConfiguration
{
    private final RaptorHdfsConfiguration hiveHdfsConfiguration;
    private final CacheManager cacheManager;
    private final boolean cacheValidationEnabled;

    @Inject
    public RaptorCachingHdfsConfiguration(
            @ForCachingFileSystem RaptorHdfsConfiguration hiveHdfsConfiguration,
            CacheConfig cacheConfig,
            CacheStats cacheStats)
    {
        this.hiveHdfsConfiguration = requireNonNull(hiveHdfsConfiguration, "hiveHdfsConfiguration is null");

        CacheConfig config = requireNonNull(cacheConfig, "cachingFileSystemConfig is null");
        this.cacheManager = config.getBaseDirectory() == null ?
                new NoOpCacheManager() :
                new LocalRangeCacheManager(
                        cacheConfig,
                        cacheStats,
                        newScheduledThreadPool(5, daemonThreadsNamed("raptor-cache-flusher-%s")),
                        newScheduledThreadPool(1, daemonThreadsNamed("raptor-cache-remover-%s")));
        this.cacheValidationEnabled = cacheConfig.isValidationEnabled();
    }

    @Override
    public Configuration getConfiguration(FileSystemContext context, URI uri)
    {
        @SuppressWarnings("resource")
        Configuration config = new CachingJobConf((factoryConfig, factoryUri) -> {
            try {
                return new CachingFileSystem(
                        factoryUri,
                        factoryConfig,
                        cacheManager,
                        (new Path(uri)).getFileSystem(hiveHdfsConfiguration.getConfiguration(context, uri)),
                        cacheValidationEnabled);
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "cannot create caching FS", e);
            }
        });
        Configuration defaultConfig = hiveHdfsConfiguration.getConfiguration(context, uri);

        copy(defaultConfig, config);
        return config;
    }

    private static void copy(Configuration from, Configuration to)
    {
        for (Map.Entry<String, String> entry : from) {
            to.set(entry.getKey(), entry.getValue());
        }
    }

    private static class CachingJobConf
            extends JobConf
            implements FileSystemFactory
    {
        private final BiFunction<Configuration, URI, FileSystem> factory;

        public CachingJobConf(BiFunction<Configuration, URI, FileSystem> factory)
        {
            super(false);
            this.factory = requireNonNull(factory, "factory is null");
        }

        @Override
        public FileSystem createFileSystem(URI uri)
        {
            return factory.apply(this, uri);
        }
    }
}
