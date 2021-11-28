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
package com.facebook.presto.hive.cache;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheFactory;
import com.facebook.presto.cache.CacheManager;
import com.facebook.presto.cache.ForCachingFileSystem;
import com.facebook.presto.hadoop.FileSystemFactory;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.function.BiFunction;

import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HiveCachingHdfsConfiguration
        implements HdfsConfiguration
{
    private final HdfsConfiguration hiveHdfsConfiguration;
    private final CacheManager cacheManager;
    private final CacheConfig cacheConfig;
    private final CacheFactory cacheFactory;

    @Inject
    public HiveCachingHdfsConfiguration(
            @ForCachingFileSystem HdfsConfiguration hdfsConfiguration,
            CacheConfig cacheConfig,
            CacheManager cacheManager,
            CacheFactory cacheFactory)
    {
        this.hiveHdfsConfiguration = requireNonNull(hdfsConfiguration, "hiveHdfsConfiguration is null");
        this.cacheManager = requireNonNull(cacheManager, "CacheManager is null");
        this.cacheConfig = requireNonNull(cacheConfig, "cacheConfig is null");
        this.cacheFactory = requireNonNull(cacheFactory, "CacheFactory is null");
    }

    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        @SuppressWarnings("resource")
        Configuration config = new CachingJobConf((factoryConfig, factoryUri) -> {
            try {
                FileSystem fileSystem = (new Path(factoryUri)).getFileSystem(hiveHdfsConfiguration.getConfiguration(context, factoryUri));
                checkState(fileSystem instanceof ExtendedFileSystem);
                return cacheFactory.createCachingFileSystem(
                        factoryConfig,
                        factoryUri,
                        (ExtendedFileSystem) fileSystem,
                        cacheManager,
                        context.getSession().map(HiveSessionProperties::isCacheEnabled).orElse(cacheConfig.isCachingEnabled()),
                        cacheConfig.getCacheType(),
                        cacheConfig.isValidationEnabled());
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "cannot create caching file system", e);
            }
        });
        Configuration defaultConfig = hiveHdfsConfiguration.getConfiguration(context, uri);

        copy(defaultConfig, config);
        return config;
    }

    private static class CachingJobConf
            extends JobConf
            implements FileSystemFactory
    {
        private final BiFunction<Configuration, URI, FileSystem> factory;

        private CachingJobConf(BiFunction<Configuration, URI, FileSystem> factory)
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
