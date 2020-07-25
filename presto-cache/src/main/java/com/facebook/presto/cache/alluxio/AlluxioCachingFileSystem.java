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

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.LocalCacheFileSystem;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.grpc.OpenFilePOptions;
import alluxio.hadoop.AbstractFileSystem;
import alluxio.hadoop.HadoopConfigurationUtils;
import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;
import alluxio.util.ConfigurationUtils;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class AlluxioCachingFileSystem
        extends CachingFileSystem
{
    private final boolean cacheValidationEnabled;
    private AlluxioCachingFileSystemInternal cachingFileSystem;

    public AlluxioCachingFileSystem(ExtendedFileSystem dataTier, URI uri)
    {
        this(dataTier, uri, false);
    }

    public AlluxioCachingFileSystem(ExtendedFileSystem dataTier, URI uri, boolean cacheValidationEnabled)
    {
        super(dataTier, uri);
        this.cacheValidationEnabled = cacheValidationEnabled;
    }

    @Override
    public synchronized void initialize(URI uri, Configuration configuration)
            throws IOException
    {
        this.cachingFileSystem = new AlluxioCachingFileSystemInternal(uri, dataTier, cacheValidationEnabled);
        cachingFileSystem.initialize(uri, configuration);
    }

    @Override
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        if (hiveFileContext.isCacheable()) {
            return cachingFileSystem.openFile(path, hiveFileContext);
        }
        return dataTier.openFile(path, hiveFileContext);
    }

    private static class AlluxioCachingFileSystemInternal
            extends AbstractFileSystem
    {
        // The filesystem to query on cache miss
        private final URI uri;
        private final ExtendedFileSystem fileSystem;
        private final boolean cacheValidationEnabled;

        AlluxioCachingFileSystemInternal(URI uri, ExtendedFileSystem fileSystem, boolean cacheValidationEnabled)
        {
            this.uri = requireNonNull(uri, "uri is null");
            this.fileSystem = requireNonNull(fileSystem, "filesystem is null");
            this.cacheValidationEnabled = cacheValidationEnabled;
        }

        @Override
        public synchronized void initialize(URI uri, Configuration configuration)
                throws IOException
        {
            // Set statistics
            setConf(configuration);
            statistics = getStatistics(uri.getScheme(), getClass());

            // Take the URI properties, hadoop configuration, and given Alluxio configuration and merge
            // all three into a single object.
            Map<String, Object> configurationFromUri = getConfigurationFromUri(uri);
            AlluxioProperties alluxioProperties = ConfigurationUtils.defaults();
            InstancedConfiguration newConfiguration = HadoopConfigurationUtils.mergeHadoopConfiguration(configuration, alluxioProperties);
            // Connection details in the URI has the highest priority
            newConfiguration.merge(configurationFromUri, Source.RUNTIME);
            mAlluxioConf = newConfiguration;

            // Handle metrics
            Properties metricsProperties = new Properties();
            for (Map.Entry<String, String> entry : configuration) {
                metricsProperties.setProperty(entry.getKey(), entry.getValue());
            }
            MetricsSystem.startSinksFromConfig(new MetricsConfig(metricsProperties));
            mFileSystem = new LocalCacheFileSystem(new AlluxioCachingClientFileSystem(fileSystem, mAlluxioConf), mAlluxioConf);
            super.initialize(uri, configuration);
        }

        public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
                throws Exception
        {
            // URIStatus is the mechanism to pass the hiveFileContext to the source filesystem
            URIStatus uriStatus = mFileSystem.getStatus(getAlluxioPath(path));
            AlluxioURIStatus alluxioURIStatus = new AlluxioURIStatus(uriStatus.getFileInfo(), hiveFileContext);
            FileInStream fileInStream = mFileSystem.openFile(alluxioURIStatus, OpenFilePOptions.getDefaultInstance());
            return new FSDataInputStream(new AlluxioCachingHdfsFileInputStream(
                    fileInStream,
                    (cacheValidationEnabled ? Optional.of(fileSystem.openFile(path, hiveFileContext)) : Optional.empty()),
                    cacheValidationEnabled));
        }

        @Override
        public String getScheme()
        {
            return uri.getScheme();
        }

        @Override
        protected boolean isZookeeperMode()
        {
            return mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
        }

        @Override
        protected Map<String, Object> getConfigurationFromUri(URI uri)
        {
            // local cache doesn't use URI authority for connection.
            return ImmutableMap.of();
        }

        @Override
        protected void validateFsUri(URI uri)
        {
        }

        @Override
        protected String getFsScheme(URI uri)
        {
            return getScheme();
        }

        @Override
        protected AlluxioURI getAlluxioPath(Path path)
        {
            return new AlluxioURI(path.toString());
        }

        @Override
        protected Path getFsPath(String uriHeader, URIStatus fileStatus)
        {
            return new Path(uriHeader + fileStatus.getPath());
        }
    }
}
