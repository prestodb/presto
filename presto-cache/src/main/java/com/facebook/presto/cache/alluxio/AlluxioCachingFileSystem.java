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
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.LocalCacheFileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.hadoop.AbstractFileSystem;
import alluxio.hadoop.HadoopConfigurationUtils;
import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;
import alluxio.uri.MultiMasterAuthority;
import alluxio.uri.SingleMasterAuthority;
import alluxio.uri.ZookeeperAuthority;
import alluxio.util.ConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AlluxioCachingFileSystem
        extends AbstractFileSystem
{
    private final FileSystem fileSystem;
    private final Configuration configuration;
    private final CacheFactory cacheFactory;

    public AlluxioCachingFileSystem(Configuration configuration, URI uri, FileSystem fileSystem, CacheFactory cacheFactory)
    {
        this.fileSystem = fileSystem;
        this.configuration = configuration;
        this.cacheFactory = cacheFactory;
        initialize(uri, configuration);
    }

    @Override
    public synchronized void initialize(URI uri, Configuration conf)
    {
        // Set statistics
        setConf(conf);
        statistics = getStatistics(uri.getScheme(), getClass());

        // Take the URI properties, hadoop configuration, and given Alluxio configuration and merge
        // all three into a single object.
        Map<String, Object> uriConfProperties = getConfigurationFromUri(uri);
        AlluxioProperties alluxioProps = ConfigurationUtils.defaults();
        InstancedConfiguration newConf = HadoopConfigurationUtils.mergeHadoopConfiguration(conf,
                alluxioProps);
        // Connection details in the URI has the highest priority
        newConf.merge(uriConfProperties, Source.RUNTIME);
        mAlluxioConf = newConf;

        // Handle metrics
        Properties metricsProps = new Properties();
        for (Map.Entry<String, String> e : conf) {
            metricsProps.setProperty(e.getKey(), e.getValue());
        }
        MetricsSystem.startSinksFromConfig(new MetricsConfig(metricsProps));
        mFileSystem = new LocalCacheFileSystem(cacheFactory.getAlluxioCachingClientFileSystem(fileSystem, mAlluxioConf), mAlluxioConf);
    }

    private AlluxioCachingClientFileSystem getAlluxioCachingClientFileSystem(FileSystem fileSystem, AlluxioConfiguration alluxioConfiguration)
    {
        return new AlluxioCachingClientFileSystem(fileSystem, alluxioConfiguration);
    }

    @Override
    public String getScheme()
    {
        return fileSystem.getScheme();
    }

    @Override
    protected boolean isZookeeperMode()
    {
        return this.mFileSystem.getConf().getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
    }

    @Override
    protected Map<String, Object> getConfigurationFromUri(URI uri)
    {
        AlluxioURI alluxioUri = new AlluxioURI(uri.toString());
        Map<String, Object> alluxioConfProperties = new HashMap<>();

        if (alluxioUri.getAuthority() instanceof ZookeeperAuthority) {
            ZookeeperAuthority authority = (ZookeeperAuthority) alluxioUri.getAuthority();
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), true);
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(),
                    authority.getZookeeperAddress());
        }
        else if (alluxioUri.getAuthority() instanceof SingleMasterAuthority) {
            SingleMasterAuthority authority = (SingleMasterAuthority) alluxioUri.getAuthority();
            alluxioConfProperties.put(PropertyKey.MASTER_HOSTNAME.getName(), authority.getHost());
            alluxioConfProperties.put(PropertyKey.MASTER_RPC_PORT.getName(), authority.getPort());
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), false);
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), null);
            // Unset the embedded journal related configuration
            // to support alluxio URI has the highest priority
            alluxioConfProperties.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES.getName(), null);
            alluxioConfProperties.put(PropertyKey.MASTER_RPC_ADDRESSES.getName(), null);
        }
        else if (alluxioUri.getAuthority() instanceof MultiMasterAuthority) {
            MultiMasterAuthority authority = (MultiMasterAuthority) alluxioUri.getAuthority();
            alluxioConfProperties.put(PropertyKey.MASTER_RPC_ADDRESSES.getName(),
                    authority.getMasterAddresses());
            // Unset the zookeeper configuration to support alluxio URI has the highest priority
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ENABLED.getName(), false);
            alluxioConfProperties.put(PropertyKey.ZOOKEEPER_ADDRESS.getName(), null);
        }
        return alluxioConfProperties;
    }

    @Override
    protected void validateFsUri(URI fsUri)
    {
    }

    @Override
    protected String getFsScheme(URI fsUri)
    {
        return getScheme();
    }

    @Override
    protected AlluxioURI getAlluxioPath(Path path)
    {
        return new AlluxioURI(path.toString());
    }

    @Override
    protected Path getFsPath(String fsUriHeader, URIStatus fileStatus)
    {
        return new Path(fsUriHeader + fileStatus.getPath());
    }
}
