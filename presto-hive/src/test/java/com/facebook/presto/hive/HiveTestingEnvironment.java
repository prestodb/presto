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
package com.facebook.presto.hive;

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.common.net.HostAndPort;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.joda.time.DateTimeZone;

import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.hive.HiveTestUtils.DEFAULT_HIVE_DATA_STREAM_FACTORIES;
import static com.facebook.presto.hive.HiveTestUtils.DEFAULT_HIVE_RECORD_CURSOR_PROVIDER;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class HiveTestingEnvironment
    implements TestingEnvironment
{
    private HdfsEnvironment hdfsEnvironment;
    private LocationService locationService;

    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorPageSourceProvider pageSourceProvider;
    private ConnectorPageSinkProvider pageSinkProvider;
    private HiveMetastore metastoreClient;

    private ExecutorService executor;
    private DateTimeZone timeZone;

    private HiveTestingEnvironment(
            HdfsEnvironment hdfsEnvironment,
            LocationService locationService,
            ConnectorMetadata metadata,
            HiveMetastore metastoreClient,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorSplitManager splitManager,
            ExecutorService executor,
            DateTimeZone timeZone)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.locationService = locationService;
        this.metadata = metadata;
        this.metastoreClient = metastoreClient;
        this.pageSinkProvider = pageSinkProvider;
        this.pageSourceProvider = pageSourceProvider;
        this.splitManager = splitManager;
        this.executor = executor;
        this.timeZone = timeZone;
    }

    public void close()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    public HdfsEnvironment getHdfsEnvironment()
    {
        return hdfsEnvironment;
    }

    public LocationService getLocationService()
    {
        return locationService;
    }

    public HiveMetastore getMetastoreClient()
    {
        return metastoreClient;
    }

    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    public DateTimeZone getTimeZone()
    {
        return timeZone;
    }

    public static HiveTestingEnvironment newInstance(String connector, String host, int port, String timeZoneId)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        hiveClientConfig.setTimeZone(timeZoneId);
        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        HiveCluster hiveCluster = new TestingHiveCluster(hiveClientConfig, host, port);
        HiveMetastore metastoreClient = new CachingHiveMetastore(hiveCluster, executor, Duration.valueOf("1m"), Duration.valueOf("15s"));
        HiveConnectorId connectorId = new HiveConnectorId(connector);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationUpdater(hiveClientConfig));

        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveClientConfig);
        LocationService locationService = new HiveLocationService(metastoreClient, hdfsEnvironment);
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        DateTimeZone timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZoneId));
        ConnectorMetadata metadata = new HiveMetadata(
                connectorId,
                metastoreClient,
                hdfsEnvironment,
                new HivePartitionManager(connectorId, hiveClientConfig),
                timeZone,
                10,
                true,
                true,
                true,
                true,
                true,
                TYPE_MANAGER,
                locationService,
                partitionUpdateCodec);
        ConnectorSplitManager splitManager = new HiveSplitManager(
                connectorId,
                metastoreClient,
                new NamenodeStats(),
                hdfsEnvironment,
                new HadoopDirectoryLister(),
                newDirectExecutorService(),
                100,
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxSplitSize(),
                hiveClientConfig.getMaxInitialSplitSize(),
                hiveClientConfig.getMaxInitialSplits(),
                false
        );
        ConnectorPageSinkProvider pageSinkProvider = new HivePageSinkProvider(hdfsEnvironment, metastoreClient, new GroupByHashPageIndexerFactory(), TYPE_MANAGER, new HiveClientConfig(), locationService, partitionUpdateCodec);
        ConnectorPageSourceProvider pageSourceProvider = new HivePageSourceProvider(hiveClientConfig, hdfsEnvironment, DEFAULT_HIVE_RECORD_CURSOR_PROVIDER, DEFAULT_HIVE_DATA_STREAM_FACTORIES, TYPE_MANAGER);

        return new HiveTestingEnvironment(
                hdfsEnvironment,
                locationService,
                metadata,
                metastoreClient,
                pageSinkProvider,
                pageSourceProvider,
                splitManager,
                executor,
                timeZone);
    }
}
