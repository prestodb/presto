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
package com.facebook.presto.hive.s3select;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.AbstractTestHiveFileSystem.TestingHiveMetastore;
import com.facebook.presto.hive.CachingDirectoryLister;
import com.facebook.presto.hive.ColumnConverterProvider;
import com.facebook.presto.hive.ConfigBasedCacheQuotaRequirementProvider;
import com.facebook.presto.hive.HadoopDirectoryLister;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCoercionPolicy;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveCommonClientConfig;
import com.facebook.presto.hive.HiveEncryptionInformationProvider;
import com.facebook.presto.hive.HiveFileRenamer;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveLocationService;
import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HivePageSourceProvider;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HivePartitionObjectBuilder;
import com.facebook.presto.hive.HivePartitionSkippabilityChecker;
import com.facebook.presto.hive.HivePartitionStats;
import com.facebook.presto.hive.HiveSplitManager;
import com.facebook.presto.hive.HiveStagingFileCommitter;
import com.facebook.presto.hive.HiveTableWritabilityChecker;
import com.facebook.presto.hive.HiveTestUtils;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.HiveZeroRowFileCreator;
import com.facebook.presto.hive.LocationService;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.TableParameterCodec;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.TestingHiveCluster;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreConfig;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.facebook.presto.hive.statistics.QuickStatsProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.HiveFileSystemTestUtils.filterTable;
import static com.facebook.presto.hive.HiveFileSystemTestUtils.getSplitsCount;
import static com.facebook.presto.hive.HiveTestUtils.DO_NOTHING_DIRECTORY_LISTER;
import static com.facebook.presto.hive.HiveTestUtils.FILTER_STATS_CALCULATOR_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_RESOLUTION;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveAggregatedPageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveBatchPageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveSelectivePageSourceFactories;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultS3HiveRecordCursorProvider;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.util.Strings.isNullOrEmpty;

public class S3SelectTestHelper
        implements AutoCloseable
{
    private HdfsEnvironment hdfsEnvironment;
    private LocationService locationService;
    private TestingHiveMetastore metastoreClient;
    private HiveMetadataFactory metadataFactory;
    private HiveTransactionManager transactionManager;
    private ConnectorSplitManager splitManager;
    private ConnectorPageSourceProvider pageSourceProvider;

    private ExecutorService executor;
    private HiveClientConfig config;
    private CacheConfig cacheConfig;
    private MetastoreClientConfig metastoreClientConfig;
    private ThriftHiveMetastoreConfig thriftHiveMetastoreConfig;

    public S3SelectTestHelper(String host,
            int port,
            String databaseName,
            String awsAccessKey,
            String awsSecretKey,
            String writableBucket,
            HiveClientConfig hiveClientConfig)
    {
        checkArgument(!isNullOrEmpty(host), "Expected non empty host");
        checkArgument(!isNullOrEmpty(databaseName), "Expected non empty databaseName");
        checkArgument(!isNullOrEmpty(awsAccessKey), "Expected non empty awsAccessKey");
        checkArgument(!isNullOrEmpty(awsSecretKey), "Expected non empty awsSecretKey");
        checkArgument(!isNullOrEmpty(writableBucket), "Expected non empty writableBucket");

        config = hiveClientConfig;
        cacheConfig = new CacheConfig();
        metastoreClientConfig = new MetastoreClientConfig();
        thriftHiveMetastoreConfig = new ThriftHiveMetastoreConfig();

        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            metastoreClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        HiveCluster hiveCluster = new TestingHiveCluster(metastoreClientConfig, thriftHiveMetastoreConfig, host, port, new HiveCommonClientConfig());
        executor = newCachedThreadPool(daemonThreadsNamed("hive-%s"));
        HivePartitionManager hivePartitionManager = new HivePartitionManager(FUNCTION_AND_TYPE_MANAGER, config);

        S3ConfigurationUpdater s3Config = new PrestoS3ConfigurationUpdater(new HiveS3Config()
                .setS3AwsAccessKey(awsAccessKey)
                .setS3AwsSecretKey(awsSecretKey));
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config, metastoreClientConfig, s3Config, ignored -> {}, ignored -> {}), ImmutableSet.of(), config);

        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        ColumnConverterProvider columnConverterProvider = HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER;
        metastoreClient = new TestingHiveMetastore(
                new BridgingHiveMetastore(new ThriftHiveMetastore(hiveCluster, metastoreClientConfig, hdfsEnvironment), new HivePartitionMutator()),
                executor,
                metastoreClientConfig,
                new Path(format("s3://%s/", writableBucket)),
                hdfsEnvironment);
        locationService = new HiveLocationService(hdfsEnvironment);
        metadataFactory = new HiveMetadataFactory(
                config,
                metastoreClientConfig,
                metastoreClient,
                hdfsEnvironment,
                hivePartitionManager,
                newDirectExecutorService(),
                FUNCTION_AND_TYPE_MANAGER,
                locationService,
                FUNCTION_RESOLUTION,
                ROW_EXPRESSION_SERVICE,
                FILTER_STATS_CALCULATOR_SERVICE,
                new TableParameterCodec(),
                HiveTestUtils.PARTITION_UPDATE_CODEC,
                HiveTestUtils.PARTITION_UPDATE_SMILE_CODEC,
                new HiveTypeTranslator(),
                new HiveStagingFileCommitter(hdfsEnvironment, listeningDecorator(executor)),
                new HiveZeroRowFileCreator(hdfsEnvironment, new OutputStreamDataSinkFactory(), listeningDecorator(executor)),
                new NodeVersion("test_version"),
                new HivePartitionObjectBuilder(),
                new HiveEncryptionInformationProvider(ImmutableSet.of()),
                new HivePartitionStats(),
                new HiveFileRenamer(),
                columnConverterProvider,
                new QuickStatsProvider(metastoreClient, hdfsEnvironment, DO_NOTHING_DIRECTORY_LISTER, new HiveClientConfig(), new NamenodeStats(), ImmutableList.of()),
                new HiveTableWritabilityChecker(config));
        transactionManager = new HiveTransactionManager();
        splitManager = new HiveSplitManager(
                transactionManager,
                new NamenodeStats(),
                hdfsEnvironment,
                new CachingDirectoryLister(new HadoopDirectoryLister(), new HiveClientConfig()),
                new BoundedExecutor(executor, config.getMaxSplitIteratorThreads()),
                new HiveCoercionPolicy(FUNCTION_AND_TYPE_MANAGER),
                new CounterStat(),
                config.getMaxOutstandingSplits(),
                config.getMaxOutstandingSplitsSize(),
                config.getMinPartitionBatchSize(),
                config.getMaxPartitionBatchSize(),
                config.getSplitLoaderConcurrency(),
                config.getRecursiveDirWalkerEnabled(),
                new ConfigBasedCacheQuotaRequirementProvider(cacheConfig),
                new HiveEncryptionInformationProvider(ImmutableSet.of()),
                new HivePartitionSkippabilityChecker());
        pageSourceProvider = new HivePageSourceProvider(
                config,
                hdfsEnvironment,
                getDefaultS3HiveRecordCursorProvider(config, metastoreClientConfig),
                getDefaultHiveBatchPageSourceFactories(config, metastoreClientConfig),
                getDefaultHiveSelectivePageSourceFactories(config, metastoreClientConfig),
                getDefaultHiveAggregatedPageSourceFactories(config, metastoreClientConfig),
                FUNCTION_AND_TYPE_MANAGER,
                ROW_EXPRESSION_SERVICE);
    }

    public void tearDown()
    {
        hdfsEnvironment = null;
        locationService = null;
        metastoreClient = null;
        metadataFactory = null;
        transactionManager = null;
        splitManager = null;
        pageSourceProvider = null;
        config = null;
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    public S3SelectTestHelper(String host,
            int port,
            String databaseName,
            String awsAccessKey,
            String awsSecretKey,
            String writableBucket)
    {
        this(host, port, databaseName, awsAccessKey, awsSecretKey, writableBucket, new HiveClientConfig().setS3SelectPushdownEnabled(true));
    }

    public HiveMetadataFactory getMetadataFactory()
    {
        return metadataFactory;
    }

    public HiveTransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    public HiveClientConfig getConfig()
    {
        return config;
    }

    public int getTableSplitsCount(SchemaTableName table)
    {
        return getSplitsCount(
                table,
                transactionManager,
                config,
                metadataFactory,
                splitManager);
    }

    MaterializedResult getFilteredTableResult(SchemaTableName table, HiveColumnHandle column)
    {
        try {
            return filterTable(
                    table,
                    ImmutableList.of(column),
                    transactionManager,
                    config,
                    metadataFactory,
                    pageSourceProvider,
                    splitManager);
        }
        catch (IOException ignored) {
        }

        return null;
    }

    public static MaterializedResult expectedResult(ConnectorSession session, int start, int end)
    {
        MaterializedResult.Builder builder = MaterializedResult.resultBuilder(session, BIGINT);
        LongStream.rangeClosed(start, end).forEach(builder::row);
        return builder.build();
    }

    public static boolean isSplitCountInOpenInterval(int splitCount,
            int lowerBound,
            int upperBound)
    {
        // Split number may vary, the minimum number of splits being obtained with
        // the first split of maxInitialSplitSize and the rest of maxSplitSize
        return lowerBound < splitCount && splitCount < upperBound;
    }

    @Override
    public void close()
    {
        tearDown();
    }
}
