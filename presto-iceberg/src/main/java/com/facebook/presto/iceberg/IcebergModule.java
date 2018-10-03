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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.CoercionPolicy;
import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.ForHiveClient;
import com.facebook.presto.hive.GenericHiveRecordCursorProvider;
import com.facebook.presto.hive.HadoopDirectoryLister;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationUpdater;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCoercionPolicy;
import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.HiveFileWriterFactory;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveLocationService;
import com.facebook.presto.hive.HiveNodePartitioningProvider;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HiveRecordCursorProvider;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveTableProperties;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.LocationService;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.PartitionUpdate;
import com.facebook.presto.hive.RcFileFileWriterFactory;
import com.facebook.presto.hive.TableParameterCodec;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.HiveCluster;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClientFactory;
import com.facebook.presto.hive.metastore.thrift.StaticHiveCluster;
import com.facebook.presto.hive.metastore.thrift.StaticMetastoreConfig;
import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.iceberg.parquet.ParquetRecordCursorProvider;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Created by parth on 2/3/18.
 */
public class IcebergModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;
    private final NodeManager nodeManager;
    private final PageIndexerFactory pageIndexerFactory;

    public IcebergModule(String connectorId, TypeManager typeManager, PageIndexerFactory pageIndexerFactory, NodeManager nodeManager)
    {
        this.connectorId = connectorId;
        this.typeManager = typeManager;
        this.nodeManager = nodeManager;
        this.pageIndexerFactory = pageIndexerFactory;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(HiveConnectorId.class).toInstance(new HiveConnectorId(connectorId));
        binder.bind(TypeTranslator.class).toInstance(new HiveTypeTranslator());
        binder.bind(CoercionPolicy.class).to(HiveCoercionPolicy.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfigurationUpdater.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        binder.bind(DirectoryLister.class).to(HadoopDirectoryLister.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HiveClientConfig.class);
        configBinder(binder).bindConfig(HiveS3Config.class);
        binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(NamenodeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NamenodeStats.class).as(generatedNameOf(NamenodeStats.class, connectorId));
        binder.bind(HiveMetastoreClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(HiveCluster.class).to(StaticHiveCluster.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StaticMetastoreConfig.class);
        configBinder(binder).bindConfig(OrcFileWriterConfig.class);
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(PageIndexerFactory.class).toInstance(pageIndexerFactory);
        binder.bind(ConnectorSplitManager.class).to(IcebergSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(IcebergPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(IcebergPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        Multibinder<HiveRecordCursorProvider> recordCursorProviderBinder = newSetBinder(binder, HiveRecordCursorProvider.class);
        recordCursorProviderBinder.addBinding().to(ParquetRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(GenericHiveRecordCursorProvider.class).in(Scopes.SINGLETON);
        binder.bind(HivePartitionManager.class).in(Scopes.SINGLETON);
        binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
        binder.bind(TableParameterCodec.class).in(Scopes.SINGLETON);
        binder.bind(IcebergMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);
        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).as(generatedNameOf(FileFormatDataSourceStats.class, connectorId));
        Multibinder<HivePageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, HivePageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(DwrfPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);
        Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder = newSetBinder(binder, HiveFileWriterFactory.class);
        fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);
    }

    @ForHiveClient
    @Singleton
    @Provides
    public ExecutorService createHiveClientExecutor(HiveConnectorId hiveClientId)
    {
        return newCachedThreadPool(daemonThreadsNamed("iceberg-" + hiveClientId + "-%s"));
    }

    @Singleton
    @Provides
    public Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> createMetastoreGetter(HiveTransactionManager transactionManager)
    {
        return transactionHandle -> ((IcebergMetadata) transactionManager.get(transactionHandle)).getMetastore();
    }
}
