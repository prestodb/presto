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

import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.parquet.ParquetRecordCursorProvider;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.event.client.EventClient;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveClientModule
        implements Module
{
    private final String connectorId;

    public HiveClientModule(String connectorId)
    {
        this.connectorId = connectorId;
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
        configBinder(binder).bindConfig(HiveClientConfig.class);
        binder.bind(DirectoryLister.class).to(CachedDirectoryLister.class).in(Scopes.SINGLETON);

        binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(NamenodeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NamenodeStats.class).as(generatedNameOf(NamenodeStats.class, connectorId));

        binder.bind(CachedDirectoryLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CachedDirectoryLister.class).as(generatedNameOf(CachedDirectoryLister.class, connectorId));

        Multibinder<HiveRecordCursorProvider> recordCursorProviderBinder = newSetBinder(binder, HiveRecordCursorProvider.class);
        recordCursorProviderBinder.addBinding().to(ParquetRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(GenericHiveRecordCursorProvider.class).in(Scopes.SINGLETON);

        binder.bind(HiveWriterStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveWriterStats.class).as(generatedNameOf(HiveWriterStats.class, connectorId));

        newSetBinder(binder, EventClient.class).addBinding().to(HiveEventClient.class).in(Scopes.SINGLETON);
        binder.bind(HivePartitionManager.class).in(Scopes.SINGLETON);
        binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
        binder.bind(TableParameterCodec.class).in(Scopes.SINGLETON);
        binder.bind(HiveMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(HiveSplitManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ConnectorSplitManager.class).as(generatedNameOf(HiveSplitManager.class, connectorId));
        binder.bind(ConnectorPageSourceProvider.class).to(HivePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(HivePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).as(generatedNameOf(FileFormatDataSourceStats.class, connectorId));

        Multibinder<HivePageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, HivePageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(DwrfPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);

        Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder = newSetBinder(binder, HiveFileWriterFactory.class);
        binder.bind(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrcFileWriterFactory.class).as(generatedNameOf(OrcFileWriterFactory.class, connectorId));
        configBinder(binder).bindConfig(OrcFileWriterConfig.class);
        fileWriterFactoryBinder.addBinding().to(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);
    }

    @ForHiveClient
    @Singleton
    @Provides
    public ExecutorService createHiveClientExecutor(HiveConnectorId hiveClientId)
    {
        return newCachedThreadPool(daemonThreadsNamed("hive-" + hiveClientId + "-%s"));
    }

    @ForCachingHiveMetastore
    @Singleton
    @Provides
    public ExecutorService createCachingHiveMetastoreExecutor(HiveConnectorId hiveClientId, HiveClientConfig hiveClientConfig)
    {
        return newFixedThreadPool(
                hiveClientConfig.getMaxMetastoreRefreshThreads(),
                daemonThreadsNamed("hive-metastore-" + hiveClientId + "-%s"));
    }

    @Singleton
    @Provides
    public Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> createMetastoreGetter(HiveTransactionManager transactionManager)
    {
        return transactionHandle -> ((HiveMetadata) transactionManager.get(transactionHandle)).getMetastore();
    }
}
