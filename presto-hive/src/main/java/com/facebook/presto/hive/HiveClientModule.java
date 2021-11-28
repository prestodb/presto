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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.concurrent.ExecutorServiceAdapter;
import com.facebook.airlift.event.client.EventClient;
import com.facebook.presto.cache.ForCachingFileSystem;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider.ForCryptoService;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider.ForUnknown;
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration;
import com.facebook.presto.hive.datasink.DataSinkFactory;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.orc.DwrfBatchPageSourceFactory;
import com.facebook.presto.hive.orc.DwrfSelectivePageSourceFactory;
import com.facebook.presto.hive.orc.OrcBatchPageSourceFactory;
import com.facebook.presto.hive.orc.OrcSelectivePageSourceFactory;
import com.facebook.presto.hive.orc.TupleDomainFilterCache;
import com.facebook.presto.hive.pagefile.PageFilePageSourceFactory;
import com.facebook.presto.hive.pagefile.PageFileWriterFactory;
import com.facebook.presto.hive.parquet.ParquetFileWriterFactory;
import com.facebook.presto.hive.parquet.ParquetPageSourceFactory;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.hive.rule.HivePlanOptimizerProvider;
import com.facebook.presto.hive.s3.PrestoS3ClientFactory;
import com.facebook.presto.orc.CachingStripeMetadataSource;
import com.facebook.presto.orc.DwrfAwareStripeMetadataSourceFactory;
import com.facebook.presto.orc.EncryptionLibrary;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.StripeReader.StripeId;
import com.facebook.presto.orc.StripeReader.StripeStreamId;
import com.facebook.presto.orc.UnsupportedEncryptionLibrary;
import com.facebook.presto.orc.cache.CachingOrcFileTailSource;
import com.facebook.presto.orc.cache.OrcCacheConfig;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.cache.CachingParquetMetadataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.cache.ParquetCacheConfig;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdaterProvider;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import io.airlift.slice.Slice;
import org.weakref.jmx.MBeanExporter;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.json.smile.SmileCodecBinder.smileCodecBinder;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

        binder.bind(HdfsConfigurationInitializer.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class);
        configBinder(binder).bindConfig(HiveClientConfig.class);

        binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveAnalyzeProperties.class).in(Scopes.SINGLETON);

        binder.bind(NamenodeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NamenodeStats.class).as(generatedNameOf(NamenodeStats.class, connectorId));

        binder.bind(PrestoS3ClientFactory.class).in(Scopes.SINGLETON);

        binder.bind(DirectoryLister.class).annotatedWith(ForCachingDirectoryLister.class).to(HadoopDirectoryLister.class).in(Scopes.SINGLETON);
        binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DirectoryLister.class)
                .as(generatedNameOf(CachingDirectoryLister.class, connectorId));

        Multibinder<HiveRecordCursorProvider> recordCursorProviderBinder = newSetBinder(binder, HiveRecordCursorProvider.class);
        recordCursorProviderBinder.addBinding().to(S3SelectRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(GenericHiveRecordCursorProvider.class).in(Scopes.SINGLETON);

        binder.bind(HiveWriterStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveWriterStats.class).as(generatedNameOf(HiveWriterStats.class, connectorId));

        binder.bind(HiveFileRenamer.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveFileRenamer.class).as(generatedNameOf(HiveFileRenamer.class, connectorId));

        newSetBinder(binder, EventClient.class).addBinding().to(HiveEventClient.class).in(Scopes.SINGLETON);
        binder.bind(HivePartitionManager.class).in(Scopes.SINGLETON);
        binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
        binder.bind(TableParameterCodec.class).in(Scopes.SINGLETON);
        binder.bind(HivePartitionStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HivePartitionStats.class).as(generatedNameOf(HivePartitionStats.class, connectorId));
        binder.bind(HiveMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(new TypeLiteral<Supplier<TransactionalMetadata>>() {}).to(HiveMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(StagingFileCommitter.class).to(HiveStagingFileCommitter.class).in(Scopes.SINGLETON);
        binder.bind(ZeroRowFileCreator.class).to(HiveZeroRowFileCreator.class).in(Scopes.SINGLETON);
        binder.bind(PartitionObjectBuilder.class).to(HivePartitionObjectBuilder.class).in(Scopes.SINGLETON);
        binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(HiveSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CacheQuotaRequirementProvider.class).to(ConfigBasedCacheQuotaRequirementProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ConnectorSplitManager.class).as(generatedNameOf(HiveSplitManager.class, connectorId));
        binder.bind(ConnectorPageSourceProvider.class).to(HivePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(HivePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPlanOptimizerProvider.class).to(HivePlanOptimizerProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadataUpdaterProvider.class).to(HiveMetadataUpdaterProvider.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);
        smileCodecBinder(binder).bindSmileCodec(PartitionUpdate.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).as(generatedNameOf(FileFormatDataSourceStats.class, connectorId));

        binder.bind(EncryptionLibrary.class).annotatedWith(ForCryptoService.class).to(UnsupportedEncryptionLibrary.class).in(Scopes.SINGLETON);
        binder.bind(EncryptionLibrary.class).annotatedWith(ForUnknown.class).to(UnsupportedEncryptionLibrary.class).in(Scopes.SINGLETON);
        binder.bind(HiveDwrfEncryptionProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetCacheConfig.class, connectorId);
        Multibinder<HiveBatchPageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, HiveBatchPageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(OrcBatchPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(DwrfBatchPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(PageFilePageSourceFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(OrcCacheConfig.class, connectorId);

        binder.bind(TupleDomainFilterCache.class).in(Scopes.SINGLETON);

        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForMetastoreHdfsEnvironment.class).to(HiveCachingHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForCachingFileSystem.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);

        Multibinder<HiveSelectivePageSourceFactory> selectivePageSourceFactoryBinder = newSetBinder(binder, HiveSelectivePageSourceFactory.class);
        selectivePageSourceFactoryBinder.addBinding().to(OrcSelectivePageSourceFactory.class).in(Scopes.SINGLETON);
        selectivePageSourceFactoryBinder.addBinding().to(DwrfSelectivePageSourceFactory.class).in(Scopes.SINGLETON);

        binder.bind(DataSinkFactory.class).to(OutputStreamDataSinkFactory.class).in(Scopes.SINGLETON);

        Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder = newSetBinder(binder, HiveFileWriterFactory.class);
        binder.bind(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrcFileWriterFactory.class).as(generatedNameOf(OrcFileWriterFactory.class, connectorId));
        configBinder(binder).bindConfig(OrcFileWriterConfig.class);
        fileWriterFactoryBinder.addBinding().to(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(PageFileWriterFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetFileWriterConfig.class);
        fileWriterFactoryBinder.addBinding().to(ParquetFileWriterFactory.class).in(Scopes.SINGLETON);
        binder.install(new MetastoreClientModule());

        binder.bind(HiveEncryptionInformationProvider.class).in(Scopes.SINGLETON);
        newSetBinder(binder, EncryptionInformationSource.class);

        binder.bind(PartitionMutator.class).to(HivePartitionMutator.class).in(Scopes.SINGLETON);
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
    public ExecutorService createCachingHiveMetastoreExecutor(HiveConnectorId hiveClientId, MetastoreClientConfig metastoreClientConfig)
    {
        return newFixedThreadPool(
                metastoreClientConfig.getMaxMetastoreRefreshThreads(),
                daemonThreadsNamed("hive-metastore-" + hiveClientId + "-%s"));
    }

    @ForUpdatingHiveMetadata
    @Singleton
    @Provides
    public ExecutorService createUpdatingHiveMetadataExecutor(HiveConnectorId hiveClientId)
    {
        return newCachedThreadPool(daemonThreadsNamed("hive-metadata-updater-" + hiveClientId + "-%s"));
    }

    @ForFileRename
    @Singleton
    @Provides
    public ListeningExecutorService createFileRanemeExecutor(HiveConnectorId hiveClientId, HiveClientConfig hiveClientConfig)
    {
        return listeningDecorator(
                new ExecutorServiceAdapter(
                        new BoundedExecutor(
                                newCachedThreadPool(daemonThreadsNamed("hive-rename-" + hiveClientId + "-%s")),
                                hiveClientConfig.getMaxConcurrentFileRenames())));
    }

    @ForZeroRowFileCreator
    @Singleton
    @Provides
    public ListeningExecutorService createZeroRowFileCreatorExecutor(HiveConnectorId hiveClientId, HiveClientConfig hiveClientConfig)
    {
        return listeningDecorator(
                new ExecutorServiceAdapter(
                        new BoundedExecutor(
                                newCachedThreadPool(daemonThreadsNamed("hive-create-zero-row-file-" + hiveClientId + "-%s")),
                                hiveClientConfig.getMaxConcurrentZeroRowFileCreations())));
    }

    @Singleton
    @Provides
    public OrcFileTailSource createOrcFileTailSource(OrcCacheConfig orcCacheConfig, MBeanExporter exporter)
    {
        int expectedFileTailSizeInBytes = toIntExact(orcCacheConfig.getExpectedFileTailSize().toBytes());
        boolean dwrfStripeCacheEnabled = orcCacheConfig.isDwrfStripeCacheEnabled();
        OrcFileTailSource orcFileTailSource = new StorageOrcFileTailSource(expectedFileTailSizeInBytes, dwrfStripeCacheEnabled);
        if (orcCacheConfig.isFileTailCacheEnabled()) {
            Cache<OrcDataSourceId, OrcFileTail> cache = CacheBuilder.newBuilder()
                    .maximumWeight(orcCacheConfig.getFileTailCacheSize().toBytes())
                    .weigher((id, tail) -> ((OrcFileTail) tail).getFooterSize() + ((OrcFileTail) tail).getMetadataSize())
                    .expireAfterAccess(orcCacheConfig.getFileTailCacheTtlSinceLastAccess().toMillis(), MILLISECONDS)
                    .recordStats()
                    .build();
            CacheStatsMBean cacheStatsMBean = new CacheStatsMBean(cache);
            orcFileTailSource = new CachingOrcFileTailSource(orcFileTailSource, cache);
            exporter.export(generatedNameOf(CacheStatsMBean.class, connectorId + "_OrcFileTail"), cacheStatsMBean);
        }
        return orcFileTailSource;
    }

    @Singleton
    @Provides
    public StripeMetadataSourceFactory createStripeMetadataSourceFactory(OrcCacheConfig orcCacheConfig, MBeanExporter exporter)
    {
        StripeMetadataSource stripeMetadataSource = new StorageStripeMetadataSource();
        if (orcCacheConfig.isStripeMetadataCacheEnabled()) {
            Cache<StripeId, Slice> footerCache = CacheBuilder.newBuilder()
                    .maximumWeight(orcCacheConfig.getStripeFooterCacheSize().toBytes())
                    .weigher((id, footer) -> toIntExact(((Slice) footer).getRetainedSize()))
                    .expireAfterAccess(orcCacheConfig.getStripeFooterCacheTtlSinceLastAccess().toMillis(), MILLISECONDS)
                    .recordStats()
                    .build();
            Cache<StripeStreamId, Slice> streamCache = CacheBuilder.newBuilder()
                    .maximumWeight(orcCacheConfig.getStripeStreamCacheSize().toBytes())
                    .weigher((id, stream) -> toIntExact(((Slice) stream).getRetainedSize()))
                    .expireAfterAccess(orcCacheConfig.getStripeStreamCacheTtlSinceLastAccess().toMillis(), MILLISECONDS)
                    .recordStats()
                    .build();
            CacheStatsMBean footerCacheStatsMBean = new CacheStatsMBean(footerCache);
            CacheStatsMBean streamCacheStatsMBean = new CacheStatsMBean(streamCache);
            stripeMetadataSource = new CachingStripeMetadataSource(stripeMetadataSource, footerCache, streamCache);
            exporter.export(generatedNameOf(CacheStatsMBean.class, connectorId + "_StripeFooter"), footerCacheStatsMBean);
            exporter.export(generatedNameOf(CacheStatsMBean.class, connectorId + "_StripeStream"), streamCacheStatsMBean);
        }
        StripeMetadataSourceFactory factory = StripeMetadataSourceFactory.of(stripeMetadataSource);
        if (orcCacheConfig.isDwrfStripeCacheEnabled()) {
            factory = new DwrfAwareStripeMetadataSourceFactory(factory);
        }
        return factory;
    }

    @Singleton
    @Provides
    public ParquetMetadataSource createParquetMetadataSource(ParquetCacheConfig parquetCacheConfig, MBeanExporter exporter)
    {
        ParquetMetadataSource parquetMetadataSource = new MetadataReader();
        if (parquetCacheConfig.isMetadataCacheEnabled()) {
            Cache<ParquetDataSourceId, ParquetFileMetadata> cache = CacheBuilder.newBuilder()
                    .maximumWeight(parquetCacheConfig.getMetadataCacheSize().toBytes())
                    .weigher((id, metadata) -> ((ParquetFileMetadata) metadata).getMetadataSize())
                    .expireAfterAccess(parquetCacheConfig.getMetadataCacheTtlSinceLastAccess().toMillis(), MILLISECONDS)
                    .recordStats()
                    .build();
            CacheStatsMBean cacheStatsMBean = new CacheStatsMBean(cache);
            parquetMetadataSource = new CachingParquetMetadataSource(cache, parquetMetadataSource);
            exporter.export(generatedNameOf(CacheStatsMBean.class, connectorId + "_ParquetMetadata"), cacheStatsMBean);
        }
        return parquetMetadataSource;
    }
}
