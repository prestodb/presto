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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheFactory;
import com.facebook.presto.cache.CacheStats;
import com.facebook.presto.cache.ForCachingFileSystem;
import com.facebook.presto.cache.filemerge.FileMergeCacheConfig;
import com.facebook.presto.hive.CacheStatsMBean;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.ForMetastoreHdfsEnvironment;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.OrcFileWriterFactory;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.hive.SortingFileWriterConfig;
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration;
import com.facebook.presto.hive.datasink.DataSinkFactory;
import com.facebook.presto.hive.datasink.OutputStreamDataSinkFactory;
import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.metastore.InvalidateMetastoreCacheProcedure;
import com.facebook.presto.iceberg.nessie.IcebergNessieConfig;
import com.facebook.presto.iceberg.optimizer.IcebergPlanOptimizerProvider;
import com.facebook.presto.iceberg.procedure.ExpireSnapshotsProcedure;
import com.facebook.presto.iceberg.procedure.FastForwardBranchProcedure;
import com.facebook.presto.iceberg.procedure.ManifestFileCacheInvalidationProcedure;
import com.facebook.presto.iceberg.procedure.RegisterTableProcedure;
import com.facebook.presto.iceberg.procedure.RemoveOrphanFiles;
import com.facebook.presto.iceberg.procedure.RollbackToSnapshotProcedure;
import com.facebook.presto.iceberg.procedure.RollbackToTimestampProcedure;
import com.facebook.presto.iceberg.procedure.SetCurrentSnapshotProcedure;
import com.facebook.presto.iceberg.procedure.SetTablePropertyProcedure;
import com.facebook.presto.iceberg.procedure.StatisticsFileCacheInvalidationProcedure;
import com.facebook.presto.iceberg.procedure.UnregisterTableProcedure;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.iceberg.statistics.StatisticsFileCacheKey;
import com.facebook.presto.orc.CachingStripeMetadataSource;
import com.facebook.presto.orc.DwrfAwareStripeMetadataSourceFactory;
import com.facebook.presto.orc.EncryptionLibrary;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.StripeReader;
import com.facebook.presto.orc.UnsupportedEncryptionLibrary;
import com.facebook.presto.orc.cache.CachingOrcFileTailSource;
import com.facebook.presto.orc.cache.OrcCacheConfig;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.cache.CachingParquetMetadataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.cache.ParquetCacheConfig;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.weakref.jmx.MBeanExporter;

import javax.inject.Singleton;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.orc.StripeMetadataSource.CacheableRowGroupIndices;
import static com.facebook.presto.orc.StripeMetadataSource.CacheableSlice;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergCommonModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;

    public IcebergCommonModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(CacheConfig.class);
        configBinder(binder).bindConfig(FileMergeCacheConfig.class);
        binder.bind(CacheStats.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HiveGcsConfig.class);
        binder.bind(GcsConfigurationInitializer.class).to(HiveGcsConfigurationInitializer.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForMetastoreHdfsEnvironment.class).to(HiveCachingHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForCachingFileSystem.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(SortingFileWriterConfig.class);
        binder.bind(HdfsConfigurationInitializer.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class);

        binder.bind(CacheFactory.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(IcebergCatalogName.class).toInstance(new IcebergCatalogName(connectorId));

        configBinder(binder).bindConfig(IcebergConfig.class);

        IcebergConfig icebergConfig = buildConfigObject(IcebergConfig.class);
        checkArgument(icebergConfig.getCatalogType().equals(HADOOP) || icebergConfig.getCatalogWarehouseDataDir() == null, "'iceberg.catalog.hadoop.warehouse.datadir' can only be specified in Hadoop catalog");

        binder.bind(IcebergSessionProperties.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, IcebergNessieConfig.class);  // bind optional Nessie config to IcebergSessionProperties

        binder.bind(IcebergTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(IcebergSplitManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ConnectorSplitManager.class).as(generatedNameOf(IcebergSplitManager.class, connectorId));
        binder.bind(ConnectorPageSourceProvider.class).to(IcebergPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(DataSinkFactory.class).to(OutputStreamDataSinkFactory.class).in(Scopes.SINGLETON);
        binder.bind(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        binder.bind(SortParameters.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(IcebergPageSinkProvider.class).in(Scopes.SINGLETON);
        // TODO #20578: Verify if the new partition provider works as expected for all queries and commands.
        binder.bind(ConnectorNodePartitioningProvider.class).to(IcebergNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetFileWriterConfig.class);

        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        binder.bind(IcebergFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergFileWriterFactory.class).withGeneratedName();

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(RollbackToSnapshotProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(RollbackToTimestampProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(RegisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(UnregisterTableProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(ExpireSnapshotsProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(RemoveOrphanFiles.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(FastForwardBranchProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(SetCurrentSnapshotProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(SetTablePropertyProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(StatisticsFileCacheInvalidationProcedure.class).in(Scopes.SINGLETON);
        procedures.addBinding().toProvider(ManifestFileCacheInvalidationProcedure.class).in(Scopes.SINGLETON);

        if (buildConfigObject(MetastoreClientConfig.class).isInvalidateMetastoreCacheProcedureEnabled()) {
            procedures.addBinding().toProvider(InvalidateMetastoreCacheProcedure.class).in(Scopes.SINGLETON);
        }

        // for orc
        binder.bind(EncryptionLibrary.class).annotatedWith(HiveDwrfEncryptionProvider.ForCryptoService.class).to(UnsupportedEncryptionLibrary.class).in(Scopes.SINGLETON);
        binder.bind(EncryptionLibrary.class).annotatedWith(HiveDwrfEncryptionProvider.ForUnknown.class).to(UnsupportedEncryptionLibrary.class).in(Scopes.SINGLETON);
        binder.bind(HiveDwrfEncryptionProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(OrcCacheConfig.class, connectorId);
        configBinder(binder).bindConfig(OrcFileWriterConfig.class);

        configBinder(binder).bindConfig(ParquetCacheConfig.class, connectorId);

        binder.bind(ConnectorPlanOptimizerProvider.class).to(IcebergPlanOptimizerProvider.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    public StatisticsFileCache createStatisticsFileCache(IcebergConfig config, MBeanExporter exporter)
    {
        Cache<StatisticsFileCacheKey, ColumnStatistics> delegate = CacheBuilder.newBuilder()
                .maximumWeight(config.getMaxStatisticsFileCacheSize().toBytes())
                .<StatisticsFileCacheKey, ColumnStatistics>weigher((key, entry) -> (int) entry.getEstimatedSize())
                .recordStats()
                .build();
        StatisticsFileCache statisticsFileCache = new StatisticsFileCache(delegate);
        exporter.export(generatedNameOf(StatisticsFileCache.class, connectorId), statisticsFileCache);
        return statisticsFileCache;
    }

    @Singleton
    @Provides
    public ManifestFileCache createManifestFileCache(IcebergConfig config, MBeanExporter exporter)
    {
        CacheBuilder<ManifestFileCacheKey, ManifestFileCachedContent> delegate = CacheBuilder.newBuilder()
                .maximumWeight(config.getMaxManifestCacheSize())
                .<ManifestFileCacheKey, ManifestFileCachedContent>weigher((key, entry) -> (int) entry.getData().stream().mapToLong(ByteBuffer::capacity).sum())
                .recordStats();
        if (config.getManifestCacheExpireDuration() > 0) {
            delegate.expireAfterWrite(Duration.ofMillis(config.getManifestCacheExpireDuration()));
        }
        ManifestFileCache manifestFileCache = new ManifestFileCache(
                delegate.build(),
                config.getManifestCachingEnabled(),
                config.getManifestCacheMaxContentLength(),
                config.getManifestCacheMaxChunkSize().toBytes());
        exporter.export(generatedNameOf(ManifestFileCache.class, connectorId), manifestFileCache);
        return manifestFileCache;
    }

    @ForCachingHiveMetastore
    @Singleton
    @Provides
    public ExecutorService createCachingHiveMetastoreExecutor(MetastoreClientConfig metastoreClientConfig)
    {
        return newFixedThreadPool(
                metastoreClientConfig.getMaxMetastoreRefreshThreads(),
                daemonThreadsNamed("hive-metastore-iceberg-%s"));
    }

    @Provides
    @Singleton
    @ForIcebergSplitManager
    public ExecutorService createSplitManagerExecutor(IcebergConfig config)
    {
        if (config.getSplitManagerThreads() == 0) {
            return newDirectExecutorService();
        }
        return newFixedThreadPool(
                config.getSplitManagerThreads(),
                daemonThreadsNamed("iceberg-split-manager-" + connectorId + "-%s"));
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
            Cache<StripeReader.StripeId, CacheableSlice> footerCache = CacheBuilder.newBuilder()
                    .maximumWeight(orcCacheConfig.getStripeFooterCacheSize().toBytes())
                    .weigher((id, footer) -> toIntExact(((CacheableSlice) footer).getSlice().getRetainedSize()))
                    .expireAfterAccess(orcCacheConfig.getStripeFooterCacheTtlSinceLastAccess().toMillis(), MILLISECONDS)
                    .recordStats()
                    .build();
            Cache<StripeReader.StripeStreamId, CacheableSlice> streamCache = CacheBuilder.newBuilder()
                    .maximumWeight(orcCacheConfig.getStripeStreamCacheSize().toBytes())
                    .weigher((id, stream) -> toIntExact(((CacheableSlice) stream).getSlice().getRetainedSize()))
                    .expireAfterAccess(orcCacheConfig.getStripeStreamCacheTtlSinceLastAccess().toMillis(), MILLISECONDS)
                    .recordStats()
                    .build();
            CacheStatsMBean footerCacheStatsMBean = new CacheStatsMBean(footerCache);
            CacheStatsMBean streamCacheStatsMBean = new CacheStatsMBean(streamCache);
            exporter.export(generatedNameOf(CacheStatsMBean.class, connectorId + "_StripeFooter"), footerCacheStatsMBean);
            exporter.export(generatedNameOf(CacheStatsMBean.class, connectorId + "_StripeStream"), streamCacheStatsMBean);

            Optional<Cache<StripeReader.StripeStreamId, CacheableRowGroupIndices>> rowGroupIndexCache = Optional.empty();
            if (orcCacheConfig.isRowGroupIndexCacheEnabled()) {
                rowGroupIndexCache = Optional.of(CacheBuilder.newBuilder()
                        .maximumWeight(orcCacheConfig.getRowGroupIndexCacheSize().toBytes())
                        .weigher((id, rowGroupIndices) -> toIntExact(((CacheableRowGroupIndices) rowGroupIndices).getRowGroupIndices().stream().mapToLong(RowGroupIndex::getRetainedSizeInBytes).sum()))
                        .expireAfterAccess(orcCacheConfig.getStripeStreamCacheTtlSinceLastAccess().toMillis(), MILLISECONDS)
                        .recordStats()
                        .build());
                CacheStatsMBean rowGroupIndexCacheStatsMBean = new CacheStatsMBean(rowGroupIndexCache.get());
                exporter.export(generatedNameOf(CacheStatsMBean.class, connectorId + "_StripeStreamRowGroupIndex"), rowGroupIndexCacheStatsMBean);
            }
            stripeMetadataSource = new CachingStripeMetadataSource(stripeMetadataSource, footerCache, streamCache, rowGroupIndexCache);
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
