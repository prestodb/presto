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
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveNodePartitioningProvider;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration;
import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.MetastoreConfig;
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
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.slice.Slice;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.inject.Singleton;
import javax.management.MBeanServer;

import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergModule
        implements Module
{
    private final String connectorId;

    public IcebergModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(CacheConfig.class);
        configBinder(binder).bindConfig(FileMergeCacheConfig.class);
        binder.bind(CacheStats.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MetastoreClientConfig.class);
        configBinder(binder).bindConfig(HiveGcsConfig.class);
        binder.bind(GcsConfigurationInitializer.class).to(HiveGcsConfigurationInitializer.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForMetastoreHdfsEnvironment.class).to(HiveCachingHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForCachingFileSystem.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(PartitionMutator.class).to(HivePartitionMutator.class).in(Scopes.SINGLETON);
        binder.bind(ExtendedHiveMetastore.class).to(CachingHiveMetastore.class).in(Scopes.SINGLETON);
        binder.bind(MBeanServer.class).toInstance(new TestingMBeanServer());

        binder.bind(HdfsConfigurationInitializer.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class);

        binder.bind(CacheFactory.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(IcebergCatalogName.class).toInstance(new IcebergCatalogName(connectorId));
        binder.bind(IcebergResourceFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(HiveClientConfig.class);
        configBinder(binder).bindConfig(IcebergConfig.class);
        configBinder(binder).bindConfig(MetastoreConfig.class);

        binder.bind(IcebergSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(IcebergSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(IcebergPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(IcebergPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetFileWriterConfig.class);

        binder.bind(IcebergMetadataFactory.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        binder.bind(IcebergFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergFileWriterFactory.class).withGeneratedName();

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(RollbackToSnapshotProcedure.class).in(Scopes.SINGLETON);

        // for orc
        binder.bind(EncryptionLibrary.class).annotatedWith(HiveDwrfEncryptionProvider.ForCryptoService.class).to(UnsupportedEncryptionLibrary.class).in(Scopes.SINGLETON);
        binder.bind(EncryptionLibrary.class).annotatedWith(HiveDwrfEncryptionProvider.ForUnknown.class).to(UnsupportedEncryptionLibrary.class).in(Scopes.SINGLETON);
        binder.bind(HiveDwrfEncryptionProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(OrcCacheConfig.class, connectorId);
        configBinder(binder).bindConfig(OrcFileWriterConfig.class);
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
            Cache<StripeReader.StripeId, Slice> footerCache = CacheBuilder.newBuilder()
                    .maximumWeight(orcCacheConfig.getStripeFooterCacheSize().toBytes())
                    .weigher((id, footer) -> toIntExact(((Slice) footer).getRetainedSize()))
                    .expireAfterAccess(orcCacheConfig.getStripeFooterCacheTtlSinceLastAccess().toMillis(), MILLISECONDS)
                    .recordStats()
                    .build();
            Cache<StripeReader.StripeStreamId, Slice> streamCache = CacheBuilder.newBuilder()
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
}
