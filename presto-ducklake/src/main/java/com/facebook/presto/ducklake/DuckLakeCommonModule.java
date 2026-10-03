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
package com.facebook.presto.ducklake;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheFactory;
import com.facebook.presto.cache.CacheStats;
import com.facebook.presto.cache.ForCachingFileSystem;
import com.facebook.presto.cache.filemerge.FileMergeCacheConfig;
import com.facebook.presto.ducklake.reader.DuckLakePageSourceProvider;
import com.facebook.presto.ducklake.reader.ParquetPageSourceFactory;
import com.facebook.presto.ducklake.split.DuckLakeSplitManager;
import com.facebook.presto.ducklake.statistics.TableStatisticsMaker;
import com.facebook.presto.hive.CacheStatsMBean;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.ForMetastoreHdfsEnvironment;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration;
import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.cache.CachingParquetMetadataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.cache.ParquetCacheConfig;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.plugin.base.security.AllowAllAccessControl;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.inject.Singleton;
import org.weakref.jmx.MBeanExporter;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DuckLakeCommonModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;

    public DuckLakeCommonModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MetastoreClientConfig.class);
        configBinder(binder).bindConfig(CacheConfig.class);
        configBinder(binder).bindConfig(FileMergeCacheConfig.class);
        binder.bind(CacheStats.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HiveGcsConfig.class);
        binder.bind(GcsConfigurationInitializer.class).to(HiveGcsConfigurationInitializer.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForMetastoreHdfsEnvironment.class).to(HiveCachingHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForCachingFileSystem.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfigurationInitializer.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class);
        binder.bind(CacheFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(DuckLakeConfig.class);
        configBinder(binder).bindConfig(DuckLakeCatalogConfig.class);

        binder.bind(DuckLakeTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(DuckLakeSessionProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(DuckLakeSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(DuckLakePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(TableStatisticsMaker.class).in(Scopes.SINGLETON);
        binder.bind(DuckLakeMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorAccessControl.class).to(AllowAllAccessControl.class).in(Scopes.SINGLETON);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        configBinder(binder).bindConfig(ParquetCacheConfig.class, connectorId);
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
