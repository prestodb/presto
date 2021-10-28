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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.hive.CacheStatsMBean;
import com.facebook.presto.orc.CachingStripeMetadataSource;
import com.facebook.presto.orc.DwrfAwareStripeMetadataSourceFactory;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSource;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.StripeReader.StripeId;
import com.facebook.presto.orc.StripeReader.StripeStreamId;
import com.facebook.presto.orc.cache.CachingOrcFileTailSource;
import com.facebook.presto.orc.cache.OrcCacheConfig;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.raptor.backup.BackupManager;
import com.facebook.presto.raptor.metadata.AssignmentLimiter;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.DatabaseShardRecorder;
import com.facebook.presto.raptor.metadata.MetadataConfig;
import com.facebook.presto.raptor.metadata.ShardCleaner;
import com.facebook.presto.raptor.metadata.ShardCleanerConfig;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardRecorder;
import com.facebook.presto.raptor.storage.organization.JobFactory;
import com.facebook.presto.raptor.storage.organization.OrganizationJobFactory;
import com.facebook.presto.raptor.storage.organization.ShardCompactionManager;
import com.facebook.presto.raptor.storage.organization.ShardCompactor;
import com.facebook.presto.raptor.storage.organization.ShardOrganizationManager;
import com.facebook.presto.raptor.storage.organization.ShardOrganizer;
import com.facebook.presto.raptor.storage.organization.TemporalFunction;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.slice.Slice;
import org.weakref.jmx.MBeanExporter;

import javax.inject.Singleton;

import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class StorageModule
        implements Module
{
    private final String connectorId;

    public StorageModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(StorageManagerConfig.class);
        configBinder(binder).bindConfig(BucketBalancerConfig.class);
        configBinder(binder).bindConfig(ShardCleanerConfig.class);
        configBinder(binder).bindConfig(MetadataConfig.class);
        configBinder(binder).bindConfig(OrcCacheConfig.class, connectorId);

        binder.bind(Ticker.class).toInstance(Ticker.systemTicker());

        binder.bind(StorageManager.class).to(OrcStorageManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardManager.class).to(DatabaseShardManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardRecorder.class).to(DatabaseShardRecorder.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseShardManager.class).in(Scopes.SINGLETON);
        binder.bind(DatabaseShardRecorder.class).in(Scopes.SINGLETON);
        binder.bind(ShardRecoveryManager.class).in(Scopes.SINGLETON);
        binder.bind(BackupManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardCompactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardOrganizationManager.class).in(Scopes.SINGLETON);
        binder.bind(ShardOrganizer.class).in(Scopes.SINGLETON);
        binder.bind(JobFactory.class).to(OrganizationJobFactory.class).in(Scopes.SINGLETON);
        binder.bind(ShardCompactor.class).in(Scopes.SINGLETON);
        binder.bind(ShardEjector.class).in(Scopes.SINGLETON);
        binder.bind(ShardCleaner.class).in(Scopes.SINGLETON);
        binder.bind(BucketBalancer.class).in(Scopes.SINGLETON);
        binder.bind(ReaderAttributes.class).in(Scopes.SINGLETON);
        binder.bind(AssignmentLimiter.class).in(Scopes.SINGLETON);
        binder.bind(TemporalFunction.class).in(Scopes.SINGLETON);

        newExporter(binder).export(DatabaseShardManager.class).as(generatedNameOf(DatabaseShardManager.class, connectorId));
        newExporter(binder).export(ShardRecoveryManager.class).as(generatedNameOf(ShardRecoveryManager.class, connectorId));
        newExporter(binder).export(BackupManager.class).as(generatedNameOf(BackupManager.class, connectorId));
        newExporter(binder).export(StorageManager.class).as(generatedNameOf(OrcStorageManager.class, connectorId));
        newExporter(binder).export(ShardCompactionManager.class).as(generatedNameOf(ShardCompactionManager.class, connectorId));
        newExporter(binder).export(ShardOrganizer.class).as(generatedNameOf(ShardOrganizer.class, connectorId));
        newExporter(binder).export(ShardCompactor.class).as(generatedNameOf(ShardCompactor.class, connectorId));
        newExporter(binder).export(ShardEjector.class).as(generatedNameOf(ShardEjector.class, connectorId));
        newExporter(binder).export(ShardCleaner.class).as(generatedNameOf(ShardCleaner.class, connectorId));
        newExporter(binder).export(BucketBalancer.class).as(generatedNameOf(BucketBalancer.class, connectorId));
        newExporter(binder).export(JobFactory.class).withGeneratedName();
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
                    .weigher((id, tail) -> ((OrcFileTail) tail).getTotalSize())
                    .expireAfterAccess(orcCacheConfig.getFileTailCacheTtlSinceLastAccess().toMillis(), TimeUnit.MILLISECONDS)
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
                    .weigher((id, footer) -> ((Slice) footer).length())
                    .expireAfterAccess(orcCacheConfig.getStripeFooterCacheTtlSinceLastAccess().toMillis(), TimeUnit.MILLISECONDS)
                    .recordStats()
                    .build();
            Cache<StripeStreamId, Slice> streamCache = CacheBuilder.newBuilder()
                    .maximumWeight(orcCacheConfig.getStripeStreamCacheSize().toBytes())
                    .weigher((id, stream) -> ((Slice) stream).length())
                    .expireAfterAccess(orcCacheConfig.getStripeStreamCacheTtlSinceLastAccess().toMillis(), TimeUnit.MILLISECONDS)
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
