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

package com.facebook.presto.hudi;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheFactory;
import com.facebook.presto.cache.CacheStats;
import com.facebook.presto.cache.ForCachingFileSystem;
import com.facebook.presto.cache.filemerge.FileMergeCacheConfig;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.ForMetastoreHdfsEnvironment;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCommonSessionProperties;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveNodePartitioningProvider;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.azure.AzureConfigurationInitializer;
import com.facebook.presto.hive.azure.HiveAzureConfig;
import com.facebook.presto.hive.azure.HiveAzureConfigurationInitializer;
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration;
import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.metastore.HiveMetastoreCacheStats;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.InvalidateMetastoreCacheProcedure;
import com.facebook.presto.hive.metastore.MetastoreCacheStats;
import com.facebook.presto.hive.metastore.MetastoreConfig;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreConfig;
import com.facebook.presto.hudi.split.ForHudiBackgroundSplitLoader;
import com.facebook.presto.hudi.split.ForHudiSplitAsyncQueue;
import com.facebook.presto.hudi.split.ForHudiSplitSource;
import com.facebook.presto.plugin.base.security.AllowAllAccessControl;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.inject.Singleton;
import javax.management.MBeanServer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HudiModule
        extends AbstractConfigurationAwareModule
{
    private final ClassLoader classLoader;
    private final String connectorId;

    public HudiModule(ClassLoader classLoader, String connectorId)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(HiveClientConfig.class);
        configBinder(binder).bindConfig(MetastoreConfig.class);
        configBinder(binder).bindConfig(HudiConfig.class);

        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(CacheConfig.class);
        configBinder(binder).bindConfig(FileMergeCacheConfig.class);
        binder.bind(CacheStats.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MetastoreClientConfig.class);
        configBinder(binder).bindConfig(ThriftHiveMetastoreConfig.class);
        configBinder(binder).bindConfig(HiveGcsConfig.class);
        configBinder(binder).bindConfig(HiveAzureConfig.class);
        binder.bind(GcsConfigurationInitializer.class).to(HiveGcsConfigurationInitializer.class).in(Scopes.SINGLETON);
        binder.bind(AzureConfigurationInitializer.class).to(HiveAzureConfigurationInitializer.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForMetastoreHdfsEnvironment.class).to(HiveCachingHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForCachingFileSystem.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(PartitionMutator.class).to(HivePartitionMutator.class).in(Scopes.SINGLETON);
        binder.bind(MetastoreCacheStats.class).to(HiveMetastoreCacheStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MetastoreCacheStats.class).as(generatedNameOf(MetastoreCacheStats.class, connectorId));
        binder.bind(MBeanServer.class).toInstance(new TestingMBeanServer());

        binder.bind(HdfsConfigurationInitializer.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class);

        binder.bind(CacheFactory.class).in(Scopes.SINGLETON);
        binder.bind(HudiTransactionManager.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(HudiSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(HudiPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        binder.bind(HudiMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HudiPartitionManager.class).in(Scopes.SINGLETON);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        binder.bind(HudiSessionProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorAccessControl.class).to(AllowAllAccessControl.class).in(Scopes.SINGLETON);

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        if (buildConfigObject(MetastoreClientConfig.class).isInvalidateMetastoreCacheProcedureEnabled()) {
            procedures.addBinding().toProvider(InvalidateMetastoreCacheProcedure.class).in(Scopes.SINGLETON);
        }
    }

    @ForCachingHiveMetastore
    @Singleton
    @Provides
    public ExecutorService createCachingHiveMetastoreExecutor(MetastoreClientConfig metastoreClientConfig)
    {
        return newFixedThreadPool(
                metastoreClientConfig.getMaxMetastoreRefreshThreads(),
                daemonThreadsNamed("hive-metastore-hudi-%s"));
    }

    @ForHudiSplitAsyncQueue
    @Singleton
    @Provides
    public ExecutorService createHudiSplitManagerExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("hudi-split-manager-%s"));
    }

    @ForHudiSplitSource
    @Singleton
    @Provides
    public ScheduledExecutorService createSplitLoaderExecutor(HudiConfig hudiConfig)
    {
        return newScheduledThreadPool(
                hudiConfig.getSplitLoaderParallelism(),
                daemonThreadsNamed("hudi-split-loader-%s"));
    }

    @ForHudiBackgroundSplitLoader
    @Singleton
    @Provides
    public ExecutorService createSplitGeneratorExecutor(HudiConfig hudiConfig)
    {
        return newFixedThreadPool(
                hudiConfig.getSplitGeneratorParallelism(),
                daemonThreadsNamed("hudi-split-generator-%s"));
    }

    @Singleton
    @Provides
    public Connector createConnector(LifeCycleManager lifeCycleManager,
            HudiTransactionManager hudiTransactionManager,
            HudiMetadataFactory hudiMetadataFactory,
            ConnectorSplitManager connectorSplitManager,
            ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorNodePartitioningProvider connectorNodePartitioningProvider,
            HudiSessionProperties hudiSessionProperties,
            HiveCommonSessionProperties hiveCommonSessionProperties)
    {
        return new HudiConnector(lifeCycleManager,
                hudiTransactionManager,
                hudiMetadataFactory,
                new ClassLoaderSafeConnectorSplitManager(connectorSplitManager, classLoader),
                new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSourceProvider, classLoader),
                new ClassLoaderSafeNodePartitioningProvider(connectorNodePartitioningProvider, classLoader),
                new AllowAllAccessControl(),
                hudiSessionProperties,
                hiveCommonSessionProperties);
    }
}
