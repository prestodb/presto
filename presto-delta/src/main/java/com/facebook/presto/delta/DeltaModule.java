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
package com.facebook.presto.delta;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheFactory;
import com.facebook.presto.cache.CacheStats;
import com.facebook.presto.cache.ForCachingFileSystem;
import com.facebook.presto.cache.filemerge.FileMergeCacheConfig;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.delta.rule.DeltaPlanOptimizerProvider;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.ForMetastoreHdfsEnvironment;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.azure.AzureConfigurationInitializer;
import com.facebook.presto.hive.azure.HiveAzureConfig;
import com.facebook.presto.hive.azure.HiveAzureConfigurationInitializer;
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration;
import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveMetastoreCacheStats;
import com.facebook.presto.hive.metastore.HivePartitionMutator;
import com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore;
import com.facebook.presto.hive.metastore.InvalidateMetastoreCacheProcedure;
import com.facebook.presto.hive.metastore.MetastoreCacheStats;
import com.facebook.presto.hive.metastore.MetastoreConfig;
import com.facebook.presto.hive.metastore.thrift.ThriftHiveMetastoreConfig;
import com.facebook.presto.spi.procedure.Procedure;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DeltaModule
        extends AbstractConfigurationAwareModule
{
    private final String connectorId;
    private final TypeManager typeManager;

    public DeltaModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);
        configBinder(binder).bindConfig(FileMergeCacheConfig.class);
        configBinder(binder).bindConfig(CacheConfig.class);
        binder.bind(CacheStats.class).in(Scopes.SINGLETON);
        binder.bind(CacheFactory.class).in(Scopes.SINGLETON);

        binder.bind(DeltaConnector.class).in(Scopes.SINGLETON);
        binder.bind(DeltaConnectorId.class).toInstance(new DeltaConnectorId(connectorId));
        binder.bind(DeltaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DeltaClient.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DeltaPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(DeltaSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(DeltaTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(DeltaPlanOptimizerProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DeltaConfig.class);

        configBinder(binder).bindConfig(MetastoreConfig.class);
        configBinder(binder).bindConfig(HiveClientConfig.class);
        configBinder(binder).bindConfig(MetastoreClientConfig.class);
        configBinder(binder).bindConfig(ThriftHiveMetastoreConfig.class);
        binder.bind(MetastoreCacheStats.class).to(HiveMetastoreCacheStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MetastoreCacheStats.class).as(generatedNameOf(MetastoreCacheStats.class, connectorId));
        binder.bind(ExtendedHiveMetastore.class).to(InMemoryCachingHiveMetastore.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForMetastoreHdfsEnvironment.class).to(HiveCachingHdfsConfiguration.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(HiveGcsConfig.class);
        binder.bind(GcsConfigurationInitializer.class).to(HiveGcsConfigurationInitializer.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HiveAzureConfig.class);
        binder.bind(AzureConfigurationInitializer.class).to(HiveAzureConfigurationInitializer.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfigurationInitializer.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        binder.bind(HdfsConfiguration.class).annotatedWith(ForCachingFileSystem.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(PartitionMutator.class).to(HivePartitionMutator.class).in(Scopes.SINGLETON);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(DeltaTable.class));

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).as(generatedNameOf(FileFormatDataSourceStats.class, connectorId));

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        if (buildConfigObject(MetastoreClientConfig.class).isInvalidateMetastoreCacheProcedureEnabled()) {
            procedures.addBinding().toProvider(InvalidateMetastoreCacheProcedure.class).in(Scopes.SINGLETON);
        }
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value));
        }
    }

    @ForCachingHiveMetastore
    @Singleton
    @Provides
    public ExecutorService createCachingHiveMetastoreExecutor(MetastoreClientConfig metastoreClientConfig)
    {
        return newFixedThreadPool(
                metastoreClientConfig.getMaxMetastoreRefreshThreads(),
                daemonThreadsNamed("hive-metastore-delta-%s"));
    }
}
