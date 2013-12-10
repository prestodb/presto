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
package com.facebook.presto.server;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.dictionary.DictionaryBlockEncoding;
import com.facebook.presto.block.rle.RunLengthBlockEncoding;
import com.facebook.presto.block.snappy.SnappyBlockEncoding;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.connector.NativeConnectorFactory;
import com.facebook.presto.connector.dual.DualModule;
import com.facebook.presto.connector.informationSchema.InformationSchemaModule;
import com.facebook.presto.connector.jmx.JmxConnectorFactory;
import com.facebook.presto.connector.system.SystemTablesModule;
import com.facebook.presto.event.query.QueryCompletionEvent;
import com.facebook.presto.event.query.QueryCreatedEvent;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.event.query.SplitCompletionEvent;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlTaskManager;
import com.facebook.presto.execution.TaskExecutor;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.guice.AbstractConfigurationAwareModule;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.CatalogManagerConfig;
import com.facebook.presto.metadata.ColumnMetadataMapper;
import com.facebook.presto.metadata.DatabaseLocalStorageManager;
import com.facebook.presto.metadata.DatabaseLocalStorageManagerConfig;
import com.facebook.presto.metadata.ForLocalStorageManager;
import com.facebook.presto.metadata.ForMetadata;
import com.facebook.presto.metadata.ForShardManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.TableColumnMapper;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientConfig;
import com.facebook.presto.operator.ExchangeClientFactory;
import com.facebook.presto.operator.ForExchange;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.operator.RecordSinkProvider;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.block.BlockEncoding.BlockEncodingFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.NullType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.NativePartitionKey;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.tree.Serialization.ExpressionSerializer;
import com.facebook.presto.sql.tree.Serialization.FunctionCallDeserializer;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.util.Threads;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import io.airlift.dbpool.H2EmbeddedDataSourceModule;
import io.airlift.dbpool.MySqlDataSourceModule;
import io.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Singleton;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.facebook.presto.guice.ConditionalModule.installIfPropertyEquals;
import static com.facebook.presto.guice.DbiProvider.bindDbiToDataSource;
import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.event.client.EventBinder.eventBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ServerMainModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);

        // TODO: this should only be installed if this is a coordinator
        install(new CoordinatorModule());

        if (serverConfig.isCoordinator()) {
            discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator");
        }

        bindFailureDetector(binder, serverConfig.isCoordinator());

        // task execution
        binder.bind(TaskResource.class).in(Scopes.SINGLETON);
        binder.bind(TaskManager.class).to(SqlTaskManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskManager.class).withGeneratedName();
        binder.bind(TaskExecutor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskExecutor.class).withGeneratedName();
        binder.bind(LocalExecutionPlanner.class).in(Scopes.SINGLETON);
        binder.bind(ExpressionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExpressionCompiler.class).withGeneratedName();
        bindConfig(binder).to(TaskManagerConfig.class);

        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        binder.bind(PagesMapper.class).in(Scopes.SINGLETON);

        // exchange client
        binder.bind(new TypeLiteral<Supplier<ExchangeClient>>() {}).to(ExchangeClientFactory.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindAsyncHttpClient("exchange", ForExchange.class).withTracing();
        bindConfig(binder).to(ExchangeClientConfig.class);

        // execution
        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);
        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindAsyncHttpClient("scheduler", ForScheduler.class).withTracing();

        // local storage manager
        bindConfig(binder).to(DatabaseLocalStorageManagerConfig.class);
        binder.bind(LocalStorageManager.class).to(DatabaseLocalStorageManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(LocalStorageManager.class).withGeneratedName();

        // data stream provider
        binder.bind(DataStreamManager.class).in(Scopes.SINGLETON);
        binder.bind(DataStreamProvider.class).to(DataStreamManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorDataStreamProvider.class);

        // record sink provider
        binder.bind(RecordSinkManager.class).in(Scopes.SINGLETON);
        binder.bind(RecordSinkProvider.class).to(RecordSinkManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorRecordSinkProvider.class);

        // metadata
        binder.bind(CatalogManager.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(CatalogManagerConfig.class);
        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);

        // type
        binder.bind(TypeRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).to(TypeRegistry.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        newSetBinder(binder, Type.class);

        // index manager
        binder.bind(IndexManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorIndexResolver.class);

        // handle resolver
        binder.install(new HandleJsonModule());

        // connector
        binder.bind(ConnectorManager.class).in(Scopes.SINGLETON);
        MapBinder<String, ConnectorFactory> connectorFactoryBinder = newMapBinder(binder, String.class, ConnectorFactory.class);

        // native
        connectorFactoryBinder.addBinding("native").to(NativeConnectorFactory.class);

        // jmx connector
        connectorFactoryBinder.addBinding("jmx").to(JmxConnectorFactory.class);

        // dual
        binder.install(new DualModule());

        // information schema
        binder.install(new InformationSchemaModule());

        // system tables
        binder.install(new SystemTablesModule());

        // splits
        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(Split.class);
        jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);
        jsonBinder(binder).addSerializerBinding(Expression.class).to(ExpressionSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionDeserializer.class);
        jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(FunctionCallDeserializer.class);

        // query monitor
        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);
        eventBinder(binder).bindEventClient(QueryCreatedEvent.class);
        eventBinder(binder).bindEventClient(QueryCompletionEvent.class);
        eventBinder(binder).bindEventClient(SplitCompletionEvent.class);

        // Determine the NodeVersion
        String prestoVersion = serverConfig.getPrestoVersion();
        if (prestoVersion == null) {
            prestoVersion = detectPrestoVersion();
        }
        checkState(prestoVersion != null, "presto.version must be provided when it cannot be automatically determined");

        NodeVersion nodeVersion = new NodeVersion(prestoVersion);
        binder.bind(NodeVersion.class).toInstance(nodeVersion);

        // presto announcement
        ServiceAnnouncementBuilder prestoAnnouncement = discoveryBinder(binder).bindHttpAnnouncement("presto")
                .addProperty("node_version", nodeVersion.toString());

        if (serverConfig.getDataSources() != null) {
            prestoAnnouncement.addProperty("datasources", serverConfig.getDataSources());
        }

        bindDataSource(binder, "presto-metastore", ForMetadata.class, ForShardManager.class);
        Multibinder<ResultSetMapper<?>> resultSetMapperBinder = newSetBinder(binder, new TypeLiteral<ResultSetMapper<?>>() {}, ForMetadata.class);
        resultSetMapperBinder.addBinding().to(TableColumnMapper.class).in(Scopes.SINGLETON);
        resultSetMapperBinder.addBinding().to(ColumnMetadataMapper.class).in(Scopes.SINGLETON);
        resultSetMapperBinder.addBinding().to(NativePartitionKey.Mapper.class).in(Scopes.SINGLETON);

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryResults.class);
        binder.bind(StatementResource.class).in(Scopes.SINGLETON);

        // execute resource
        binder.bind(ExecuteResource.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindAsyncHttpClient("execute", ForExecute.class);

        // plugin manager
        binder.bind(PluginManager.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(PluginManagerConfig.class);

        // optimizers
        binder.bind(new TypeLiteral<List<PlanOptimizer>>() {}).toProvider(PlanOptimizersFactory.class).in(Scopes.SINGLETON);

        // block encodings
        binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
        Multibinder<BlockEncodingFactory<?>> blockEncodingFactoryBinder = newSetBinder(binder, new TypeLiteral<BlockEncodingFactory<?>>() {});
        blockEncodingFactoryBinder.addBinding().toInstance(NullType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(BooleanType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(BigintType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(DoubleType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(VarcharType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(DateType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(TimeType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(TimeWithTimeZoneType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(TimestampType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(TimestampWithTimeZoneType.BLOCK_ENCODING_FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(RunLengthBlockEncoding.FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(DictionaryBlockEncoding.FACTORY);
        blockEncodingFactoryBinder.addBinding().toInstance(SnappyBlockEncoding.FACTORY);

        // thread visualizer
        binder.bind(ThreadResource.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForExchange
    public Executor createExchangeExecutor()
    {
        return Executors.newCachedThreadPool(Threads.daemonThreadsNamed("exchange-callback-%s"));
    }

    @Provides
    @Singleton
    @ForLocalStorageManager
    public IDBI createLocalStorageManagerDBI(DatabaseLocalStorageManagerConfig config)
            throws Exception
    {
        return new DBI(new H2EmbeddedDataSource(new H2EmbeddedDataSourceConfig()
                .setFilename(new File(config.getDataDirectory(), "db/StorageManager").getAbsolutePath())
                .setMaxConnections(500)
                .setMaxConnectionWait(new Duration(1, SECONDS))));
    }

    @SafeVarargs
    private final void bindDataSource(Binder binder, String type, Class<? extends Annotation> annotation, Class<? extends Annotation>... aliases)
    {
        String property = type + ".db.type";
        install(installIfPropertyEquals(new MySqlDataSourceModule(type, annotation, aliases), property, "mysql"));
        install(installIfPropertyEquals(new H2EmbeddedDataSourceModule(type, annotation, aliases), property, "h2"));

        bindDbiToDataSource(binder, annotation);
        for (Class<? extends Annotation> alias : aliases) {
            bindDbiToDataSource(binder, alias);
        }
    }

    private static String detectPrestoVersion()
    {
        String title = PrestoServer.class.getPackage().getImplementationTitle();
        String version = PrestoServer.class.getPackage().getImplementationVersion();
        return ((title == null) || (version == null)) ? null : (title + ":" + version);
    }

    private static void bindFailureDetector(Binder binder, boolean coordinator)
    {
        // TODO: this is a hack until the coordinator module works correctly
        if (coordinator) {
            binder.install(new FailureDetectorModule());
            binder.bind(NodeResource.class).in(Scopes.SINGLETON);
        }
        else {
            binder.bind(FailureDetector.class).toInstance(new FailureDetector()
            {
                @Override
                public Set<ServiceDescriptor> getFailed()
                {
                    return ImmutableSet.of();
                }
            });
        }
    }
}
