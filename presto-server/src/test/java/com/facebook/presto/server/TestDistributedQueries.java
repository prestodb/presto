/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.AbstractTestQueries;
import com.facebook.presto.cli.ClientSession;
import com.facebook.presto.cli.HttpQueryClient;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.tree.Serialization.ExpressionSerializer;
import com.facebook.presto.sql.tree.Serialization.FunctionCallDeserializer;
import com.facebook.presto.tpch.TpchSplit;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.MaterializedTuple;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.discovery.DiscoveryServerModule;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.CachingServiceSelector;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.netty.NettyAsyncHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonBinder;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeInfo;
import io.airlift.node.NodeModule;
import io.airlift.testing.FileUtils;
import io.airlift.tracetoken.TraceTokenModule;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.management.MBeanServer;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.util.MaterializedResult.materialize;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;

public class TestDistributedQueries
        extends AbstractTestQueries
{
    private final JsonCodec<QueryInfo> queryInfoCodec = createCodecFactory().jsonCodec(QueryInfo.class);

    private String catalog;
    private String schema;
    private DataStreamProvider dataStreamProvider;
    private Metadata metadata;
    private PrestoTestingServer coordinator;
    private List<PrestoTestingServer> servers;
    private AsyncHttpClient httpClient;
    private DiscoveryTestingServer discoveryServer;
    private List<String> loadedTableNames;

    @Test
    public void testNodeRoster()
            throws Exception
    {
        assertEquals(computeActual("SELECT * FROM sys.nodes").getMaterializedTuples().size(), servers.size());
    }

    @Test
    public void testDual()
            throws Exception
    {
        MaterializedResult result = computeActual("SELECT * FROM dual");
        List<MaterializedTuple> tuples = result.getMaterializedTuples();
        assertEquals(tuples.size(), 1);
    }

    @Test
    public void testShowTables()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW TABLES");
        assertEquals(result.getMaterializedTuples().size(), 2);
        ImmutableSet<String> tableNames = ImmutableSet.copyOf(Iterables.transform(result.getMaterializedTuples(), new Function<MaterializedTuple, String>()
        {
            public String apply(MaterializedTuple input)
            {
                assertEquals(input.getFieldCount(), 1);
                return (String) input.getField(0);
            }
        }));
        assertEquals(tableNames, ImmutableSet.copyOf(loadedTableNames));
    }

    @Override
    protected void setUpQueryFramework(String catalog, String schema, DataStreamProvider dataStreamProvider, Metadata metadata)
            throws Exception
    {
        this.catalog = catalog;
        this.schema = schema;
        this.dataStreamProvider = dataStreamProvider;
        this.metadata = metadata;

        try {
            discoveryServer = new DiscoveryTestingServer();
            coordinator = new PrestoTestingServer(discoveryServer.getBaseUrl());
            servers = ImmutableList.<PrestoTestingServer>builder()
                    .add(coordinator)
                    .add(new PrestoTestingServer(discoveryServer.getBaseUrl()))
                    .add(new PrestoTestingServer(discoveryServer.getBaseUrl()))
                    .build();
        }
        catch (Exception e) {
            tearDownQueryFramework();
            throw e;
        }

        this.httpClient = new NettyAsyncHttpClient(new HttpClientConfig()
                .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                .setReadTimeout(new Duration(10, TimeUnit.DAYS)));

        for (PrestoTestingServer server : servers) {
            server.refreshServiceSelectors();
        }

        loadedTableNames = distributeData();
    }


    @Override
    protected void tearDownQueryFramework()
            throws Exception
    {
        Closeables.closeQuietly(discoveryServer);
        if (servers != null) {
            for (PrestoTestingServer server : servers) {
                Closeables.closeQuietly(server);
            }
        }
    }

    private List<String> distributeData()
            throws IOException
    {
        ImmutableList.Builder<String> tableNames = ImmutableList.builder();
        List<QualifiedTableName> qualifiedTableNames = metadata.listTables(catalog);
        for (QualifiedTableName qualifiedTableName : qualifiedTableNames) {
            tableNames.add(qualifiedTableName.getTableName());

            TableMetadata sourceTable = metadata.getTable(qualifiedTableName.getCatalogName(), qualifiedTableName.getSchemaName(), qualifiedTableName.getTableName());

            TableMetadata targetTable = coordinator.createTable("default", "default", sourceTable.getTableName(), sourceTable.getColumns());

            ImmutableList.Builder<ProjectionFunction> builder = ImmutableList.builder();
            for (int i = 0; i < sourceTable.getColumns().size(); i++) {
                ColumnMetadata column = sourceTable.getColumns().get(i);
                builder.add(ProjectionFunctions.singleColumn(column.getType(), i, 0));
            }
            List<ProjectionFunction> projectionFunctions = builder.build();

            for (int i = 0; i < servers.size(); i++) {
                long shardId = coordinator.addShard(targetTable);
                final int serverIndex = i;
                TpchSplit split = new TpchSplit((TpchTableHandle) sourceTable.getTableHandle().get()); // Currently the whole table
                Operator rawDataStream = dataStreamProvider.createDataStream(split, Lists.transform(sourceTable.getColumns(), handleGetter()));
                Operator filteredStream = new FilterAndProjectOperator(rawDataStream, new FilterFunction()
                {
                    @Override
                    public boolean filter(TupleReadable... cursors)
                    {
                        TupleReadable cursor = cursors[0];
                        return Math.abs(cursor.getTuple().hashCode()) % servers.size() == serverIndex;
                    }
                }, projectionFunctions);
                PrestoTestingServer server = servers.get(i);
                server.importShard(targetTable, shardId, filteredStream);
                coordinator.commitShard(shardId, server.getNodeId());
            }
        }
        return tableNames.build();
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        ClientSession session = new ClientSession(coordinator.getBaseUrl(), "testuser", "default", "default", true);

        try (HttpQueryClient client = new HttpQueryClient(session, sql, httpClient, queryInfoCodec)) {
            while (true) {
                QueryInfo queryInfo = client.getQueryInfo(false);
                QueryState state = queryInfo.getState();
                if (state == QueryState.FAILED) {
                    throw Iterables.getFirst(queryInfo.getFailures(), null).toException();
                }
                else if (state == QueryState.CANCELED) {
                    throw new RuntimeException("Query was cancelled");
                }
                else if (state == QueryState.RUNNING || state.isDone()) {
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }

            client.getQueryInfo(true);

            MaterializedResult materializedResult = materialize(client.getResultsOperator());
            QueryInfo queryInfo = client.getQueryInfo(true);
            if (queryInfo.getState() != QueryState.FINISHED) {
                throw new RuntimeException("Expected query to be FINISHED, but is " + queryInfo.getState());
            }

            // dump query info to console for debugging (NOTE: not pretty printed)
            // JsonCodec<QueryInfo> queryInfoJsonCodec = createCodecFactory().prettyPrint().jsonCodec(QueryInfo.class);
            // System.out.println(queryInfoJsonCodec.toJson(queryInfo));

            return materializedResult;
        }
    }

    // TODO: replace this with util version
    private static Function<ColumnMetadata, ColumnHandle> handleGetter()
    {
        return new Function<ColumnMetadata, ColumnHandle>()
        {
            @Override
            public ColumnHandle apply(ColumnMetadata columnMetadata)
            {
                return columnMetadata.getColumnHandle().get();
            }
        };
    }

    public static class PrestoTestingServer
            implements Closeable
    {
        private static final AtomicLong NEXT_PARTITION_ID = new AtomicLong();

        private final File baseDataDir;
        private final TestingHttpServer server;
        private final ImmutableList<ServiceSelector> serviceSelectors;
        private final Metadata metadata;
        private final ShardManager shardManager;
        private final StorageManager storageManager;
        private final NodeInfo nodeInfo;

        public PrestoTestingServer(URI discoveryUri)
                throws Exception
        {
            checkNotNull(discoveryUri, "discoveryUri is null");

            // TODO: extract all this into a TestingServer class and unify with TestServer
            baseDataDir = Files.createTempDir();

            Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                    .put("node.environment", "testing")
                    .put("storage-manager.data-directory", baseDataDir.getPath())
                    .put("query.client.timeout", "10m")
                    .put("presto-metastore.db.type", "h2")
                    .put("exchange.http-client.read-timeout ", "1h")
                    .put("presto-metastore.db.filename", new File(baseDataDir, "db/MetaStore").getPath())
                    .put("discovery.uri", discoveryUri.toASCIIString())
                    .build();

            // TODO: wrap all this stuff in a TestBootstrap class
            Injector injector = createTestInjector(serverProperties,
                    new NodeModule(),
                    new DiscoveryModule(),
                    new TestingHttpServerModule(),
                    new JsonModule(),
                    new JaxrsModule(),
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bind(MBeanServer.class).toInstance(new TestingMBeanServer());
                        }
                    },
                    new InMemoryEventModule(),
                    new TraceTokenModule(),
                    new ServerMainModule()
            );

            nodeInfo = injector.getInstance(NodeInfo.class);
            metadata = injector.getInstance(Metadata.class);
            shardManager = injector.getInstance(ShardManager.class);
            storageManager = injector.getInstance(StorageManager.class);

            server = injector.getInstance(TestingHttpServer.class);
            try {
                server.start();
            }
            catch (Exception e) {
                try {
                    server.stop();
                }
                catch (Exception ignored) {
                }
                throw e;
            }
            injector.getInstance(Announcer.class).start();

            ImmutableList.Builder<ServiceSelector> serviceSelectors = ImmutableList.builder();
            for (Binding<ServiceSelector> binding : injector.findBindingsByType(TypeLiteral.get(ServiceSelector.class))) {
                serviceSelectors.add(binding.getProvider().get());
            }
            this.serviceSelectors = serviceSelectors.build();
        }

        public String getNodeId()
        {
            return nodeInfo.getNodeId();
        }

        public URI getBaseUrl()
        {
            return server.getBaseUrl();
        }

        public TableMetadata createTable(String catalog, String schema, String tableName, List<ColumnMetadata> columns)
        {
            TableMetadata table = new TableMetadata(catalog, schema, tableName, columns);
            metadata.createTable(table);
            table = metadata.getTable(catalog, schema, tableName);

            // setup the table for imports
            long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
            shardManager.createImportTable(tableId, "unknown", "unknown", "unknown");
            return table;
        }

        public long addShard(TableMetadata table)
        {
            long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
            List<Long> shardIds = shardManager.createImportPartition(tableId, "partition_" + NEXT_PARTITION_ID.incrementAndGet(), ImmutableList.of(new SerializedPartitionChunk(new byte[0])));
            return shardIds.get(0);
        }

        public void importShard(TableMetadata table, long shardId, Operator source)
                throws IOException
        {
            ImmutableList.Builder<Long> columnIds = ImmutableList.builder();
            for (ColumnMetadata column : table.getColumns()) {
                long columnId = ((NativeColumnHandle) column.getColumnHandle().get()).getColumnId();
                columnIds.add(columnId);
            }

            storageManager.importShard(shardId, columnIds.build(), source);
        }

        public void commitShard(long shardId, String nodeId)
        {
            shardManager.commitShard(shardId, nodeId);
        }

        public void refreshServiceSelectors()
        {
            // todo this is super lame
            // todo add a service selector manager to airlift with a refresh method
            for (ServiceSelector selector : serviceSelectors) {
                if (selector instanceof CachingServiceSelector) {
                    try {
                        Method refresh = selector.getClass().getDeclaredMethod("refresh");
                        refresh.setAccessible(true);
                        Future<?> future = (Future<?>) refresh.invoke(selector);
                        future.get();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void close()
        {
            try {
                if (server != null) {
                    try {
                        server.stop();
                    }
                    catch (Exception e) {
                        Throwables.propagate(e);
                    }
                }
            }
            finally {
                FileUtils.deleteRecursively(baseDataDir);
            }
        }

        private static Injector createTestInjector(Map<String, String> properties, Module... modules)
        {
            ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
            for (Module module : modules) {
                if (module instanceof ConfigurationAwareModule) {
                    ((ConfigurationAwareModule) module).setConfigurationFactory(configurationFactory);
                }
            }
            ImmutableList.Builder<Module> moduleList = ImmutableList.builder();
            moduleList.add(new ConfigurationModule(configurationFactory));
            moduleList.add(modules);
            return Guice.createInjector(Stage.DEVELOPMENT, moduleList.build());
        }
    }

    public static class DiscoveryTestingServer
            implements Closeable
    {
        private final TestingHttpServer server;
        private final File tempDir;

        public DiscoveryTestingServer()
                throws Exception
        {
            tempDir = Files.createTempDir();

            // start server
            Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                    .put("node.environment", "testing")
                    .put("static.db.location", tempDir.getAbsolutePath())
                    .build();

            Injector serverInjector = Guice.createInjector(Stage.DEVELOPMENT,
                    new MBeanModule(),
                    new NodeModule(),
                    new TestingHttpServerModule(),
                    new JsonModule(),
                    new JaxrsModule(),
                    new DiscoveryServerModule(),
                    new DiscoveryModule(),
                    new ConfigurationModule(new ConfigurationFactory(serverProperties)),
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bind(MBeanServer.class).toInstance(new TestingMBeanServer());
                        }
                    });

            server = serverInjector.getInstance(TestingHttpServer.class);
            try {
                server.start();
            }
            catch (Exception e) {
                try {
                    server.stop();
                }
                catch (Exception ignored) {
                }
                FileUtils.deleteRecursively(tempDir);
                throw e;
            }
        }

        public URI getBaseUrl()
        {
            return server.getBaseUrl();
        }

        @Override
        public void close()
        {
            try {
                if (server != null) {
                    try {
                        server.stop();
                    }
                    catch (Exception e) {
                        Throwables.propagate(e);
                    }
                }
            }
            finally {
                FileUtils.deleteRecursively(tempDir);
            }
        }
    }

    private static JsonCodecFactory createCodecFactory()
    {
        Injector injector = Guice.createInjector(Stage.PRODUCTION,
                new JsonModule(),
                new HandleJsonModule(),
                new Module() {
                    @Override
                    public void configure(Binder binder)
                    {
                        JsonBinder.jsonBinder(binder).addSerializerBinding(Expression.class).to(ExpressionSerializer.class);
                        JsonBinder.jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionDeserializer.class);
                        JsonBinder.jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(FunctionCallDeserializer.class);
                    }
                });

        return injector.getInstance(JsonCodecFactory.class);
    }
}
