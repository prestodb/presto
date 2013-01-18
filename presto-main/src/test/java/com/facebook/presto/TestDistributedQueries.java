/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TestQueries.Concat;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.cli.ClientSession;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.server.HttpQueryClient;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.tree.Serialization.FunctionCallDeserializer;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonModule;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.airlift.node.NodeModule;
import io.airlift.testing.FileUtils;
import io.airlift.tracetoken.TraceTokenModule;
import io.airlift.units.Duration;
import org.codehaus.jackson.map.JsonDeserializer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.management.MBeanServer;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.TestQueries.getTuples;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;

public class TestDistributedQueries
{
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final JsonCodec<QueryInfo> queryInfoCodec = createCodecFactory().jsonCodec(QueryInfo.class);
    private final JsonCodec<TaskInfo> taskInfoCodec = createCodecFactory().jsonCodec(TaskInfo.class);

    private PrestoTestingServer coordinator;
    private List<PrestoTestingServer> servers = new ArrayList<>();
    private ApacheHttpClient httpClient;
    private DiscoveryTestingServer discoveryServer;


    @BeforeClass
    public void setUp()
            throws Exception
    {
        try {
            discoveryServer = new DiscoveryTestingServer();
            coordinator = new PrestoTestingServer(discoveryServer.getBaseUrl());
            servers.add(coordinator);
            servers.add(new PrestoTestingServer(discoveryServer.getBaseUrl()));
            servers.add(new PrestoTestingServer(discoveryServer.getBaseUrl()));
        }
        catch (Exception e) {
            tearDown();
            throw e;
        }

        this.httpClient = new ApacheHttpClient(new HttpClientConfig()
                .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                .setReadTimeout(new Duration(10, TimeUnit.DAYS)));

        for (PrestoTestingServer server : servers) {
            server.refreshServiceSelectors();
        }
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        Closeables.closeQuietly(discoveryServer);
        for (PrestoTestingServer server : servers) {
            Closeables.closeQuietly(server);
        }
    }

    @Test
    public void testSelectNodes()
            throws Exception
    {
        List<List<Object>> tuples = executeQuery("select * from sys.nodes");
        Assert.assertEquals(tuples.size(), 3);
        printResults(tuples);
    }

    @Test
    public void testDistributed()
            throws Exception
    {
        // add a test table
        TableMetadata table = coordinator.createTable("default", "default", "test", new ColumnMetadata("value", Type.VARIABLE_BINARY));
        int base = 100;
        for (PrestoTestingServer server : servers) {
            long shardId = coordinator.addShard(table);
            Operator data = createOperator(new Page(BlockAssertions.createStringSequenceBlock(base, base + 10)));
            server.importShard(table, shardId, data);
            coordinator.commitShard(shardId, server.getNodeId());
            base += 100;
        }

        // query the test table
        List<List<Object>> tuples = executeQuery("select * from test");
        printResults(tuples);
        Assert.assertEquals(tuples.size(), 30);
    }

    private void printResults(List<List<Object>> tuples)
    {
        System.out.println("===============================================");
        for (List<Object> tuple : tuples) {
            System.out.println("X " + Joiner.on(" X ").join(tuple) + " X");
        }
        System.out.println("===============================================");
    }

    public List<List<Object>> executeQuery(String sql)
    {
        ClientSession session = new ClientSession(coordinator.getBaseUrl(), null, null, null, true);

        try (HttpQueryClient client = new HttpQueryClient(session, sql, httpClient, executor, queryInfoCodec, taskInfoCodec)) {
            while (true) {
                QueryInfo queryInfo = client.getQueryInfo(false);
                QueryState state = queryInfo.getState();
                if (state == QueryState.RUNNING || state.isDone()) {
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
            Operator operator = client.getResultsOperator();

            List<Tuple> tuples = getTuples(new FilterAndProjectOperator(operator, FilterFunctions.TRUE_FUNCTION, new Concat(operator.getTupleInfos())));
            ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();
            for (Tuple tuple : tuples) {
                builder.add(tuple.toValues());
            }
            return builder.build();
        }
    }

    public static class PrestoTestingServer
            implements Closeable
    {
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
            // TODO: extract all this into a TestingServer class and unify with TestServer
            baseDataDir = Files.createTempDir();

            Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                    .put("node.environment", "testing")
                    .put("storage-manager.data-directory", baseDataDir.getPath())
                    .put("presto-metastore.db.type", "h2")
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

        public TableMetadata createTable(String catalog, String schema, String tableName, ColumnMetadata... columns)
        {
            TableMetadata table = new TableMetadata(catalog, schema, tableName, ImmutableList.copyOf(columns));
            metadata.createTable(table);
            table = metadata.getTable(catalog, schema, tableName);

            // setup the table for imports
            long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
            shardManager.createImportTable(tableId, "unknown", "unknown", "unknown");
            return table;
        }

        private static final AtomicLong nextPartitionId = new AtomicLong();

        public long addShard(TableMetadata table)
        {
            long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
            List<Long> shardIds = shardManager.createImportPartition(tableId, "partition_" + nextPartitionId.incrementAndGet(), ImmutableList.of(new SerializedPartitionChunk(new byte[0])));
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
            if (server != null) {
                try {
                    server.stop();
                }
                catch (Exception ignored) {
                }
            }
            FileUtils.deleteRecursively(baseDataDir);
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
                server.stop();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
            finally {
                FileUtils.deleteRecursively(tempDir);
            }
        }
    }

    private static JsonCodecFactory createCodecFactory()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        ImmutableMap.Builder<Class<?>, JsonDeserializer<?>> deserializers = ImmutableMap.builder();
        deserializers.put(Expression.class, new ExpressionDeserializer());
        deserializers.put(FunctionCall.class, new FunctionCallDeserializer());
        objectMapperProvider.setJsonDeserializers(deserializers.build());
        return new JsonCodecFactory(objectMapperProvider);
    }
}
