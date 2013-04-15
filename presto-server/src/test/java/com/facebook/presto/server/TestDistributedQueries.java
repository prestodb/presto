/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.AbstractTestQueries;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.guice.TestingJmxModule;
import com.facebook.presto.metadata.ConnectorMetadata;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.tree.Serialization.ExpressionSerializer;
import com.facebook.presto.sql.tree.Serialization.FunctionCallDeserializer;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.MaterializedTuple;
import com.facebook.presto.util.TestingTpchBlocksProvider;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.DiscoveryServerModule;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.CachingServiceSelector;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.netty.StandaloneNettyAsyncHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonBinder;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.node.NodeModule;
import io.airlift.testing.FileUtils;
import io.airlift.tracetoken.TraceTokenModule;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.util.TestingTpchBlocksProvider.readTpchRecords;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDistributedQueries
        extends AbstractTestQueries
{
    private static final Logger log = Logger.get(TestDistributedQueries.class.getSimpleName());
    private final JsonCodec<QueryResults> queryResultsCodec = createCodecFactory().jsonCodec(QueryResults.class);

    private PrestoTestingServer coordinator;
    private List<PrestoTestingServer> servers;
    private AsyncHttpClient httpClient;
    private DiscoveryTestingServer discoveryServer;
    private List<String> loadedTableNames;

    @Test
    public void testNodeRoster()
            throws Exception
    {
        List<MaterializedTuple> result = computeActual("SELECT * FROM sys.nodes").getMaterializedTuples();
        assertEquals(result.size(), servers.size());
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
        ImmutableSet<String> tableNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), new Function<MaterializedTuple, String>()
        {
            @Override
            public String apply(MaterializedTuple input)
            {
                assertEquals(input.getFieldCount(), 1);
                return (String) input.getField(0);
            }
        }));
        assertEquals(tableNames.size(), 2);
        assertEquals(tableNames, ImmutableSet.copyOf(loadedTableNames));
    }

    @Test
    public void testShowColumns()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW COLUMNS FROM orders");
        ImmutableSet<String> columnNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), new Function<MaterializedTuple, String>()
        {
            @Override
            public String apply(MaterializedTuple input)
            {
                assertEquals(input.getFieldCount(), 3);
                return (String) input.getField(0);
            }
        }));
        assertEquals(columnNames, ImmutableSet.of("orderkey", "custkey", "orderstatus", "totalprice", "orderdate", "orderpriority", "clerk", "shippriority", "comment"));
    }

    @Test
    public void testShowFunctions()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW FUNCTIONS");
        ImmutableSet<String> functionNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), new Function<MaterializedTuple, String>()
        {
            @Override
            public String apply(MaterializedTuple input)
            {
                assertEquals(input.getFieldCount(), 3);
                return (String) input.getField(0);
            }
        }));
        assertTrue(functionNames.contains("avg"), "Expected function names " + functionNames + " to contain 'avg'");
        assertTrue(functionNames.contains("abs"), "Expected function names " + functionNames + " to contain 'abs'");
    }

    @Test
    public void testNoFrom()
            throws Exception
    {
        assertQuery("SELECT 1 + 2, 3 + 4", "SELECT 1 + 2, 3 + 4 FROM orders LIMIT 1");
    }

    @Override
    protected void setUpQueryFramework(String catalog, String schema)
            throws Exception
    {
        Logging.initialize();

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

        this.httpClient = new StandaloneNettyAsyncHttpClient("test",
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                        .setReadTimeout(new Duration(10, TimeUnit.DAYS)));

        for (PrestoTestingServer server : servers) {
            server.refreshServiceSelectors();
        }

        log.info("Loading data...");
        long startTime = System.nanoTime();
        loadedTableNames = distributeData(catalog, schema);
        log.info("Loading complete in %.2fs", Duration.nanosSince(startTime).convertTo(TimeUnit.SECONDS));
    }

    @Override
    protected void tearDownQueryFramework()
            throws Exception
    {
        if (servers != null) {
            for (PrestoTestingServer server : servers) {
                Closeables.closeQuietly(server);
            }
        }
        Closeables.closeQuietly(discoveryServer);
    }

    private List<String> distributeData(String catalog, String schema)
            throws Exception
    {
        ImmutableList.Builder<String> tableNames = ImmutableList.builder();
        List<QualifiedTableName> qualifiedTableNames = coordinator.metadata.listTables(new QualifiedTablePrefix(catalog, schema));
        for (QualifiedTableName qualifiedTableName : qualifiedTableNames) {
            log.info("Running import for %s", qualifiedTableName.getTableName());
            MaterializedResult importResult = computeActual(format("CREATE MATERIALIZED VIEW default.default.%s AS SELECT * FROM %s",
                    qualifiedTableName.getTableName(),
                    qualifiedTableName));
            log.info("Imported %s rows for %s", importResult.getMaterializedTuples().get(0).getField(0), qualifiedTableName.getTableName());
            tableNames.add(qualifiedTableName.getTableName());
        }

        return tableNames.build();
    }

    @Override
    protected MaterializedResult computeActual(@Language("SQL") String sql)
    {
        ClientSession session = new ClientSession(coordinator.getBaseUrl(), "testuser", "default", "default", true);

        try (StatementClient client = new StatementClient(httpClient, queryResultsCodec, session, sql)) {
            AtomicBoolean loggedUri = new AtomicBoolean(false);
            ImmutableList.Builder<Tuple> rows = ImmutableList.builder();
            TupleInfo tupleInfo = null;

            while (client.isValid()) {
                QueryResults results = client.current();
                if (!loggedUri.getAndSet(true)) {
                    log.info("Query %s: %s?pretty", results.getId(), results.getInfoUri());
                }

                if ((tupleInfo == null) && (results.getColumns() != null)) {
                    tupleInfo = getTupleInfo(results.getColumns());
                }
                if (results.getData() != null) {
                    rows.addAll(transform(results.getData(), dataToTuple(tupleInfo)));
                }

                client.advance();
            }

            if (!client.isFailed()) {
                return new MaterializedResult(rows.build(), tupleInfo);
            }

            QueryError error = client.finalResults().getError();
            assert error != null;
            if (error.getFailureInfo() != null) {
                throw error.getFailureInfo().toException();
            }
            throw new RuntimeException("Query failed: " + error.getMessage());

            // dump query info to console for debugging (NOTE: not pretty printed)
            // JsonCodec<QueryInfo> queryInfoJsonCodec = createCodecFactory().prettyPrint().jsonCodec(QueryInfo.class);
            // log.info("\n" + queryInfoJsonCodec.toJson(queryInfo));
        }
    }

    private static TupleInfo getTupleInfo(List<Column> columns)
    {
        return new TupleInfo(transform(transform(columns, Column.typeGetter()), tupleType()));
    }

    private static Function<String, Type> tupleType()
    {
        return new Function<String, Type>()
        {
            @Override
            public Type apply(String type)
            {
                switch (type) {
                    case "bigint":
                        return Type.FIXED_INT_64;
                    case "double":
                        return Type.DOUBLE;
                    case "varchar":
                        return Type.VARIABLE_BINARY;
                }
                throw new AssertionError("Unhandled type: " + type);
            }
        };
    }

    private static Function<List<Object>, Tuple> dataToTuple(final TupleInfo tupleInfo)
    {
        return new Function<List<Object>, Tuple>()
        {
            @Override
            public Tuple apply(List<Object> data)
            {
                checkArgument(data.size() == tupleInfo.getTypes().size(), "columns size does not match tuple info");
                TupleInfo.Builder tuple = tupleInfo.builder();
                for (int i = 0; i < data.size(); i++) {
                    Object value = data.get(i);
                    if (value == null) {
                        tuple.appendNull();
                        continue;
                    }
                    Type type = tupleInfo.getTypes().get(i);
                    switch (type) {
                        case FIXED_INT_64:
                            tuple.append(((Number) value).longValue());
                            break;
                        case DOUBLE:
                            tuple.append(((Number) value).doubleValue());
                            break;
                        case VARIABLE_BINARY:
                            tuple.append((String) value);
                            break;
                        default:
                            throw new AssertionError("unhandled type: " + type);
                    }
                }
                return tuple.build();
            }
        };
    }

    public static class PrestoTestingServer
            implements Closeable
    {
        private final File baseDataDir;
        private final LifeCycleManager lifeCycleManager;
        private final TestingHttpServer server;
        private final ImmutableList<ServiceSelector> serviceSelectors;
        private final NodeManager nodeManager;
        private final Metadata metadata;

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
                    .put("exchange.http-client.read-timeout", "1h")
                    .put("presto-metastore.db.filename", new File(baseDataDir, "db/MetaStore").getPath())
                    .put("discovery.uri", discoveryUri.toASCIIString())
                    .put("failure-detector.warmup-interval", "0ms")
                    .put("failure-detector.enabled", "false") // todo enable failure detector
                    .put("datasources", "native,tpch")
                    .build();

            Bootstrap app = new Bootstrap(
                    new NodeModule(),
                    new DiscoveryModule(),
                    new TestingHttpServerModule(),
                    new JsonModule(),
                    new JaxrsModule(),
                    new TestingJmxModule(),
                    new InMemoryEventModule(),
                    new TraceTokenModule(),
                    new FailureDetectorModule(),
                    new ServerMainModule(),
                    new Module() {
                        @Override
                        public void configure(Binder binder)
                        {
                            TestingTpchBlocksProvider tpchBlocksProvider = new TestingTpchBlocksProvider(ImmutableMap.of(
                                    "orders", readTpchRecords("orders"),
                                    "lineitem", readTpchRecords("lineitem")));
                            binder.bind(TpchDataStreamProvider.class).toInstance(new TpchDataStreamProvider(tpchBlocksProvider));
                            newMapBinder(binder, String.class, ConnectorMetadata.class).addBinding("tpch").to(TpchMetadata.class);
                        }
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(serverProperties)
                    .initialize();

            injector.getInstance(Announcer.class).start();

            lifeCycleManager = injector.getInstance(LifeCycleManager.class);

            nodeManager = injector.getInstance(NodeManager.class);

            metadata = injector.getInstance(Metadata.class);

            server = injector.getInstance(TestingHttpServer.class);

            ImmutableList.Builder<ServiceSelector> serviceSelectors = ImmutableList.builder();
            for (Binding<ServiceSelector> binding : injector.findBindingsByType(TypeLiteral.get(ServiceSelector.class))) {
                serviceSelectors.add(binding.getProvider().get());
            }
            this.serviceSelectors = serviceSelectors.build();
        }

        public URI getBaseUrl()
        {
            return server.getBaseUrl();
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

            // HACK ALERT!!!! This is to work around the fact that calling get() on the future returned by CachingServiceSelector.refresh() does not actually
            // guarantee that the selectors have been refreshed. TODO: remove the loop when this is fixed: https://github.com/airlift/airlift/issues/78
            long start = System.nanoTime();
            while (!nodeManager.getCurrentNode().isPresent() && Duration.nanosSince(start).convertTo(TimeUnit.SECONDS) < 1) {
                nodeManager.refreshNodes(true);
            }

            assertTrue(nodeManager.getCurrentNode().isPresent(), "Current node is not in active set");
        }

        @Override
        public void close()
        {
            try {
                if (lifeCycleManager != null) {
                    try {
                        lifeCycleManager.stop();
                    }
                    catch (Exception e) {
                        throw Throwables.propagate(e);
                    }
                }
            }
            finally {
                FileUtils.deleteRecursively(baseDataDir);
            }
        }
    }

    public static class DiscoveryTestingServer
            implements Closeable
    {
        private final LifeCycleManager lifeCycleManager;
        private final TestingHttpServer server;
        private final File tempDir;

        public DiscoveryTestingServer()
                throws Exception
        {
            tempDir = Files.createTempDir();

            Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                    .put("node.environment", "testing")
                    .put("static.db.location", tempDir.getAbsolutePath())
                    .build();

            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new NodeModule(),
                    new TestingHttpServerModule(),
                    new JsonModule(),
                    new JaxrsModule(),
                    new DiscoveryServerModule(),
                    new DiscoveryModule(),
                    new TestingJmxModule());

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(serverProperties)
                    .initialize();

            lifeCycleManager = injector.getInstance(LifeCycleManager.class);

            server = injector.getInstance(TestingHttpServer.class);
        }

        public URI getBaseUrl()
        {
            return server.getBaseUrl();
        }

        @Override
        public void close()
        {
            try {
                if (lifeCycleManager != null) {
                    try {
                        lifeCycleManager.stop();
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

    public static JsonCodecFactory createCodecFactory()
    {
        Injector injector = Guice.createInjector(Stage.PRODUCTION,
                new JsonModule(),
                new HandleJsonModule(),
                new Module()
                {
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
