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
package com.facebook.presto.flightshim;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.util.ResourceFileUtils.getResourceFile;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public abstract class AbstractTestFlightShimPlugins
        extends AbstractTestQueryFramework
{
    public static final JsonCodec<FlightShimRequest> REQUEST_JSON_CODEC = jsonCodec(FlightShimRequest.class);
    public static final JsonCodec<JdbcColumnHandle> COLUMN_HANDLE_JSON_CODEC = jsonCodec(JdbcColumnHandle.class);
    public static final String TPCH_TABLE = "lineitem";
    public static final String ORDERKEY_COLUMN = "orderkey";
    public static final String LINENUMBER_COLUMN = "linenumber";
    public static final String LINESTATUS_COLUMN = "linestatus";
    public static final String EXTENDEDPRICE_COLUMN = "extendedprice";
    public static final String QUANTITY_COLUMN = "quantity";
    public static final String SHIPDATE_COLUMN = "shipdate";
    protected final List<AutoCloseable> closables = new ArrayList<>();
    protected static final CallOption CALL_OPTIONS = CallOptions.timeout(300, TimeUnit.SECONDS);
    protected BufferAllocator allocator;
    protected FlightServer server;
    private ArrowBlockBuilder blockBuilder;

    protected abstract String getConnectorId();

    protected abstract String getConnectionUrl();

    protected abstract String getPluginBundles();

    @BeforeClass
    public void setup()
            throws Exception
    {
        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.builder();
        configBuilder.put("flight-shim.server", "localhost");
        configBuilder.put("flight-shim.server.port", String.valueOf(findUnusedPort()));
        configBuilder.put("flight-shim.server-ssl-certificate-file", "src/test/resources/certs/server.crt");
        configBuilder.put("flight-shim.server-ssl-key-file", "src/test/resources/certs/server.key");
        configBuilder.put("plugin.bundles", getPluginBundles());

        // Allow for 3 batches using testing tpch db
        configBuilder.put("flight-shim.max-rows-per-batch", String.valueOf(500));

        Injector injector = FlightShimServer.initialize(configBuilder.build());

        server = FlightShimServer.start(injector, FlightServer.builder());
        closables.add(server);

        // Set test properties after catalogs have been loaded
        FlightShimPluginManager pluginManager = injector.getInstance(FlightShimPluginManager.class);
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("connection-url", getConnectionUrl());
        pluginManager.setCatalogProperties(getConnectorId(), getConnectorId(), connectorProperties);

        // Make sure these resources close properly
        allocator = injector.getInstance(BufferAllocator.class);
        closables.add(allocator);
        closables.add(injector.getInstance(FlightShimProducer.class));

        TypeManager typeManager = injector.getInstance(TypeManager.class);
        blockBuilder = new ArrowBlockBuilder(typeManager);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        for (AutoCloseable closeable : Lists.reverse(closables)) {
            closeable.close();
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        return new FlightShimQueryRunner();
    }

    @Test
    public void testConnectorGetStream() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(ImmutableList.of(getOrderKeyColumn()))));

            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                }
            }

            assertGreaterThan(rowCount, 0);
        }
    }

    @Test
    public void testStopStreamAtLimit() throws Exception
    {
        int rowLimit = 500;
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(ImmutableList.of(getOrderKeyColumn()))));

            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                    if (rowCount >= rowLimit) {
                        break;
                    }
                }
            }

            assertEquals(rowCount, rowLimit);
        }
    }

    @Test
    public void testCancelStream() throws Exception
    {
        String cancelMessage = "READ COMPLETE";
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(ImmutableList.of(getOrderKeyColumn()))));

            // Cancel stream explicitly
            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                    if (rowCount >= 500) {
                        stream.cancel("Cancel", new CancellationException(cancelMessage));
                        break;
                    }
                }

                // Drain any remaining messages to properly release messages
                try {
                    do {
                        Thread.sleep(100);
                    }
                    while (stream.next());
                }
                catch (final Exception e) {
                    assertNotNull(e.getCause());
                    assertEquals(e.getCause().getMessage(), cancelMessage);
                }
            }

            assertGreaterThan(rowCount, 0);
        }
    }

    @Test
    void testJdbcSplitWithTupleDomain() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequestWithTupleDomain()));

            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                }
            }

            assertGreaterThan(rowCount, 0);
        }
    }

    @Test
    void testJdbcSplitWithAdditionalPredicate() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequestWithAdditionalPredicate()));

            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                }
            }

            assertGreaterThan(rowCount, 0);
        }
    }

    @Test
    void testWithMtls() throws Exception
    {
        InputStream trustedCertificate = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/certs/server.crt")));
        InputStream clientCertificate = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/certs/client.crt")));
        InputStream clientKey = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/certs/client.key")));

        Location location = Location.forGrpcTls("localhost", server.getPort());

        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = FlightClient.builder(bufferAllocator, location).useTls().clientCertificate(clientCertificate, clientKey).trustedCertificates(trustedCertificate).build()) {
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(ImmutableList.of(getOrderKeyColumn()))));

            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                }
            }

            assertGreaterThan(rowCount, 0);
        }
    }

    @Test
    public void testSelectColumns()
    {
        assertSelectQueryFromColumns(ImmutableList.of(ORDERKEY_COLUMN, LINENUMBER_COLUMN));
    }

    @Test
    public void testDateColumns()
    {
        assertSelectQueryFromColumns(ImmutableList.of(ORDERKEY_COLUMN, SHIPDATE_COLUMN));
    }

    @Test
    public void testFloatingPointColumns()
    {
        assertSelectQueryFromColumns(ImmutableList.of(ORDERKEY_COLUMN, QUANTITY_COLUMN, EXTENDEDPRICE_COLUMN));
    }

    protected void assertSelectQueryFromColumns(List<String> tpchColumnNames)
    {
        @Language("SQL") String query = format("SELECT %s FROM %s", String.join(",", tpchColumnNames), TPCH_TABLE);
        assertQuery(query);
    }

    protected List<JdbcColumnHandle> getHandlesFromSelectQuery(String sql)
    {
        String sqlLower = sql.toLowerCase(Locale.ENGLISH);
        int start = sqlLower.indexOf("select");
        if (start < 0) {
            throw new RuntimeException("Expected 'SELECT' in query: " + sql);
        }
        start += "select".length();
        int stop = sqlLower.indexOf("from");
        if (stop < start) {
            throw new RuntimeException("Expected 'FROM' in query: " + sql);
        }
        String columnsString = sql.substring(start, stop);
        List<String> columns = Arrays.stream(columnsString.split(",")).map(String::trim).collect(Collectors.toList());

        if (columns.isEmpty()) {
            throw new RuntimeException("No columns found in query: " + sql);
        }

        ImmutableList.Builder<JdbcColumnHandle> columnHandlesBuilder = ImmutableList.builder();
        for (String column : columns) {
            switch (column) {
                case ORDERKEY_COLUMN:
                    columnHandlesBuilder.add(getOrderKeyColumn());
                    break;
                case LINENUMBER_COLUMN:
                    columnHandlesBuilder.add(getLineNumberColumn());
                    break;
                case QUANTITY_COLUMN:
                    columnHandlesBuilder.add(getQuantityColumn());
                    break;
                case EXTENDEDPRICE_COLUMN:
                    columnHandlesBuilder.add(getExtendedPriceColumn());
                    break;
                case LINESTATUS_COLUMN:
                    columnHandlesBuilder.add(getLineStatusColumn());
                    break;
                case SHIPDATE_COLUMN:
                    columnHandlesBuilder.add(getShipDateColumn());
                    break;
                default:
                    throw new RuntimeException("Unknown column handle for: " + column);
            }
        }

        return columnHandlesBuilder.build();
    }

    protected class FlightShimQueryRunner
            implements QueryRunner
    {
        @Override
        public Session getDefaultSession()
        {
            return ((QueryRunner) getExpectedQueryRunner()).getDefaultSession();
        }

        @Override
        public MaterializedResult execute(String sql)
        {
            try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                    FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
                List<JdbcColumnHandle> columnHandles = getHandlesFromSelectQuery(sql);
                Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(columnHandles)));

                List<Page> pages = new ArrayList<>();
                try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                    while (stream.next()) {
                        List<Block> blocks = new ArrayList<>();
                        for (JdbcColumnHandle columnHandle : columnHandles) {
                            FieldVector vector = stream.getRoot().getVector(columnHandle.getColumnName());
                            Block block = blockBuilder.buildBlockFromFieldVector(vector, columnHandle.getColumnType(), null);
                            blocks.add(block);
                        }
                        pages.add(new Page(stream.getRoot().getRowCount(), blocks.toArray(new Block[0])));
                    }
                }

                List<Type> types = columnHandles.stream().map(JdbcColumnHandle::getColumnType).collect(Collectors.toList());
                MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(getSession(), types);
                resultBuilder.pages(pages);
                return resultBuilder.build();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int getNodeCount()
        {
            return 0;
        }

        @Override
        public TransactionManager getTransactionManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Metadata getMetadata()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitManager getSplitManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PageSourceManager getPageSourceManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public NodePartitioningManager getNodePartitioningManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPlanOptimizerManager getPlanOptimizerManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanCheckerProviderManager getPlanCheckerProviderManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public StatsCalculator getStatsCalculator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<EventListener> getEventListeners()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TestingAccessControlManager getAccessControl()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpressionOptimizerManager getExpressionManager()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MaterializedResult execute(Session session, String sql)
        {
            return execute(sql);
        }

        @Override
        public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(Session session, String table)
        {
            return false;
        }

        @Override
        public void installPlugin(Plugin plugin)
        {
        }

        @Override
        public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
        {
        }

        @Override
        public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
        {
        }

        @Override
        public Lock getExclusiveLock()
        {
            return null;
        }

        @Override
        public void close()
        {
        }
    }

    private int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private JdbcColumnHandle getOrderKeyColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                ORDERKEY_COLUMN,
                new JdbcTypeHandle(Types.BIGINT, "bigint", 8, 0),
                BigintType.BIGINT,
                false,
                Optional.empty());
    }

    private JdbcColumnHandle getLineNumberColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                LINENUMBER_COLUMN,
                new JdbcTypeHandle(Types.INTEGER, "integer", 4, 0),
                IntegerType.INTEGER,
                false,
                Optional.empty());
    }

    private JdbcColumnHandle getLineStatusColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                LINESTATUS_COLUMN,
                new JdbcTypeHandle(Types.VARCHAR, "varchar", 32, 0),
                VarcharType.createVarcharType(32),
                false,
                Optional.empty());
    }

    private JdbcColumnHandle getQuantityColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                QUANTITY_COLUMN,
                new JdbcTypeHandle(Types.DOUBLE, "double", 8, 0),
                DoubleType.DOUBLE,
                false,
                Optional.empty());
    }

    private JdbcColumnHandle getExtendedPriceColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                EXTENDEDPRICE_COLUMN,
                new JdbcTypeHandle(Types.DOUBLE, "double", 8, 0),
                DoubleType.DOUBLE,
                false,
                Optional.empty());
    }

    private JdbcColumnHandle getShipDateColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                SHIPDATE_COLUMN,
                new JdbcTypeHandle(Types.DATE, "date", 8, 0),
                DateType.DATE,
                false,
                Optional.empty());
    }

    private JdbcColumnHandle getShipTimestampColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                SHIPDATE_COLUMN,
                new JdbcTypeHandle(Types.TIMESTAMP, "timestamp", 8, 0),
                TimestampType.TIMESTAMP,
                false,
                Optional.empty());
    }

    protected FlightShimRequest createTpchTableRequest(List<JdbcColumnHandle> columnHandles)
    {
        String split = createJdbcSplit(getConnectorId(), "tpch", TPCH_TABLE);
        byte[] splitBytes = split.getBytes(StandardCharsets.UTF_8);

        ImmutableList.Builder<byte[]> columnBuilder = ImmutableList.builder();
        for (JdbcColumnHandle columnHandle : columnHandles) {
            columnBuilder.add(COLUMN_HANDLE_JSON_CODEC.toJsonBytes(columnHandle));
        }

        return new FlightShimRequest(getConnectorId(), splitBytes, columnBuilder.build());
    }

    protected FlightShimRequest createTpchTableRequestWithTupleDomain() throws Exception
    {
        JdbcColumnHandle orderKeyHandle = getOrderKeyColumn();
        byte[] splitBytes = Files.readAllBytes(getResourceFile("split_tuple_domain.json").toPath());

        List<JdbcColumnHandle> columnHandles = ImmutableList.of(orderKeyHandle);
        ImmutableList.Builder<byte[]> columnBuilder = ImmutableList.builder();
        for (JdbcColumnHandle columnHandle : columnHandles) {
            columnBuilder.add(COLUMN_HANDLE_JSON_CODEC.toJsonBytes(columnHandle));
        }

        return new FlightShimRequest(
                getConnectorId(),
                splitBytes,
                columnBuilder.build());
    }

    protected FlightShimRequest createTpchTableRequestWithAdditionalPredicate()
            throws IOException
    {
        // Query is: "SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)"
        JdbcColumnHandle orderKeyHandle = getOrderKeyColumn();
        byte[] splitBytes = Files.readAllBytes(getResourceFile("split_additional_predicate.json").toPath());

        List<JdbcColumnHandle> columnHandles = ImmutableList.of(orderKeyHandle);
        ImmutableList.Builder<byte[]> columnBuilder = ImmutableList.builder();
        for (JdbcColumnHandle columnHandle : columnHandles) {
            columnBuilder.add(COLUMN_HANDLE_JSON_CODEC.toJsonBytes(columnHandle));
        }

        return new FlightShimRequest(
                getConnectorId(),
                splitBytes,
                columnBuilder.build());
    }

    protected static String removeDatabaseFromJdbcUrl(String jdbcUrl)
    {
        return jdbcUrl.replaceFirst("/[^/?]+([?]|$)", "/$1");
    }

    protected static String addDatabaseCredentialsToJdbcUrl(String jdbcUrl, String username, String password)
    {
        return jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") +
                "user=" + username + "&password=" + password;
    }

    protected static String createJdbcSplit(String connectorId, String schemaName, String tableName)
    {
        return format("{\n" +
                "  \"connectorId\" : \"%s\",\n" +
                "  \"schemaName\" : \"%s\",\n" +
                "  \"tableName\" : \"%s\",\n" +
                "  \"tupleDomain\" : {\n" +
                "    \"columnDomains\" : [ ]\n" +
                "  }\n" +
                "}", connectorId, schemaName, tableName);
    }

    protected static FlightClient createFlightClient(BufferAllocator allocator, int serverPort) throws IOException
    {
        InputStream trustedCertificate = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/certs/server.crt")));
        Location location = Location.forGrpcTls("localhost", serverPort);
        return FlightClient.builder(allocator, location).useTls().trustedCertificates(trustedCertificate).build();
    }
}
