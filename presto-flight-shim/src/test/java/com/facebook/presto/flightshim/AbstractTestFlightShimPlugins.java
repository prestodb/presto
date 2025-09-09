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
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
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
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTransactionHandle;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.flightshim.TestFlightShimRequest.REQUEST_JSON_CODEC;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static java.lang.String.format;

@Test(singleThreaded = true)
public abstract class AbstractTestFlightShimPlugins
        extends AbstractTestQueryFramework
{
    public static final JsonCodec<TpchColumnHandle> TPCH_COLUMN_JSON_CODEC = jsonCodec(TpchColumnHandle.class);
    public static final JsonCodec<TpchTableHandle> TPCH_TABLE_HANDLE_JSON_CODEC = jsonCodec(TpchTableHandle.class);
    public static final JsonCodec<TpchTransactionHandle> TPCH_TRANSACTION_HANDLE_JSON_CODEC = jsonCodec(TpchTransactionHandle.class);
    public static final String TPCH_TABLE = "lineitem";
    public static final String ORDERKEY_COLUMN = "orderkey";
    public static final String LINENUMBER_COLUMN = "linenumber";
    public static final String LINESTATUS_COLUMN = "linestatus";
    public static final String EXTENDEDPRICE_COLUMN = "extendedprice";
    public static final String QUANTITY_COLUMN = "quantity";
    public static final String SHIPDATE_COLUMN = "shipdate";
    public static final String SHIPINSTRUCT_COLUMN = "shipinstruct";
    protected static final CallOption CALL_OPTIONS = CallOptions.timeout(300, TimeUnit.SECONDS);
    protected BufferAllocator allocator;
    protected FlightShimProducer producer;
    protected FlightServer server;
    private ArrowBlockBuilder blockBuilder;

    protected abstract String getConnectorId();

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

        // Create the catalog for the test connector
        ImmutableMap.Builder<String, String> propertyBuilder = ImmutableMap.builder();
        propertyBuilder.put("connector.name", getConnectorId()).putAll(getConnectorProperties());

        Injector injector = FlightShimServer.initialize(configBuilder.build());

        server = FlightShimServer.start(injector, FlightServer.builder(), ImmutableMap.of(getConnectorId(), propertyBuilder.build()));
        producer = injector.getInstance(FlightShimProducer.class);
        allocator = injector.getInstance(BufferAllocator.class);

        TypeManager typeManager = injector.getInstance(TypeManager.class);
        blockBuilder = new ArrowBlockBuilder(typeManager);
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        super.close();
        if (server != null && producer != null) {
            server.shutdown();
            producer.shutdown();
        }
        if (server != null) {
            server.close();
            server = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    protected Map<String, String> getConnectorProperties()
    {
        return ImmutableMap.of();
    }

    protected int getExpectedTotalParts()
    {
        return 1;
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        return new FlightShimQueryRunner(getExpectedTotalParts());
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
            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchTableRequest(0, 1, ImmutableList.of(getOrderKeyColumn()))));

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

    protected List<TpchColumnHandle> getHandlesFromSelectQuery(String sql)
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

        ImmutableList.Builder<TpchColumnHandle> columnHandlesBuilder = ImmutableList.builder();
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
                case SHIPINSTRUCT_COLUMN:
                    columnHandlesBuilder.add(getShipInstructColumn());
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
        private int totalParts;

        public FlightShimQueryRunner(int totalParts)
        {
            this.totalParts = totalParts;
        }

        @Override
        public Session getDefaultSession()
        {
            return ((QueryRunner) getExpectedQueryRunner()).getDefaultSession();
        }

        protected int getTotalParts()
        {
            return totalParts;
        }

        protected FlightShimRequest getRequestForPart(int partNumber, List<TpchColumnHandle> columnHandles)
        {
            return createTpchTableRequest(partNumber, getTotalParts(), columnHandles);
        }

        @Override
        public MaterializedResult execute(String sql)
        {
            try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                    FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {
                List<TpchColumnHandle> columnHandles = getHandlesFromSelectQuery(sql);
                List<Page> pages = new ArrayList<>();

                for (int i = 0; i < getTotalParts(); i++) {
                    Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(getRequestForPart(i, columnHandles)));
                    try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                        while (stream.next()) {
                            List<Block> blocks = new ArrayList<>();
                            for (TpchColumnHandle columnHandle : columnHandles) {
                                FieldVector vector = stream.getRoot().getVector(columnHandle.getColumnName());
                                Block block = blockBuilder.buildBlockFromFieldVector(vector, columnHandle.getType(), null);
                                blocks.add(block);
                            }
                            pages.add(new Page(stream.getRoot().getRowCount(), blocks.toArray(new Block[0])));
                        }
                    }
                }

                List<Type> types = columnHandles.stream().map(TpchColumnHandle::getType).collect(Collectors.toList());
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

    protected TpchColumnHandle getOrderKeyColumn()
    {
        return new TpchColumnHandle(ORDERKEY_COLUMN, BigintType.BIGINT);
    }

    protected TpchColumnHandle getLineNumberColumn()
    {
        return new TpchColumnHandle(LINENUMBER_COLUMN, IntegerType.INTEGER);
    }

    protected TpchColumnHandle getLineStatusColumn()
    {
        return new TpchColumnHandle(LINESTATUS_COLUMN, VarcharType.createVarcharType(32));
    }

    protected TpchColumnHandle getQuantityColumn()
    {
        return new TpchColumnHandle(QUANTITY_COLUMN, DoubleType.DOUBLE);
    }

    protected TpchColumnHandle getExtendedPriceColumn()
    {
        return new TpchColumnHandle(EXTENDEDPRICE_COLUMN, DoubleType.DOUBLE);
    }

    protected TpchColumnHandle getShipDateColumn()
    {
        return new TpchColumnHandle(SHIPDATE_COLUMN, DateType.DATE);
    }

    protected TpchColumnHandle getShipInstructColumn()
    {
        return new TpchColumnHandle(SHIPINSTRUCT_COLUMN, VarcharType.createVarcharType(64));
    }

    protected FlightShimRequest createTpchTableRequest(int partNumber, int totalParts, List<TpchColumnHandle> columnHandles)
    {
        String split = createTpchSplit(TPCH_TABLE, partNumber, totalParts);
        byte[] splitBytes = split.getBytes(StandardCharsets.UTF_8);

        ImmutableList.Builder<RowType.Field> fieldBuilder = ImmutableList.builder();
        ImmutableList.Builder<byte[]> columnBuilder = ImmutableList.builder();
        for (TpchColumnHandle columnHandle : columnHandles) {
            fieldBuilder.add(new RowType.Field(Optional.of(columnHandle.getColumnName()), columnHandle.getType()));
            columnBuilder.add(TPCH_COLUMN_JSON_CODEC.toJsonBytes(columnHandle));
        }
        RowType outputType = RowType.from(fieldBuilder.build());

        TpchTableHandle tableHandle = new TpchTableHandle(TPCH_TABLE, 1.0);
        byte[] tableHandleBytes = TPCH_TABLE_HANDLE_JSON_CODEC.toJsonBytes(tableHandle);

        byte[] transactionHandleBytes = TPCH_TRANSACTION_HANDLE_JSON_CODEC.toJsonBytes(TpchTransactionHandle.INSTANCE);

        return new FlightShimRequest(getConnectorId(), outputType, splitBytes, columnBuilder.build(), tableHandleBytes, Optional.empty(), transactionHandleBytes);
    }

    protected static String createTpchSplit(String tableName, int partNumber, int totalParts)
    {
        return format("{\n" +
                "  \"tableHandle\" : {\n" +
                "    \"tableName\" : \"%s\",\n" +
                "    \"scaleFactor\" : %.2f\n" +
                "  },\n" +
                "  \"partNumber\" : %d,\n" +
                "  \"totalParts\" : %d,\n" +
                "  \"addresses\" : [ \"127.0.0.1:9999\" ],\n" +
                "  \"predicate\" : {\n" +
                "    \"columnDomains\" : [ ]\n" +
                "  }\n" +
                "}", tableName, TINY_SCALE_FACTOR, partNumber, totalParts);
    }

    protected static FlightClient createFlightClient(BufferAllocator allocator, int serverPort) throws IOException
    {
        InputStream trustedCertificate = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/certs/server.crt")));
        Location location = Location.forGrpcTls("localhost", serverPort);
        return FlightClient.builder(allocator, location).useTls().trustedCertificates(trustedCertificate).build();
    }
}
