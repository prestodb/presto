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
import com.facebook.airlift.testing.postgresql.TestingPostgreSqlServer;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.AssertJUnit.assertNotNull;

public class TestFlightShimProducer
        extends AbstractTestQueryFramework
{
    private static final CallOption CALL_OPTIONS = CallOptions.timeout(300, TimeUnit.SECONDS);
    private static final JsonCodec<FlightShimRequest> REQUEST_JSON_CODEC = jsonCodec(FlightShimRequest.class);
    private static final JsonCodec<JdbcColumnHandle> COLUMN_HANDLE_JSON_CODEC = jsonCodec(JdbcColumnHandle.class);
    private final List<AutoCloseable> closables  = new ArrayList<>();
    private final TestingPostgreSqlServer postgreSqlServer;
    private BufferAllocator allocator;
    private FlightServer server;

    public TestFlightShimProducer()
            throws Exception
    {
        this.postgreSqlServer = new TestingPostgreSqlServer("testuser", "tpch");
        closables.add(postgreSqlServer);
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.builder();
        configBuilder.put("flight-shim.server", "localhost");
        configBuilder.put("flight-shim.server.port", String.valueOf(findUnusedPort()));
        configBuilder.put("flight-shim.server-ssl-certificate-file", "src/test/resources/server.crt");
        configBuilder.put("flight-shim.server-ssl-key-file", "src/test/resources/server.key");

        // Allow for 3 batches using testing tpch db
        configBuilder.put("flight-shim.max-rows-per-batch", String.valueOf(500));

        Injector injector = FlightShimServer.initialize(configBuilder.build());

        server = FlightShimServer.start(injector, FlightServer.builder());
        closables.add(server);

        // Set test properties after catalogs have been loaded
        FlightShimPluginManager pluginManager = injector.getInstance(FlightShimPluginManager.class);
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("connection-url", postgreSqlServer.getJdbcUrl());
        pluginManager.setCatalogProperties("postgresql", "postgresql", connectorProperties);

        // Make sure these resources close properly
        allocator = injector.getInstance(BufferAllocator.class);
        closables.add(allocator);
        closables.add(injector.getInstance(FlightShimProducer.class));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ImmutableMap.of(), TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        for (AutoCloseable closeable : Lists.reverse(closables)) {
            closeable.close();
        }
    }

    private int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String createJdbcSplit(String connectorId, String schemaName, String tableName)
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

    private static FlightShimRequest createTpchCustomerRequest()
    {
        String connectorId = "postgresql";
        String split = createJdbcSplit(connectorId, "tpch", "customer");
        byte[] splitBytes = split.getBytes(StandardCharsets.UTF_8);

        JdbcColumnHandle custkeyHandle = new JdbcColumnHandle(
                connectorId,
                "custkey",
                new JdbcTypeHandle(Types.BIGINT, "bigint", 8, 0),
                BigintType.BIGINT,
                false,
                Optional.empty()
        );
        byte[] custkeyHandleBytes = COLUMN_HANDLE_JSON_CODEC.toJsonBytes(custkeyHandle);

        JdbcColumnHandle nameHandle = new JdbcColumnHandle(
                connectorId,
                "name",
                new JdbcTypeHandle(Types.VARCHAR, "varchar", 32, 0),
                VarcharType.createVarcharType(32),
                false,
                Optional.empty()
        );
        byte[] nameHandleBytes = COLUMN_HANDLE_JSON_CODEC.toJsonBytes(nameHandle);

        return new FlightShimRequest(
                connectorId,
                splitBytes,
                ImmutableList.of(custkeyHandleBytes, nameHandleBytes));
    }

    @Test
    public void testConnectorGetStream() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {

            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchCustomerRequest()));

            int rowCount = 0;
            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    rowCount += stream.getRoot().getRowCount();
                }
            }

            // TODO compare results against query
            assertGreaterThan(rowCount, 0);
        }
    }

    @Test
    public void testStopStreamAtLimit() throws Exception
    {
        int rowLimit = 500;
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {

            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchCustomerRequest()));

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

            Ticket ticket = new Ticket(REQUEST_JSON_CODEC.toJsonBytes(createTpchCustomerRequest()));

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
                        Thread.sleep(1000);
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

    private static FlightClient createFlightClient(BufferAllocator allocator, int serverPort) throws IOException
    {
        InputStream trustedCertificate = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/server.crt")));
        Location location = Location.forGrpcTls("localhost", serverPort);
        return FlightClient.builder(allocator, location).useTls().trustedCertificates(trustedCertificate).build();
    }
}
