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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
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
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;

@Test(singleThreaded = true)
public abstract class AbstractTestFlightShimBase
        extends AbstractTestQueryFramework
{
    public static final JsonCodec<FlightShimRequest> REQUEST_JSON_CODEC = jsonCodec(FlightShimRequest.class);
    public static final JsonCodec<JdbcColumnHandle> COLUMN_HANDLE_JSON_CODEC = jsonCodec(JdbcColumnHandle.class);
    public static final String CUSTKEY_COLUMN = "custkey";
    public static final String NAME_COLUMN = "name";
    protected final List<AutoCloseable> closables  = new ArrayList<>();
    protected static final CallOption CALL_OPTIONS = CallOptions.timeout(300, TimeUnit.SECONDS);
    protected BufferAllocator allocator;
    protected FlightServer server;

    protected abstract String getConnectorId();

    protected abstract String getConnectionUrl();

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
        connectorProperties.putIfAbsent("connection-url", getConnectionUrl());
        pluginManager.setCatalogProperties(getConnectorId(), getConnectorId(), connectorProperties);

        // Make sure these resources close properly
        allocator = injector.getInstance(BufferAllocator.class);
        closables.add(allocator);
        closables.add(injector.getInstance(FlightShimProducer.class));

        // Allow subclasses to setup with injected dependencies
        setup(injector);
    }

    protected void setup(Injector injector)
    {

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

    static String createJdbcSplit(String connectorId, String schemaName, String tableName)
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

    protected JdbcColumnHandle getCustKeyColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                CUSTKEY_COLUMN,
                new JdbcTypeHandle(Types.BIGINT, "bigint", 8, 0),
                BigintType.BIGINT,
                false,
                Optional.empty()
        );
    }

    protected JdbcColumnHandle getNameColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                NAME_COLUMN,
                new JdbcTypeHandle(Types.VARCHAR, "varchar", 32, 0),
                VarcharType.createVarcharType(32),
                false,
                Optional.empty()
        );
    }

    protected List<JdbcColumnHandle> getAllColumns()
    {
        ImmutableList.Builder<JdbcColumnHandle> columnBuilder = ImmutableList.builder();
        columnBuilder.add(getCustKeyColumn());
        columnBuilder.add(getNameColumn());
        return columnBuilder.build();
    }

    protected FlightShimRequest createTpchCustomerRequest(List<JdbcColumnHandle> columnHandles)
    {
        String split = createJdbcSplit(getConnectorId(), "tpch", "customer");
        byte[] splitBytes = split.getBytes(StandardCharsets.UTF_8);

        ImmutableList.Builder<byte[]> columnBuilder = ImmutableList.builder();
        for (JdbcColumnHandle columnHandle : columnHandles) {
            columnBuilder.add(COLUMN_HANDLE_JSON_CODEC.toJsonBytes(columnHandle));
        }

        return new FlightShimRequest(
                getConnectorId(),
                splitBytes,
                columnBuilder.build());
    }

    protected static FlightClient createFlightClient(BufferAllocator allocator, int serverPort) throws IOException
    {
        InputStream trustedCertificate = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/server.crt")));
        Location location = Location.forGrpcTls("localhost", serverPort);
        return FlightClient.builder(allocator, location).useTls().trustedCertificates(trustedCertificate).build();
    }
}
