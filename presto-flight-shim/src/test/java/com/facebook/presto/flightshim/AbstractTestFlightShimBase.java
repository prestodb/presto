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
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Module;
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
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.getFlightServerShimConfig;
import static java.lang.String.format;

@Test(singleThreaded = true)
public abstract class AbstractTestFlightShimBase
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

    protected abstract String getConnectorId();

    protected abstract String getConnectionUrl();

    protected abstract String getPluginBundles();

    @BeforeClass
    public void setup()
            throws Exception
    {
        Module testModule = binder -> {
            binder.bind(ConnectorManager.class).toProvider(() -> null);
        };

        Injector injector = FlightShimServer.initialize(getFlightServerShimConfig(getPluginBundles(), true), testModule);

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

    protected static String removeDatabaseFromJdbcUrl(String jdbcUrl)
    {
        return jdbcUrl.replaceFirst("/[^/?]+([?]|$)", "/$1");
    }

    protected static String addDatabaseCredentialsToJdbcUrl(String jdbcUrl, String username, String password)
    {
        return jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") +
                "user=" + username + "&password=" + password;
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        for (AutoCloseable closeable : Lists.reverse(closables)) {
            closeable.close();
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

    protected JdbcColumnHandle getOrderKeyColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                ORDERKEY_COLUMN,
                new JdbcTypeHandle(Types.BIGINT, "bigint", 8, 0),
                BigintType.BIGINT,
                false,
                Optional.empty());
    }

    protected JdbcColumnHandle getLineNumberColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                LINENUMBER_COLUMN,
                new JdbcTypeHandle(Types.INTEGER, "integer", 4, 0),
                IntegerType.INTEGER,
                false,
                Optional.empty());
    }

    protected JdbcColumnHandle getLineStatusColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                LINESTATUS_COLUMN,
                new JdbcTypeHandle(Types.VARCHAR, "varchar", 32, 0),
                VarcharType.createVarcharType(32),
                false,
                Optional.empty());
    }

    protected JdbcColumnHandle getQuantityColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                QUANTITY_COLUMN,
                new JdbcTypeHandle(Types.DOUBLE, "double", 8, 0),
                DoubleType.DOUBLE,
                false,
                Optional.empty());
    }

    protected JdbcColumnHandle getExtendedPriceColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                EXTENDEDPRICE_COLUMN,
                new JdbcTypeHandle(Types.DOUBLE, "double", 8, 0),
                DoubleType.DOUBLE,
                false,
                Optional.empty());
    }

    protected JdbcColumnHandle getShipDateColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                SHIPDATE_COLUMN,
                new JdbcTypeHandle(Types.DATE, "date", 8, 0),
                DateType.DATE,
                false,
                Optional.empty());
    }

    protected JdbcColumnHandle getShipTimestampColumn()
    {
        return new JdbcColumnHandle(
                getConnectorId(),
                SHIPDATE_COLUMN,
                new JdbcTypeHandle(Types.TIMESTAMP, "timestamp", 8, 0),
                TimestampType.TIMESTAMP,
                false,
                Optional.empty());
    }

    protected List<JdbcColumnHandle> getAllColumns()
    {
        ImmutableList.Builder<JdbcColumnHandle> columnBuilder = ImmutableList.builder();
        columnBuilder.add(getOrderKeyColumn());
        columnBuilder.add(getLineNumberColumn());
        columnBuilder.add(getLineStatusColumn());
        columnBuilder.add(getQuantityColumn());
        columnBuilder.add(getExtendedPriceColumn());
        columnBuilder.add(getShipDateColumn());
        return columnBuilder.build();
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

        // TODO: remove - code for generating JSON, need presto-common test-jar
        /*
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
        ObjectMapper mapper = new JsonObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(JdbcSplit.class, new JsonDeserializer<JdbcSplit>() {
                            @Override
                            public JdbcSplit deserialize(JsonParser jsonParser, DeserializationContext ctxt)
                                    throws IOException
                            {
                                return new JsonObjectMapperProvider().get().readValue(jsonParser, JdbcSplit.class);
                            }
                        })
                        .addDeserializer(ColumnHandle.class, new JsonDeserializer<ColumnHandle>()
                        {
                            @Override
                            public ColumnHandle deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                                    throws IOException
                            {
                                return new JsonObjectMapperProvider().get().readValue(jsonParser, JdbcColumnHandle.class);
                            }
                        })
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));
        TupleDomain<JdbcColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                orderKeyHandle, Domain.create(SortedRangeSet.copyOf(BIGINT, ImmutableList.of(Range.equal(BIGINT, 3L))), false)));
        String json = mapper.writeValueAsString(tupleDomain);
        //Object o = mapper.readValue(json, new TypeReference<TupleDomain<ColumnHandle>>() {});
        //Object obj = mapper.readValue(tmp, new TypeReference<JdbcSplit>() {});
        //String json = jsonCodec(tupleDomain.getClass()).toJson(tupleDomain);
        */

        String split = format("{\n" +
                "  \"connectorId\" : \"postgresql\",\n" +
                "  \"schemaName\" : \"tpch\",\n" +
                "  \"tableName\" : \"lineitem\",\n" +
                "  \"tupleDomain\" : {\n" +
                "  \"columnDomains\" : [ {\n" +
                "    \"column\" : {\n" +
                "      \"connectorId\" : \"postgresql\",\n" +
                "      \"columnName\" : \"orderkey\",\n" +
                "      \"jdbcTypeHandle\" : {\n" +
                "        \"jdbcType\" : -5,\n" +
                "        \"jdbcTypeName\" : \"bigint\",\n" +
                "        \"columnSize\" : 8,\n" +
                "        \"decimalDigits\" : 0\n" +
                "      },\n" +
                "      \"columnType\" : \"bigint\",\n" +
                "      \"nullable\" : false\n" +
                "    },\n" +
                "    \"domain\" : {\n" +
                "      \"values\" : {\n" +
                "        \"@type\" : \"sortable\",\n" +
                "        \"type\" : \"bigint\",\n" +
                "        \"ranges\" : [ {\n" +
                "          \"low\" : {\n" +
                "            \"type\" : \"bigint\",\n" +
                "            \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAMAAAAAAAAA\",\n" +
                "            \"bound\" : \"EXACTLY\"\n" +
                "          },\n" +
                "          \"high\" : {\n" +
                "            \"type\" : \"bigint\",\n" +
                "            \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAMAAAAAAAAA\",\n" +
                "            \"bound\" : \"EXACTLY\"\n" +
                "          }\n" +
                "        } ]\n" +
                "      },\n" +
                "      \"nullAllowed\" : false\n" +
                "    }\n" +
                "  } ]\n" +
                "  }\n" +
                "}");

        byte[] splitBytes = split.getBytes(StandardCharsets.UTF_8);

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
    {
        // "SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)"
        JdbcColumnHandle orderKeyHandle = getOrderKeyColumn();

        String split = format("{\n" +
                "\"connectorId\" : \"postgresql\",\n" +
                "\"schemaName\" : \"tpch\",\n" +
                "\"tableName\" : \"orders\",\n" +
                "\"tupleDomain\" : {\n" +
                "  \"columnDomains\" : [ {\n" +
                "    \"column\" : {\n" +
                "      \"@type\" : \"postgresql\",\n" +
                "      \"connectorId\" : \"postgresql\",\n" +
                "      \"columnName\" : \"orderkey\",\n" +
                "      \"jdbcTypeHandle\" : {\n" +
                "        \"jdbcType\" : -5,\n" +
                "        \"jdbcTypeName\" : \"int8\",\n" +
                "        \"columnSize\" : 19,\n" +
                "        \"decimalDigits\" : 0\n" +
                "      },\n" +
                "      \"columnType\" : \"bigint\",\n" +
                "      \"nullable\" : true\n" +
                "    },\n" +
                "    \"domain\" : {\n" +
                "      \"values\" : {\n" +
                "        \"@type\" : \"sortable\",\n" +
                "        \"type\" : \"bigint\",\n" +
                "        \"ranges\" : [ {\n" +
                "          \"low\" : {\n" +
                "            \"type\" : \"bigint\",\n" +
                "            \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA\",\n" +
                "            \"bound\" : \"EXACTLY\"\n" +
                "          },\n" +
                "          \"high\" : {\n" +
                "            \"type\" : \"bigint\",\n" +
                "            \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA\",\n" +
                "            \"bound\" : \"EXACTLY\"\n" +
                "          }\n" +
                "        }, {\n" +
                "          \"low\" : {\n" +
                "            \"type\" : \"bigint\",\n" +
                "            \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAIAAAAAAAAA\",\n" +
                "            \"bound\" : \"EXACTLY\"\n" +
                "          },\n" +
                "          \"high\" : {\n" +
                "            \"type\" : \"bigint\",\n" +
                "            \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAIAAAAAAAAA\",\n" +
                "            \"bound\" : \"EXACTLY\"\n" +
                "          }\n" +
                "        }, {\n" +
                "          \"low\" : {\n" +
                "            \"type\" : \"bigint\",\n" +
                "            \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAMAAAAAAAAA\",\n" +
                "            \"bound\" : \"EXACTLY\"\n" +
                "          },\n" +
                "          \"high\" : {\n" +
                "            \"type\" : \"bigint\",\n" +
                "            \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAMAAAAAAAAA\",\n" +
                "            \"bound\" : \"EXACTLY\"\n" +
                "          }\n" +
                "        } ]\n" +
                "      },\n" +
                "      \"nullAllowed\" : false\n" +
                "    }\n" +
                "  } ]\n" +
                "},\n" +
                "\"additionalProperty\" : {\n" +
                "  \"translatedString\" : \"((\\\"orderkey\\\") IN ((?) , (?) , (?)))\",\n" +
                "  \"boundConstantValues\" : [ {\n" +
                "    \"@type\" : \"constant\",\n" +
                "    \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA\",\n" +
                "    \"type\" : \"bigint\",\n" +
                "    \"sourceLocation\" : {\n" +
                "      \"line\" : 1,\n" +
                "      \"column\" : 22\n" +
                "    }\n" +
                "  }, {\n" +
                "    \"@type\" : \"constant\",\n" +
                "    \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAIAAAAAAAAA\",\n" +
                "    \"type\" : \"bigint\",\n" +
                "    \"sourceLocation\" : {\n" +
                "      \"line\" : 1,\n" +
                "      \"column\" : 22\n" +
                "    }\n" +
                "  }, {\n" +
                "    \"@type\" : \"constant\",\n" +
                "    \"valueBlock\" : \"CgAAAExPTkdfQVJSQVkBAAAAAAMAAAAAAAAA\",\n" +
                "    \"type\" : \"bigint\",\n" +
                "    \"sourceLocation\" : {\n" +
                "      \"line\" : 1,\n" +
                "      \"column\" : 22\n" +
                "    }\n" +
                "  } ]\n" +
                "}\n" +
                "}");

        byte[] splitBytes = split.getBytes(StandardCharsets.UTF_8);

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

    protected static FlightClient createFlightClient(BufferAllocator allocator, int serverPort) throws IOException
    {
        InputStream trustedCertificate = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/certs/server.crt")));
        Location location = Location.forGrpcTls("localhost", serverPort);
        return FlightClient.builder(allocator, location).useTls().trustedCertificates(trustedCertificate).build();
    }
}
