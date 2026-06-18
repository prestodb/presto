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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTransactionHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.flightshim.TestFlightShimRequest.REQUEST_JSON_CODEC;
import static com.facebook.presto.util.ResourceFileUtils.getResourceFile;
import static java.lang.String.format;

@Test(singleThreaded = true)
public abstract class AbstractTestFlightShimJdbcPlugins
        extends AbstractTestFlightShimPlugins
{
    public static final JsonCodec<JdbcColumnHandle> COLUMN_HANDLE_JSON_CODEC = jsonCodec(JdbcColumnHandle.class);
    public static final JsonCodec<JdbcTableHandle> TABLE_HANDLE_JSON_CODEC = jsonCodec(JdbcTableHandle.class);
    public static final JsonCodec<JdbcTransactionHandle> TRANSACTION_HANDLE_JSON_CODEC = jsonCodec(JdbcTransactionHandle.class);

    protected abstract String getConnectionUrl();

    @Override
    protected Map<String, String> getConnectorProperties()
    {
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("connection-url", getConnectionUrl());
        return connectorProperties;
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

    private JdbcColumnHandle convertToJdbcColumnHandle(TpchColumnHandle columnHandle)
    {
        Type type = columnHandle.getType();
        return new JdbcColumnHandle(
                getConnectorId(),
                columnHandle.getColumnName(),
                new JdbcTypeHandle(jdbcDataType(type), type.getDisplayName(), columnSize(type), 0),
                type,
                false,
                Optional.empty());
    }

    @Override
    protected FlightShimRequest createTpchTableRequest(int partNumber, int totalParts, List<TpchColumnHandle> columnHandles)
    {
        Preconditions.checkArgument(totalParts == 1, "JDBC request must be of a single partition");
        String split = createJdbcSplit(getConnectorId(), "tpch", TPCH_TABLE);
        byte[] splitBytes = split.getBytes(StandardCharsets.UTF_8);

        ImmutableList.Builder<RowType.Field> fieldBuilder = ImmutableList.builder();
        ImmutableList.Builder<byte[]> columnBuilder = ImmutableList.builder();
        for (TpchColumnHandle columnHandle : columnHandles) {
            fieldBuilder.add(new RowType.Field(Optional.of(columnHandle.getColumnName()), columnHandle.getType()));
            columnBuilder.add(COLUMN_HANDLE_JSON_CODEC.toJsonBytes(convertToJdbcColumnHandle(columnHandle)));
        }

        JdbcTableHandle tableHandle = new JdbcTableHandle(getConnectorId(), new SchemaTableName("tpch", TPCH_TABLE), getConnectorId(), "tpch", TPCH_TABLE);
        byte[] tableHandleBytes = TABLE_HANDLE_JSON_CODEC.toJsonBytes(tableHandle);
        byte[] transactionHandleBytes = TRANSACTION_HANDLE_JSON_CODEC.toJsonBytes(new JdbcTransactionHandle());

        return new FlightShimRequest(getConnectorId(), fieldBuilder.build(), splitBytes, columnBuilder.build(), tableHandleBytes, Optional.empty(), transactionHandleBytes);
    }

    protected FlightShimRequest createTpchTableRequestWithTupleDomain() throws Exception
    {
        JdbcColumnHandle orderKeyHandle = convertToJdbcColumnHandle(getOrderKeyColumn());
        byte[] splitBytes = Files.readAllBytes(getResourceFile("split_tuple_domain.json").toPath());

        ImmutableList.Builder<RowType.Field> fieldBuilder = ImmutableList.builder();
        List<JdbcColumnHandle> columnHandles = ImmutableList.of(orderKeyHandle);
        ImmutableList.Builder<byte[]> columnBuilder = ImmutableList.builder();
        for (JdbcColumnHandle columnHandle : columnHandles) {
            fieldBuilder.add(new RowType.Field(Optional.of(columnHandle.getColumnName()), columnHandle.getColumnType()));
            columnBuilder.add(COLUMN_HANDLE_JSON_CODEC.toJsonBytes(columnHandle));
        }

        JdbcTableHandle tableHandle = new JdbcTableHandle(getConnectorId(), new SchemaTableName("tpch", TPCH_TABLE), getConnectorId(), "tpch", TPCH_TABLE);
        byte[] tableHandleBytes = TABLE_HANDLE_JSON_CODEC.toJsonBytes(tableHandle);
        byte[] transactionHandleBytes = TRANSACTION_HANDLE_JSON_CODEC.toJsonBytes(new JdbcTransactionHandle());

        return new FlightShimRequest(
                getConnectorId(),
                fieldBuilder.build(),
                splitBytes,
                columnBuilder.build(),
                tableHandleBytes,
                Optional.empty(),
                transactionHandleBytes);
    }

    protected FlightShimRequest createTpchTableRequestWithAdditionalPredicate()
            throws IOException
    {
        // Query is: "SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)"
        JdbcColumnHandle orderKeyHandle = convertToJdbcColumnHandle(getOrderKeyColumn());
        byte[] splitBytes = Files.readAllBytes(getResourceFile("split_additional_predicate.json").toPath());

        ImmutableList.Builder<RowType.Field> fieldBuilder = ImmutableList.builder();
        List<JdbcColumnHandle> columnHandles = ImmutableList.of(orderKeyHandle);
        ImmutableList.Builder<byte[]> columnBuilder = ImmutableList.builder();
        for (JdbcColumnHandle columnHandle : columnHandles) {
            fieldBuilder.add(new RowType.Field(Optional.of(columnHandle.getColumnName()), columnHandle.getColumnType()));
            columnBuilder.add(COLUMN_HANDLE_JSON_CODEC.toJsonBytes(columnHandle));
        }

        JdbcTableHandle tableHandle = new JdbcTableHandle(getConnectorId(), new SchemaTableName("tpch", TPCH_TABLE), getConnectorId(), "tpch", TPCH_TABLE);
        byte[] tableHandleBytes = TABLE_HANDLE_JSON_CODEC.toJsonBytes(tableHandle);
        byte[] transactionHandleBytes = TRANSACTION_HANDLE_JSON_CODEC.toJsonBytes(new JdbcTransactionHandle());

        return new FlightShimRequest(
                getConnectorId(),
                fieldBuilder.build(),
                splitBytes,
                columnBuilder.build(),
                tableHandleBytes,
                Optional.empty(),
                transactionHandleBytes);
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

    static int jdbcDataType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return Types.BOOLEAN;
        }
        if (type.equals(BIGINT)) {
            return Types.BIGINT;
        }
        if (type.equals(INTEGER)) {
            return Types.INTEGER;
        }
        if (type.equals(SMALLINT)) {
            return Types.SMALLINT;
        }
        if (type.equals(TINYINT)) {
            return Types.TINYINT;
        }
        if (type.equals(REAL)) {
            return Types.REAL;
        }
        if (type.equals(DOUBLE)) {
            return Types.DOUBLE;
        }
        if (type instanceof DecimalType) {
            return Types.DECIMAL;
        }
        if (isVarcharType(type)) {
            return Types.VARCHAR;
        }
        if (isCharType(type)) {
            return Types.CHAR;
        }
        if (type.equals(VARBINARY)) {
            return Types.VARBINARY;
        }
        if (type.equals(TIME)) {
            return Types.TIME;
        }
        if (type.equals(TIME_WITH_TIME_ZONE)) {
            return Types.TIME_WITH_TIMEZONE;
        }
        if (type.equals(TIMESTAMP)) {
            return Types.TIMESTAMP;
        }
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return Types.TIMESTAMP_WITH_TIMEZONE;
        }
        if (type.equals(DATE)) {
            return Types.DATE;
        }
        if (type instanceof ArrayType) {
            return Types.ARRAY;
        }
        return Types.JAVA_OBJECT;
    }

    static int columnSize(Type type)
    {
        if (type.equals(BIGINT)) {
            return 19;  // 2**63-1
        }
        if (type.equals(INTEGER)) {
            return 10;  // 2**31-1
        }
        if (type.equals(SMALLINT)) {
            return 5;   // 2**15-1
        }
        if (type.equals(TINYINT)) {
            return 3;   // 2**7-1
        }
        if (type instanceof DecimalType) {
            return ((DecimalType) type).getPrecision();
        }
        if (type.equals(REAL)) {
            return 24; // IEEE 754
        }
        if (type.equals(DOUBLE)) {
            return 53; // IEEE 754
        }
        if (isVarcharType(type)) {
            return ((VarcharType) type).getLength();
        }
        if (isCharType(type)) {
            return ((CharType) type).getLength();
        }
        if (type.equals(VARBINARY)) {
            return Integer.MAX_VALUE;
        }
        if (type.equals(TIME)) {
            return 8; // 00:00:00
        }
        if (type.equals(TIME_WITH_TIME_ZONE)) {
            return 8 + 6; // 00:00:00+00:00
        }
        if (type.equals(DATE)) {
            return 14; // +5881580-07-11 (2**31-1 days)
        }
        if (type.equals(TIMESTAMP)) {
            return 15 + 8;
        }
        if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return 15 + 8 + 6;
        }
        return 0;
    }
}
