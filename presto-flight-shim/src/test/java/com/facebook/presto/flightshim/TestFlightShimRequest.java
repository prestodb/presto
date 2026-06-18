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
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTransactionHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.flightshim.AbstractTestFlightShimJdbcPlugins.COLUMN_HANDLE_JSON_CODEC;
import static com.facebook.presto.flightshim.AbstractTestFlightShimJdbcPlugins.TABLE_HANDLE_JSON_CODEC;
import static com.facebook.presto.flightshim.AbstractTestFlightShimJdbcPlugins.TRANSACTION_HANDLE_JSON_CODEC;
import static com.facebook.presto.flightshim.AbstractTestFlightShimJdbcPlugins.createJdbcSplit;
import static com.facebook.presto.flightshim.AbstractTestFlightShimPlugins.LINESTATUS_COLUMN;
import static com.facebook.presto.flightshim.AbstractTestFlightShimPlugins.ORDERKEY_COLUMN;
import static com.facebook.presto.flightshim.AbstractTestFlightShimPlugins.TPCH_TABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestFlightShimRequest
{
    public static final JsonCodec<FlightShimRequest> REQUEST_JSON_CODEC;

    static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        public TestingTypeDeserializer(FunctionAndTypeManager functionAndTypeManager)
        {
            super(Type.class);
            this.functionAndTypeManager = functionAndTypeManager;
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return functionAndTypeManager.getType(parseTypeSignature(value));
        }
    }

    static {
        Metadata metadata = MetadataManager.createTestMetadataManager();
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        TestingTypeDeserializer typeDeserializer = new TestingTypeDeserializer(functionAndTypeManager);
        provider.setJsonDeserializers(ImmutableMap.of(RowType.class, typeDeserializer, Type.class, typeDeserializer));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        REQUEST_JSON_CODEC = codecFactory.jsonCodec(FlightShimRequest.class);
    }

    @Test
    public void testJsonRoundTrip()
    {
        FlightShimRequest expected = createTpchCustomerRequest();
        String json = REQUEST_JSON_CODEC.toJson(expected);
        FlightShimRequest copy = REQUEST_JSON_CODEC.fromJson(json);
        assertEquals(copy.getConnectorId(), expected.getConnectorId());
        assertEquals(copy.getFields(), expected.getFields());
        assertEquals(copy.getSplitBytes(), expected.getSplitBytes());
        assertArrayEquals(copy.getColumnHandlesBytes().toArray(), expected.getColumnHandlesBytes().toArray());
        assertEquals(copy.getTableHandleBytes(), expected.getTableHandleBytes());
        assertEquals(copy.getTableLayoutHandleBytes(), expected.getTableLayoutHandleBytes());
        assertEquals(copy.getTransactionHandleBytes(), expected.getTransactionHandleBytes());
    }

    FlightShimRequest createTpchCustomerRequest()
    {
        String split = createJdbcSplit("postgresql", "tpch", TPCH_TABLE);
        byte[] splitBytes = split.getBytes(StandardCharsets.UTF_8);

        JdbcColumnHandle custkeyHandle = new JdbcColumnHandle(
                "postgresql",
                ORDERKEY_COLUMN,
                new JdbcTypeHandle(Types.BIGINT, "bigint", 8, 0),
                BigintType.BIGINT,
                false,
                Optional.empty());
        byte[] custkeyBytes = COLUMN_HANDLE_JSON_CODEC.toJsonBytes(custkeyHandle);

        JdbcColumnHandle nameHandle = new JdbcColumnHandle(
                "postgresql",
                LINESTATUS_COLUMN,
                new JdbcTypeHandle(Types.VARCHAR, "varchar", 32, 0),
                VarcharType.createVarcharType(32),
                false,
                Optional.empty());
        byte[] nameBytes = COLUMN_HANDLE_JSON_CODEC.toJsonBytes(nameHandle);

        ImmutableList<RowType.Field> fields = ImmutableList.of(
                new RowType.Field(Optional.of(custkeyHandle.getColumnName()), custkeyHandle.getColumnType()),
                new RowType.Field(Optional.of(nameHandle.getColumnName()), nameHandle.getColumnType()));

        JdbcTableHandle tableHandle = new JdbcTableHandle("postgresql", new SchemaTableName("tpch", TPCH_TABLE), "postgresql", "tpch", TPCH_TABLE);
        byte[] tableHandleBytes = TABLE_HANDLE_JSON_CODEC.toJsonBytes(tableHandle);

        JdbcTransactionHandle transactionHandle = new JdbcTransactionHandle();
        byte[] transactionHandleBytes = TRANSACTION_HANDLE_JSON_CODEC.toJsonBytes(transactionHandle);

        return new FlightShimRequest(
                "postgresql",
                fields,
                splitBytes,
                ImmutableList.of(custkeyBytes, nameBytes),
                tableHandleBytes,
                Optional.empty(),
                transactionHandleBytes);
    }
}
