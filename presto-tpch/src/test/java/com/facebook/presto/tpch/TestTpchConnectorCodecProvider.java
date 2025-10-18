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
package com.facebook.presto.tpch;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomainSerde;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestTpchConnectorCodecProvider
{
    private TpchConnectorCodecProvider codecProvider;

    @BeforeMethod
    public void setUp()
    {
        codecProvider = new TpchConnectorCodecProvider(
                new TestingTypeManager(),
                new TestTupleDomainSerde(new ObjectMapper()));
    }

    @Test
    public void testTableHandleSerialization()
    {
        ConnectorCodec<ConnectorTableHandle> codec = codecProvider.getConnectorTableHandleCodec().get();

        TpchTableHandle originalHandle = new TpchTableHandle("customer", 0.01);

        byte[] serialized = codec.serialize(originalHandle);
        ConnectorTableHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TpchTableHandle);
        TpchTableHandle deserializedTpch = (TpchTableHandle) deserialized;
        assertEquals(deserializedTpch.getTableName(), originalHandle.getTableName());
        assertEquals(deserializedTpch.getScaleFactor(), originalHandle.getScaleFactor());
    }

    @Test
    public void testColumnHandleSerialization()
    {
        ConnectorCodec<ColumnHandle> codec = codecProvider.getColumnHandleCodec().get();

        TpchColumnHandle originalHandle = new TpchColumnHandle("c_custkey", BIGINT);

        byte[] serialized = codec.serialize(originalHandle);
        ColumnHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TpchColumnHandle);
        TpchColumnHandle deserializedTpch = (TpchColumnHandle) deserialized;
        assertEquals(deserializedTpch.getColumnName(), originalHandle.getColumnName());
        assertEquals(deserializedTpch.getType(), originalHandle.getType());
    }

    @Test
    public void testColumnHandleWithSubfields()
    {
        ConnectorCodec<ColumnHandle> codec = codecProvider.getColumnHandleCodec().get();

        List<Subfield> subfields = ImmutableList.of(
                new Subfield("field1"),
                new Subfield("field2.nested"));
        TpchColumnHandle originalHandle = new TpchColumnHandle("complex_column", VARCHAR, subfields);

        byte[] serialized = codec.serialize(originalHandle);
        ColumnHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TpchColumnHandle);
        TpchColumnHandle deserializedTpch = (TpchColumnHandle) deserialized;
        assertEquals(deserializedTpch.getColumnName(), originalHandle.getColumnName());
        assertEquals(deserializedTpch.getType(), originalHandle.getType());
        assertEquals(deserializedTpch.getRequiredSubfields(), originalHandle.getRequiredSubfields());
    }

    @Test
    public void testSplitSerialization()
    {
        ConnectorCodec<ConnectorSplit> codec = codecProvider.getConnectorSplitCodec().get();

        TpchTableHandle tableHandle = new TpchTableHandle("orders", 1.0);
        List<HostAddress> addresses = ImmutableList.of(
                HostAddress.fromParts("localhost", 8080),
                HostAddress.fromParts("192.168.1.1", 9090));
        TupleDomain<ColumnHandle> predicate = TupleDomain.all();

        TpchSplit originalSplit = new TpchSplit(tableHandle, 2, 10, addresses, predicate);

        byte[] serialized = codec.serialize(originalSplit);
        ConnectorSplit deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TpchSplit);
        TpchSplit deserializedTpch = (TpchSplit) deserialized;
        assertEquals(deserializedTpch.getTableHandle().getTableName(), originalSplit.getTableHandle().getTableName());
        assertEquals(deserializedTpch.getTableHandle().getScaleFactor(), originalSplit.getTableHandle().getScaleFactor());
        assertEquals(deserializedTpch.getPartNumber(), originalSplit.getPartNumber());
        assertEquals(deserializedTpch.getTotalParts(), originalSplit.getTotalParts());
        assertEquals(deserializedTpch.getAddresses(), originalSplit.getAddresses());
    }

    @Test
    public void testTableLayoutHandleSerialization()
    {
        ConnectorCodec<ConnectorTableLayoutHandle> codec = codecProvider.getConnectorTableLayoutHandleCodec().get();

        TpchTableHandle table = new TpchTableHandle("nation", 0.1);
        TupleDomain<ColumnHandle> predicate = TupleDomain.all();

        TpchTableLayoutHandle originalHandle = new TpchTableLayoutHandle(table, predicate);

        byte[] serialized = codec.serialize(originalHandle);
        ConnectorTableLayoutHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TpchTableLayoutHandle);
        TpchTableLayoutHandle deserializedTpch = (TpchTableLayoutHandle) deserialized;
        assertEquals(deserializedTpch.getTable().getTableName(), originalHandle.getTable().getTableName());
        assertEquals(deserializedTpch.getTable().getScaleFactor(), originalHandle.getTable().getScaleFactor());
    }

    @Test
    public void testPartitioningHandleSerialization()
    {
        ConnectorCodec<ConnectorPartitioningHandle> codec = codecProvider.getConnectorPartitioningHandleCodec().get();

        TpchPartitioningHandle originalHandle = new TpchPartitioningHandle("abc", 123);

        byte[] serialized = codec.serialize(originalHandle);
        ConnectorPartitioningHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TpchPartitioningHandle);
        TpchPartitioningHandle deserializedTpch = (TpchPartitioningHandle) deserialized;
        assertEquals(deserializedTpch.getTable(), originalHandle.getTable());
        assertEquals(deserializedTpch.getTotalRows(), originalHandle.getTotalRows());
    }

    @Test
    public void testAllTpchTableNames()
    {
        ConnectorCodec<ConnectorTableHandle> codec = codecProvider.getConnectorTableHandleCodec().get();

        String[] tableNames = {"customer", "lineitem", "nation", "orders",
                "part", "partsupp", "region", "supplier"};

        for (String tableName : tableNames) {
            TpchTableHandle originalHandle = new TpchTableHandle(tableName, 1.0);

            byte[] serialized = codec.serialize(originalHandle);
            ConnectorTableHandle deserialized = codec.deserialize(serialized);

            assertTrue(deserialized instanceof TpchTableHandle);
            TpchTableHandle deserializedTpch = (TpchTableHandle) deserialized;
            assertEquals(deserializedTpch.getTableName(), originalHandle.getTableName());
            assertEquals(deserializedTpch.getScaleFactor(), originalHandle.getScaleFactor());
        }
    }

    @Test
    public void testColumnHandleWithSpecialCharacters()
    {
        ConnectorCodec<ColumnHandle> codec = codecProvider.getColumnHandleCodec().get();

        TpchColumnHandle originalHandle = new TpchColumnHandle("column with spaces", VARCHAR);

        byte[] serialized = codec.serialize(originalHandle);
        ColumnHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TpchColumnHandle);
        TpchColumnHandle deserializedTpch = (TpchColumnHandle) deserialized;
        assertEquals(deserializedTpch.getColumnName(), originalHandle.getColumnName());
        assertEquals(deserializedTpch.getType(), originalHandle.getType());
    }

    @Test
    public void testVariousScaleFactors()
    {
        ConnectorCodec<ConnectorTableHandle> codec = codecProvider.getConnectorTableHandleCodec().get();

        double[] scaleFactors = {0.01, 0.1, 1.0, 10.0, 100.0, 1000.0};

        for (double scaleFactor : scaleFactors) {
            TpchTableHandle originalHandle = new TpchTableHandle("lineitem", scaleFactor);

            byte[] serialized = codec.serialize(originalHandle);
            ConnectorTableHandle deserialized = codec.deserialize(serialized);

            assertTrue(deserialized instanceof TpchTableHandle);
            TpchTableHandle deserializedTpch = (TpchTableHandle) deserialized;
            assertEquals(deserializedTpch.getTableName(), originalHandle.getTableName());
            assertEquals(deserializedTpch.getScaleFactor(), originalHandle.getScaleFactor());
        }
    }

    @Test
    public void testSplitWithEmptyAddresses()
    {
        ConnectorCodec<ConnectorSplit> codec = codecProvider.getConnectorSplitCodec().get();

        TpchTableHandle tableHandle = new TpchTableHandle("region", 0.01);
        List<HostAddress> addresses = ImmutableList.of();
        TupleDomain<ColumnHandle> predicate = TupleDomain.all();

        TpchSplit originalSplit = new TpchSplit(tableHandle, 1, 2, addresses, predicate);

        byte[] serialized = codec.serialize(originalSplit);
        ConnectorSplit deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TpchSplit);
        TpchSplit deserializedTpch = (TpchSplit) deserialized;
        assertEquals(deserializedTpch.getTableHandle().getTableName(), originalSplit.getTableHandle().getTableName());
        assertEquals(deserializedTpch.getTableHandle().getScaleFactor(), originalSplit.getTableHandle().getScaleFactor());
        assertEquals(deserializedTpch.getPartNumber(), originalSplit.getPartNumber());
        assertEquals(deserializedTpch.getTotalParts(), originalSplit.getTotalParts());
        assertEquals(deserializedTpch.getAddresses(), originalSplit.getAddresses());
    }

    @Test
    public void testColumnHandleDeserialization()
            throws IOException
    {
        ConnectorCodec<ColumnHandle> codec = codecProvider.getColumnHandleCodec().get();

        TpchColumnHandle originalHandle = new TpchColumnHandle("test_column", BIGINT);
        byte[] serialized = codec.serialize(originalHandle);

        ByteArrayInputStream byteIn = new ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(byteIn);

        String columnName = in.readUTF();
        String typeSignature = in.readUTF();
        int subfieldCount = in.readInt();

        assertEquals(columnName, "test_column");
        assertEquals(typeSignature, "bigint");
        assertEquals(subfieldCount, 0);

        ColumnHandle deserialized = codec.deserialize(serialized);
        assertTrue(deserialized instanceof TpchColumnHandle);
        TpchColumnHandle deserializedTpch = (TpchColumnHandle) deserialized;
        assertEquals(deserializedTpch.getColumnName(), "test_column");
        assertEquals(deserializedTpch.getType(), BIGINT);
    }

    @Test
    public void testTableHandleDeserialization()
            throws IOException
    {
        ConnectorCodec<ConnectorTableHandle> codec = codecProvider.getConnectorTableHandleCodec().get();

        TpchTableHandle originalHandle = new TpchTableHandle("supplier", 10.0);
        byte[] serialized = codec.serialize(originalHandle);

        ByteArrayInputStream byteIn = new ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(byteIn);

        String tableName = in.readUTF();
        double scaleFactor = in.readDouble();

        assertEquals(tableName, "supplier");
        assertEquals(scaleFactor, 10.0);

        ConnectorTableHandle deserialized = codec.deserialize(serialized);
        assertTrue(deserialized instanceof TpchTableHandle);
        TpchTableHandle deserializedTpch = (TpchTableHandle) deserialized;
        assertEquals(deserializedTpch.getTableName(), "supplier");
        assertEquals(deserializedTpch.getScaleFactor(), 10.0);
    }

    @Test
    public void testSplitWithComplexPredicate()
    {
        ConnectorCodec<ConnectorSplit> codec = codecProvider.getConnectorSplitCodec().get();

        TpchTableHandle tableHandle = new TpchTableHandle("customer", 1.0);
        TpchColumnHandle columnHandle = new TpchColumnHandle("c_custkey", BIGINT);

        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 100L, false)),
                false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(columnHandle, domain));

        TpchSplit originalSplit = new TpchSplit(
                tableHandle,
                1,
                2,
                ImmutableList.of(HostAddress.fromParts("localhost", 8080)),
                predicate);

        byte[] serialized = codec.serialize(originalSplit);
        ConnectorSplit deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TpchSplit);
        TpchSplit deserializedTpch = (TpchSplit) deserialized;
        assertEquals(deserializedTpch.getTableHandle().getTableName(), "customer");
        assertNotNull(deserializedTpch.getPredicate());
    }

    @Test
    public void testColumnHandleWithVariousTypes()
    {
        ConnectorCodec<ColumnHandle> codec = codecProvider.getColumnHandleCodec().get();

        Map<String, com.facebook.presto.common.type.Type> types = ImmutableMap.of(
                "bigint_col", BIGINT,
                "integer_col", INTEGER,
                "double_col", DOUBLE,
                "varchar_col", VARCHAR);

        for (Map.Entry<String, com.facebook.presto.common.type.Type> entry : types.entrySet()) {
            TpchColumnHandle originalHandle = new TpchColumnHandle(entry.getKey(), entry.getValue());

            byte[] serialized = codec.serialize(originalHandle);
            ColumnHandle deserialized = codec.deserialize(serialized);

            assertTrue(deserialized instanceof TpchColumnHandle);
            TpchColumnHandle deserializedTpch = (TpchColumnHandle) deserialized;
            assertEquals(deserializedTpch.getColumnName(), originalHandle.getColumnName());
            assertEquals(deserializedTpch.getType(), originalHandle.getType());
        }
    }

    private static class TestTupleDomainSerde
            implements TupleDomainSerde
    {
        private final ObjectMapper objectMapper;

        public TestTupleDomainSerde(ObjectMapper objectMapper)
        {
            this.objectMapper = objectMapper;
        }

        @Override
        public String serialize(TupleDomain<ColumnHandle> tupleDomain)
        {
            try {
                if (tupleDomain.isAll()) {
                    return "{\"all\":true}";
                }
                if (tupleDomain.isNone()) {
                    return "{\"none\":true}";
                }
                return "{\"columnDomains\":[]}";
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to serialize TupleDomain", e);
            }
        }

        @Override
        public TupleDomain<ColumnHandle> deserialize(String serialized)
        {
            try {
                JsonNode node = objectMapper.readTree(serialized);
                if (node.has("all") && node.get("all").asBoolean()) {
                    return TupleDomain.all();
                }
                if (node.has("none") && node.get("none").asBoolean()) {
                    return TupleDomain.none();
                }
                return TupleDomain.all();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to deserialize TupleDomain", e);
            }
        }
    }

    public static class TestingTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            for (Type type : getTypes()) {
                if (signature.getBase().equals(type.getTypeSignature().getBase())) {
                    return type;
                }
            }
            return null;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return getType(new TypeSignature(baseTypeName, typeParameters));
        }

        @Override
        public boolean canCoerce(Type actualType, Type expectedType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of(BOOLEAN, INTEGER, BIGINT, DOUBLE, VARCHAR, VARBINARY, TIMESTAMP, DATE, HYPER_LOG_LOG);
        }

        @Override
        public boolean hasType(TypeSignature signature)
        {
            return getType(signature) != null;
        }
    }
}
