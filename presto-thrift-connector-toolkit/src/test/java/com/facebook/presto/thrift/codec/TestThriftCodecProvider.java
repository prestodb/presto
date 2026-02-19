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
package com.facebook.presto.thrift.codec;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestThriftCodecProvider
{
    private ThriftCodecProvider provider;

    @BeforeMethod
    public void setUp()
    {
        provider = new ThriftCodecProvider.Builder()
                .setThriftCodecManager(new ThriftCodecManager())
                .setConnectorSplitType(TestSplit.class)
                .setConnectorTransactionHandle(TestTransactionHandle.class)
                .setConnectorTableLayoutHandle(TestTableLayoutHandle.class)
                .setConnectorTableHandle(TestTableHandle.class)
                .setConnectorOutputTableHandle(TestOutputTableHandle.class)
                .setConnectorInsertTableHandle(TestInsertTableHandle.class)
                .setConnectorDeleteTableHandle(TestDeleteTableHandle.class)
                .build();
    }

    @Test
    public void testSplitCodecRoundTrip()
    {
        Optional<ConnectorCodec<ConnectorSplit>> codecOptional = provider.getConnectorSplitCodec();
        assertTrue(codecOptional.isPresent());

        ConnectorCodec<ConnectorSplit> codec = codecOptional.get();
        TestSplit original = new TestSplit("test-id", 100);
        byte[] serialized = codec.serialize(original);
        ConnectorSplit deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TestSplit);
        TestSplit deserializedSplit = (TestSplit) deserialized;
        assertEquals(deserializedSplit.getSplitId(), original.getSplitId());
        assertEquals(deserializedSplit.getSplitSize(), original.getSplitSize());
    }

    @Test
    public void testTableHandleCodecRoundTrip()
    {
        Optional<ConnectorCodec<ConnectorTableHandle>> codecOptional = provider.getConnectorTableHandleCodec();
        assertTrue(codecOptional.isPresent());

        ConnectorCodec<ConnectorTableHandle> codec = codecOptional.get();
        TestTableHandle original = new TestTableHandle("test-schema", "test-table");
        byte[] serialized = codec.serialize(original);
        ConnectorTableHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TestTableHandle);
        TestTableHandle deserializedHandle = (TestTableHandle) deserialized;
        assertEquals(deserializedHandle.getSchemaName(), original.getSchemaName());
        assertEquals(deserializedHandle.getTableName(), original.getTableName());
    }

    @Test
    public void testTransactionHandleCodecRoundTrip()
    {
        Optional<ConnectorCodec<ConnectorTransactionHandle>> codecOptional = provider.getConnectorTransactionHandleCodec();
        assertTrue(codecOptional.isPresent());

        ConnectorCodec<ConnectorTransactionHandle> codec = codecOptional.get();
        TestTransactionHandle original = new TestTransactionHandle("test");
        byte[] serialized = codec.serialize(original);
        ConnectorTransactionHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TestTransactionHandle);
        TestTransactionHandle deserializedHandle = (TestTransactionHandle) deserialized;
        assertEquals(deserializedHandle.getTransactionId(), original.getTransactionId());
    }

    @Test
    public void testTableLayoutHandleCodecRoundTrip()
    {
        Optional<ConnectorCodec<ConnectorTableLayoutHandle>> codecOptional = provider.getConnectorTableLayoutHandleCodec();
        assertTrue(codecOptional.isPresent());

        ConnectorCodec<ConnectorTableLayoutHandle> codec = codecOptional.get();
        TestTableLayoutHandle original = new TestTableLayoutHandle("test");
        byte[] serialized = codec.serialize(original);
        ConnectorTableLayoutHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TestTableLayoutHandle);
        TestTableLayoutHandle deserializedHandle = (TestTableLayoutHandle) deserialized;
        assertEquals(deserializedHandle.getLayoutId(), original.getLayoutId());
    }

    @Test
    public void testOutputTableHandleCodecRoundTrip()
    {
        Optional<ConnectorCodec<ConnectorOutputTableHandle>> codecOptional = provider.getConnectorOutputTableHandleCodec();
        assertTrue(codecOptional.isPresent());

        ConnectorCodec<ConnectorOutputTableHandle> codec = codecOptional.get();
        TestOutputTableHandle original = new TestOutputTableHandle("test");
        byte[] serialized = codec.serialize(original);
        ConnectorOutputTableHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TestOutputTableHandle);
        TestOutputTableHandle deserializedHandle = (TestOutputTableHandle) deserialized;
        assertEquals(deserializedHandle.getOutputId(), original.getOutputId());
    }

    @Test
    public void testInsertTableHandleCodecRoundTrip()
    {
        Optional<ConnectorCodec<ConnectorInsertTableHandle>> codecOptional = provider.getConnectorInsertTableHandleCodec();
        assertTrue(codecOptional.isPresent());

        ConnectorCodec<ConnectorInsertTableHandle> codec = codecOptional.get();
        TestInsertTableHandle original = new TestInsertTableHandle("test");
        byte[] serialized = codec.serialize(original);
        ConnectorInsertTableHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TestInsertTableHandle);
        TestInsertTableHandle deserializedHandle = (TestInsertTableHandle) deserialized;
        assertEquals(deserializedHandle.getInsertId(), original.getInsertId());
    }

    @Test
    public void testDeleteTableHandleCodecRoundTrip()
    {
        Optional<ConnectorCodec<ConnectorDeleteTableHandle>> codecOptional = provider.getConnectorDeleteTableHandleCodec();
        assertTrue(codecOptional.isPresent());

        ConnectorCodec<ConnectorDeleteTableHandle> codec = codecOptional.get();
        TestDeleteTableHandle original = new TestDeleteTableHandle("test");
        byte[] serialized = codec.serialize(original);
        ConnectorDeleteTableHandle deserialized = codec.deserialize(serialized);

        assertTrue(deserialized instanceof TestDeleteTableHandle);
        TestDeleteTableHandle deserializedHandle = (TestDeleteTableHandle) deserialized;
        assertEquals(deserializedHandle.getDeleteId(), original.getDeleteId());
    }

    @ThriftStruct
    public static class TestSplit
            implements ConnectorSplit
    {
        private final String splitId;
        private final long splitSize;

        @ThriftConstructor
        public TestSplit(String splitId, long splitSize)
        {
            this.splitId = splitId;
            this.splitSize = splitSize;
        }

        @ThriftField(1)
        public String getSplitId()
        {
            return splitId;
        }

        @ThriftField(2)
        public long getSplitSize()
        {
            return splitSize;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return null;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return this;
        }
    }

    @ThriftStruct
    public static class TestTransactionHandle
            implements ConnectorTransactionHandle
    {
        private final String transactionId;

        @ThriftConstructor
        public TestTransactionHandle(String transactionId)
        {
            this.transactionId = transactionId;
        }

        @ThriftField(1)
        public String getTransactionId()
        {
            return transactionId;
        }
    }

    @ThriftStruct
    public static class TestTableLayoutHandle
            implements ConnectorTableLayoutHandle
    {
        private final String layoutId;

        @ThriftConstructor
        public TestTableLayoutHandle(String layoutId)
        {
            this.layoutId = layoutId;
        }

        @ThriftField(1)
        public String getLayoutId()
        {
            return layoutId;
        }
    }

    @ThriftStruct
    public static class TestTableHandle
            implements ConnectorTableHandle
    {
        private final String schemaName;
        private final String tableName;

        @ThriftConstructor
        public TestTableHandle(String schemaName, String tableName)
        {
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        @ThriftField(1)
        public String getSchemaName()
        {
            return schemaName;
        }

        @ThriftField(2)
        public String getTableName()
        {
            return tableName;
        }
    }

    @ThriftStruct
    public static class TestOutputTableHandle
            implements ConnectorOutputTableHandle
    {
        private final String outputId;

        @ThriftConstructor
        public TestOutputTableHandle(String outputId)
        {
            this.outputId = outputId;
        }

        @ThriftField(1)
        public String getOutputId()
        {
            return outputId;
        }
    }

    @ThriftStruct
    public static class TestInsertTableHandle
            implements ConnectorInsertTableHandle
    {
        private final String insertId;

        @ThriftConstructor
        public TestInsertTableHandle(String insertId)
        {
            this.insertId = insertId;
        }

        @ThriftField(1)
        public String getInsertId()
        {
            return insertId;
        }
    }

    @ThriftStruct
    public static class TestDeleteTableHandle
            implements ConnectorDeleteTableHandle
    {
        private final String deleteId;

        @ThriftConstructor
        public TestDeleteTableHandle(String deleteId)
        {
            this.deleteId = deleteId;
        }

        @ThriftField(1)
        public String getDeleteId()
        {
            return deleteId;
        }
    }
}
