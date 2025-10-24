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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomainSerde;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public class TpchConnectorCodecProvider
        implements ConnectorCodecProvider
{
    private final TypeManager typeManager;
    private final TupleDomainSerde tupleDomainSerde;

    public TpchConnectorCodecProvider(TypeManager typeManager, TupleDomainSerde tupleDomainSerde)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tupleDomainSerde = requireNonNull(tupleDomainSerde, "tupleDomainSerde is null");
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
    {
        return Optional.of(new TpchTableHandleCodec());
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
    {
        return Optional.of(new TpchTableLayoutHandleCodec(tupleDomainSerde));
    }

    @Override
    public Optional<ConnectorCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.of(new TpchColumnHandleCodec(typeManager));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return Optional.of(new TpchSplitCodec(tupleDomainSerde));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorOutputTableHandle>> getConnectorOutputTableHandleCodec()
    {
        // TPC-H doesn't support writes
        return Optional.empty();
    }

    @Override
    public Optional<ConnectorCodec<ConnectorInsertTableHandle>> getConnectorInsertTableHandleCodec()
    {
        // TPC-H doesn't support writes
        return Optional.empty();
    }

    @Override
    public Optional<ConnectorCodec<ConnectorPartitioningHandle>> getConnectorPartitioningHandleCodec()
    {
        return Optional.of(new TpchPartitioningHandleCodec());
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return Optional.of(new TpchTransactionHandleCodec());
    }

    private static class TpchTableHandleCodec
            implements ConnectorCodec<ConnectorTableHandle>
    {
        @Override
        public byte[] serialize(ConnectorTableHandle handle)
        {
            try {
                TpchTableHandle tableHandle = (TpchTableHandle) handle;
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut);
                out.writeUTF(tableHandle.getTableName());
                out.writeDouble(tableHandle.getScaleFactor());
                return byteOut.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to serialize TpchTableHandle", e);
            }
        }

        @Override
        public ConnectorTableHandle deserialize(byte[] bytes)
        {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn);
                String tableName = in.readUTF();
                double scaleFactor = in.readDouble();
                return new TpchTableHandle(tableName, scaleFactor);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize TpchTableHandle", e);
            }
        }
    }

    private static class TpchTableLayoutHandleCodec
            implements ConnectorCodec<ConnectorTableLayoutHandle>
    {
        private final TupleDomainSerde tupleDomainSerde;

        public TpchTableLayoutHandleCodec(TupleDomainSerde tupleDomainSerde)
        {
            this.tupleDomainSerde = requireNonNull(tupleDomainSerde, "tupleDomainSerde is null");
        }

        @Override
        public byte[] serialize(ConnectorTableLayoutHandle handle)
        {
            try {
                TpchTableLayoutHandle layoutHandle = (TpchTableLayoutHandle) handle;
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut);
                // Serialize the table handle
                out.writeUTF(layoutHandle.getTable().getTableName());
                out.writeDouble(layoutHandle.getTable().getScaleFactor());

                // Serialize the predicate using JSON
                TupleDomain<ColumnHandle> predicate = layoutHandle.getPredicate();
                String predicateJson = tupleDomainSerde.serialize(predicate);
                out.writeUTF(predicateJson);

                return byteOut.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to serialize TpchTableLayoutHandle", e);
            }
        }

        @Override
        public ConnectorTableLayoutHandle deserialize(byte[] bytes)
        {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn);
                String tableName = in.readUTF();
                double scaleFactor = in.readDouble();
                TpchTableHandle table = new TpchTableHandle(tableName, scaleFactor);

                String predicateJson = in.readUTF();
                TupleDomain<ColumnHandle> predicate = tupleDomainSerde.deserialize(predicateJson);

                return new TpchTableLayoutHandle(table, predicate);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize TpchTableLayoutHandle", e);
            }
        }
    }

    private static class TpchColumnHandleCodec
            implements ConnectorCodec<ColumnHandle>
    {
        private final TypeManager typeManager;

        public TpchColumnHandleCodec(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public byte[] serialize(ColumnHandle handle)
        {
            try {
                TpchColumnHandle columnHandle = (TpchColumnHandle) handle;
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut);
                out.writeUTF(columnHandle.getColumnName());
                out.writeUTF(columnHandle.getType().getTypeSignature().toString());
                // Serialize required subfields
                List<Subfield> subfields = columnHandle.getRequiredSubfields();
                out.writeInt(subfields.size());
                for (Subfield subfield : subfields) {
                    out.writeUTF(subfield.serialize());
                }
                return byteOut.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to serialize TpchColumnHandle", e);
            }
        }

        @Override
        public ColumnHandle deserialize(byte[] bytes)
        {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn);
                String columnName = in.readUTF();
                String typeSignature = in.readUTF();
                Type type = typeManager.getType(parseTypeSignature(typeSignature));
                int subfieldCount = in.readInt();
                List<Subfield> subfields = new ArrayList<>(subfieldCount);
                for (int i = 0; i < subfieldCount; i++) {
                    subfields.add(new Subfield(in.readUTF()));
                }
                return new TpchColumnHandle(columnName, type, subfields);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize TpchColumnHandle", e);
            }
        }
    }

    private static class TpchSplitCodec
            implements ConnectorCodec<ConnectorSplit>
    {
        private final TupleDomainSerde tupleDomainSerde;

        public TpchSplitCodec(TupleDomainSerde tupleDomainSerde)
        {
            this.tupleDomainSerde = requireNonNull(tupleDomainSerde, "tupleDomainSerde is null");
        }

        @Override
        public byte[] serialize(ConnectorSplit split)
        {
            try {
                TpchSplit tpchSplit = (TpchSplit) split;
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut);
                // Serialize table handle
                out.writeUTF(tpchSplit.getTableHandle().getTableName());
                out.writeDouble(tpchSplit.getTableHandle().getScaleFactor());
                // Serialize split info
                out.writeInt(tpchSplit.getPartNumber());
                out.writeInt(tpchSplit.getTotalParts());
                // Serialize addresses
                List<HostAddress> addresses = tpchSplit.getAddresses();
                out.writeInt(addresses.size());
                for (HostAddress address : addresses) {
                    out.writeUTF(address.getHostText());
                    out.writeInt(address.getPort());
                }
                // Serialize the predicate using JSON
                TupleDomain<ColumnHandle> predicate = tpchSplit.getPredicate();
                String predicateJson = tupleDomainSerde.serialize(predicate);
                out.writeUTF(predicateJson);
                return byteOut.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to serialize TpchSplit", e);
            }
        }

        @Override
        public ConnectorSplit deserialize(byte[] bytes)
        {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn);
                // Deserialize table handle
                String tableName = in.readUTF();
                double scaleFactor = in.readDouble();
                TpchTableHandle tableHandle = new TpchTableHandle(tableName, scaleFactor);
                // Deserialize split info
                int partNumber = in.readInt();
                int totalParts = in.readInt();
                // Deserialize addresses
                int addressCount = in.readInt();
                ImmutableList.Builder<HostAddress> addresses = ImmutableList.builder();
                for (int i = 0; i < addressCount; i++) {
                    String host = in.readUTF();
                    int port = in.readInt();
                    addresses.add(HostAddress.fromParts(host, port));
                }
                // Deserialize the predicate
                String predicateJson = in.readUTF();
                TupleDomain<ColumnHandle> predicate = tupleDomainSerde.deserialize(predicateJson);
                return new TpchSplit(tableHandle, partNumber, totalParts, addresses.build(), predicate);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize TpchSplit", e);
            }
        }
    }

    private static class TpchPartitioningHandleCodec
            implements ConnectorCodec<ConnectorPartitioningHandle>
    {
        @Override
        public byte[] serialize(ConnectorPartitioningHandle handle)
        {
            try {
                TpchPartitioningHandle partitioningHandle = (TpchPartitioningHandle) handle;
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut);
                out.writeUTF(partitioningHandle.getTable());
                out.writeLong(partitioningHandle.getTotalRows());
                return byteOut.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to serialize TpchPartitioningHandle", e);
            }
        }

        @Override
        public ConnectorPartitioningHandle deserialize(byte[] bytes)
        {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn);
                String tableName = in.readUTF();
                long totalRows = in.readLong();
                return new TpchPartitioningHandle(tableName, totalRows);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize TpchPartitioningHandle", e);
            }
        }
    }

    private static class TpchTransactionHandleCodec
            implements ConnectorCodec<ConnectorTransactionHandle>
    {
        @Override
        public byte[] serialize(ConnectorTransactionHandle handle)
        {
            // TpchTransactionHandle is a singleton with no data
            // Return empty byte array
            return new byte[0];
        }

        @Override
        public ConnectorTransactionHandle deserialize(byte[] bytes)
        {
            // Return the singleton instance
            return TpchTransactionHandle.INSTANCE;
        }
    }
}
