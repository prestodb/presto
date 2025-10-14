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
package com.facebook.presto.server.thrift;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.common.thrift.ByteBufferPoolManager;
import com.facebook.presto.connector.ConnectorCodecManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.server.thrift.ThriftCodecUtils.deserialize;
import static com.facebook.presto.server.thrift.ThriftCodecUtils.serialize;
import static java.util.Objects.requireNonNull;

public class DeleteTableHandleThriftCodec
        extends AbstractTypedThriftCodec<ConnectorDeleteTableHandle>
{
    private static final ThriftType THRIFT_TYPE = createThriftType(ConnectorDeleteTableHandle.class);
    private final ConnectorCodecManager connectorCodecManager;
    private final ByteBufferPoolManager byteBufferPoolManager;

    @Inject
    public DeleteTableHandleThriftCodec(HandleResolver handleResolver,
            ConnectorCodecManager connectorCodecManager,
            JsonCodec<ConnectorDeleteTableHandle> jsonCodec,
            ByteBufferPoolManager byteBufferPoolManager)
    {
        super(ConnectorDeleteTableHandle.class,
                requireNonNull(jsonCodec, "jsonCodec is null"),
                requireNonNull(handleResolver, "handleResolver is null")::getId,
                handleResolver::getDeleteTableHandleClass);
        this.connectorCodecManager = requireNonNull(connectorCodecManager, "connectorThriftCodecManager is null");
        this.byteBufferPoolManager = requireNonNull(byteBufferPoolManager, "byteBufferPoolManager is null");
    }

    @CodecThriftType
    public static ThriftType getThriftType()
    {
        return THRIFT_TYPE;
    }

    @Override
    public ThriftType getType()
    {
        return THRIFT_TYPE;
    }

    @Override
    public ConnectorDeleteTableHandle readConcreteValue(String connectorId, TProtocolReader reader)
            throws Exception
    {
        Optional<ConnectorCodec<ConnectorDeleteTableHandle>> codec = connectorCodecManager.getDeleteTableHandleCodec(connectorId);
        if (!codec.isPresent()) {
            return null;
        }
        return deserialize(codec.get(), reader, byteBufferPoolManager);
    }

    @Override
    public void writeConcreteValue(String connectorId, ConnectorDeleteTableHandle value, TProtocolWriter writer)
            throws Exception
    {
        requireNonNull(value, "value is null");
        Optional<ConnectorCodec<ConnectorDeleteTableHandle>> codec = connectorCodecManager.getDeleteTableHandleCodec(connectorId);
        if (!codec.isPresent()) {
            return;
        }

        serialize(codec.get(), value, writer, byteBufferPoolManager);
    }

    @Override
    public boolean isThriftCodecAvailable(String connectorId)
    {
        return connectorCodecManager.getDeleteTableHandleCodec(connectorId).isPresent();
    }
}
