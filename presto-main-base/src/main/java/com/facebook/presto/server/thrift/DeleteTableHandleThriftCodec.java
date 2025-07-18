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
import com.facebook.presto.connector.ConnectorCodecManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;

import javax.inject.Inject;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class DeleteTableHandleThriftCodec
        extends AbstractTypedThriftCodec<ConnectorDeleteTableHandle>
{
    private static final ThriftType THRIFT_TYPE = createThriftType(ConnectorDeleteTableHandle.class);
    private final ConnectorCodecManager connectorCodecManager;

    @Inject
    public DeleteTableHandleThriftCodec(HandleResolver handleResolver, ConnectorCodecManager connectorCodecManager, JsonCodec<ConnectorDeleteTableHandle> jsonCodec)
    {
        super(ConnectorDeleteTableHandle.class,
                requireNonNull(jsonCodec, "jsonCodec is null"),
                requireNonNull(handleResolver, "handleResolver is null")::getId,
                handleResolver::getDeleteTableHandleClass);
        this.connectorCodecManager = requireNonNull(connectorCodecManager, "connectorThriftCodecManager is null");
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
        ByteBuffer byteBuffer = reader.readBinary();
        assert (byteBuffer.position() == 0);
        byte[] bytes = byteBuffer.array();
        return connectorCodecManager.getDeleteTableHandleCodec(connectorId).map(codec -> codec.deserialize(bytes)).orElse(null);
    }

    @Override
    public void writeConcreteValue(String connectorId, ConnectorDeleteTableHandle value, TProtocolWriter writer)
            throws Exception
    {
        requireNonNull(value, "value is null");
        writer.writeBinary(ByteBuffer.wrap(connectorCodecManager.getDeleteTableHandleCodec(connectorId).map(codec -> codec.serialize(value)).orElseThrow(() -> new IllegalArgumentException("Can not serialize " + value))));
    }

    @Override
    public boolean isThriftCodecAvailable(String connectorId)
    {
        return connectorCodecManager.getDeleteTableHandleCodec(connectorId).isPresent();
    }
}
