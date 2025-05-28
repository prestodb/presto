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
import com.facebook.presto.connector.ConnectorThriftCodecManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.ConnectorSplit;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ConnectorSplitThriftCodec
        extends AbstractTypedThriftCodec<ConnectorSplit>
{
    private final ConnectorThriftCodecManager connectorThriftCodecManager;

    @Inject
    public ConnectorSplitThriftCodec(HandleResolver handleResolver, ConnectorThriftCodecManager connectorThriftCodecManager, JsonCodec<ConnectorSplit> jsonCodec)
    {
        super(ConnectorSplit.class,
                requireNonNull(jsonCodec, "jsonCodec is null"),
                requireNonNull(handleResolver, "handleResolver is null")::getId,
                handleResolver::getSplitClass,
                requireNonNull(connectorThriftCodecManager, "connectorThriftCodecManager is null"));
        this.connectorThriftCodecManager = connectorThriftCodecManager;
    }

    @CodecThriftType
    public static ThriftType getThriftType()
    {
        return createThriftType(ConnectorSplit.class, true);
    }

    @Override
    public ThriftType getType()
    {
        return createThriftType(ConnectorSplit.class, false);
    }

    @Override
    public ConnectorSplit readConcreteValue(String connectorId, TProtocolReader reader)
    {
        return connectorThriftCodecManager.getConnectorSplitThriftCodec(connectorId).deserialize(reader);
    }

    @Override
    public void writeConcreteValue(String connectorId, ConnectorSplit value, TProtocolWriter writer)
    {
        connectorThriftCodecManager.getConnectorSplitThriftCodec(connectorId).serialize(value, writer);
    }
}
