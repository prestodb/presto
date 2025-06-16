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
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TField;
import com.facebook.drift.protocol.TProtocolException;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.TStruct;
import com.facebook.drift.protocol.TType;
import com.facebook.presto.connector.ConnectorSpecificCodecManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;

import javax.inject.Inject;

import java.nio.ByteBuffer;

import static com.facebook.presto.server.thrift.CustomCodecUtils.createConnectorSpecificThriftType;
import static com.facebook.presto.server.thrift.CustomCodecUtils.writeSingleJsonField;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConnectorSplitThriftCodec
        implements ThriftCodec<Split.ConnectorSplitWrapper>
{
    private static final short JSON_DATA_FIELD_ID = 111;
    private static final String JSON_DATA_FIELD_NAME = "connectorSplitWrapper";
    private static final String JSON_DATA_STRUCT_NAME = "ConnectorSplitWrapper";
    private static final ThriftType THRIFT_TYPE = createConnectorSpecificThriftType(Split.ConnectorSplitWrapper.class, "connectorSplit", "getConnectorSplit");
    private final ConnectorSpecificCodecManager connectorSpecificCodecManager;
    private JsonCodec<Split.ConnectorSplitWrapper> jsonCodec;

    @Inject
    public ConnectorSplitThriftCodec(JsonCodec<Split.ConnectorSplitWrapper> jsonCodec, ConnectorSpecificCodecManager connectorSpecificCodecManager, ThriftCatalog thriftCatalog)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.connectorSpecificCodecManager = requireNonNull(connectorSpecificCodecManager, "connectorSpecificCodecManager is null");
        thriftCatalog.addThriftType(getThriftType());
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
    public Split.ConnectorSplitWrapper read(TProtocolReader protocol)
            throws Exception
    {
        String jsonWrapper = null;
        ConnectorId connectorId = null;
        byte[] bytes = null;

        protocol.readStructBegin();
        while (true) {
            TField field = protocol.readFieldBegin();
            if (field.getType() == TType.STOP || jsonWrapper != null) {
                break;
            }
            switch (field.getId()) {
                case JSON_DATA_FIELD_ID:
                    if (field.getType() == TType.STRING) {
                        jsonWrapper = protocol.readString();
                    }
                    else {
                        throw new TProtocolException(format("Unexpected field type: %s for field %s", field.getType(), field.getName()));
                    }
                    break;
                case 1:
                    connectorId = new ConnectorId(protocol.readString());
                    break;
                case 2:
                    bytes = protocol.readBinary().array();
                    break;
                default:
                    throw new TProtocolException(format("Unexpected field id: %s", field.getId()));
            }
        }
        protocol.readStructEnd();

        if (jsonWrapper != null) {
            return jsonCodec.fromJson(jsonWrapper);
        }
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(bytes, "bytes is null");

        return new Split.ConnectorSplitWrapper(connectorId, connectorSpecificCodecManager.getConnectorSplitCodec(connectorId).deserialize(bytes));
    }

    @Override
    public void write(Split.ConnectorSplitWrapper value, TProtocolWriter protocol)
            throws Exception
    {
        if (connectorSpecificCodecManager.isConnectorSpecificCodecAvailable(value.getConnectorId())) {
            protocol.writeStructBegin(new TStruct("ConnectorSplitWrapper"));

            protocol.writeFieldBegin(new TField("connectorId", TType.STRING, (short) 1));
            protocol.writeString(value.getConnectorId().getCatalogName());
            protocol.writeFieldEnd();

            protocol.writeFieldBegin(new TField("connectorSplit", TType.STRING, (short) 2));
            protocol.writeBinary(ByteBuffer.wrap(connectorSpecificCodecManager.getConnectorSplitCodec(value.getConnectorId()).serialize(value.getConnectorSplit())));
            protocol.writeFieldEnd();

            protocol.writeFieldStop();
            protocol.writeStructEnd();
            return;
        }
        // For non-enabled connectors, we still use json blob
        writeSingleJsonField(value, protocol, jsonCodec, JSON_DATA_FIELD_ID, JSON_DATA_FIELD_NAME, JSON_DATA_STRUCT_NAME);
    }
}
