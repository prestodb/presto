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
import com.facebook.drift.TException;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.DefaultThriftTypeReference;
import com.facebook.drift.codec.metadata.FieldKind;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftFieldMetadata;
import com.facebook.drift.codec.metadata.ThriftMethodInjection;
import com.facebook.drift.codec.metadata.ThriftStructMetadata;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TField;
import com.facebook.drift.protocol.TProtocolException;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.TStruct;
import com.facebook.drift.protocol.TType;
import com.facebook.presto.connector.ConnectorSpecificCodecManager;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSpecificCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.drift.annotations.ThriftField.Requiredness.NONE;
import static com.facebook.presto.server.thrift.CustomCodecUtils.writeSingleJsonField;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SplitThriftCodec
        implements ThriftCodec<Split>
{
    private static final short SPLIT_DATA_FIELD_ID = 111;
    private static final String SPLIT_DATA_FIELD_NAME = "split";
    private static final String SPLIT_DATA_STRUCT_NAME = "Split";

    private final ThriftCatalog thriftCatalog;
    private final JsonCodec<Split> jsonCodec;
    private final ConnectorSpecificCodecManager connectorSpecificCodecManager;

    @Inject
    public SplitThriftCodec(ThriftCatalog thriftCatalog, JsonCodec<Split> jsonCodec, ConnectorSpecificCodecManager connectorSpecificCodecManager)
    {
        this.thriftCatalog = requireNonNull(thriftCatalog, "thriftCatalog is null");
        this.thriftCatalog.addThriftType(createSplitMetadata(this.thriftCatalog));
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.connectorSpecificCodecManager = requireNonNull(connectorSpecificCodecManager, "connectorSpecificCodecManager is null");
    }

    private static ThriftType createSplitMetadata(ThriftCatalog catalog)
    {
        List<ThriftFieldMetadata> fieldMetadata = new ArrayList<>();

        try {
            fieldMetadata.add(new ThriftFieldMetadata(
                    (short) 1,
                    false, false, NONE, ImmutableMap.of(),
                    new DefaultThriftTypeReference(catalog.getThriftType(ConnectorId.class)),
                    "connectorId",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(Split.class.getMethod("getConnectorId"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));

            fieldMetadata.add(new ThriftFieldMetadata(
                    (short) 2,
                    false, false, NONE, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.BINARY),
                    "transactionHandle",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(Split.class.getMethod("getTransactionHandle"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));

            fieldMetadata.add(new ThriftFieldMetadata(
                    (short) 3,
                    false, false, NONE, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.BINARY),
                    "connectorSplit",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(Split.class.getMethod("getConnectorSplit"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));

            fieldMetadata.add(new ThriftFieldMetadata(
                    (short) 4,
                    false, false, NONE, ImmutableMap.of(),
                    new DefaultThriftTypeReference(catalog.getThriftType(Lifespan.class)),
                    "lifespan",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(Split.class.getMethod("getLifespan"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));

            fieldMetadata.add(new ThriftFieldMetadata(
                    (short) 5,
                    false, false, NONE, ImmutableMap.of(),
                    new DefaultThriftTypeReference(catalog.getThriftType(SplitContext.class)),
                    "splitContext",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(Split.class.getMethod("getSplitContext"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
        }
        catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Can not find methodInjection.", e);
        }

        return ThriftType.struct(new ThriftStructMetadata(
                "Split",
                ImmutableMap.of(),
                Split.class, null,
                ThriftStructMetadata.MetadataType.STRUCT,
                Optional.empty(), ImmutableList.of(), fieldMetadata, Optional.empty(), ImmutableList.of()));
    }

    @CodecThriftType
    public static ThriftType getThriftType(ThriftCatalog catalog)
    {
        return createSplitMetadata(catalog);
    }

    @Override
    public ThriftType getType()
    {
        return createSplitMetadata(this.thriftCatalog);
    }

    @Override
    public Split read(TProtocolReader protocol)
            throws Exception
    {
        String jsonSplit = null;
        ConnectorId connectorId = null;
        ConnectorTransactionHandle transactionHandle = null;
        ConnectorSplit connectorSplit = null;
        Lifespan lifespan = null;
        SplitContext splitContext = null;

        protocol.readStructBegin();
        while (true) {
            TField field = protocol.readFieldBegin();
            if (field.getType() == TType.STOP || jsonSplit != null) {
                break;
            }
            switch (field.getId()) {
                case SPLIT_DATA_FIELD_ID:
                    if (field.getType() == TType.STRING) {
                        jsonSplit = protocol.readString();
                    }
                    else {
                        throw new TProtocolException(format("Unexpected field type: %s for field %s", field.getType(), field.getName()));
                    }
                    break;
                case 1:
                    connectorId = new ConnectorId(protocol.readString());
                    break;
                case 2:
                    ConnectorSpecificCodec<ConnectorTransactionHandle> transactionHandleCodec = connectorSpecificCodecManager.getConnectorTransactionHandleCodec(connectorId);
                    transactionHandle = transactionHandleCodec.deserialize(protocol.readBinary().array());
                    break;
                case 3:
                    ConnectorSpecificCodec<ConnectorSplit> splitCodec = connectorSpecificCodecManager.getConnectorSplitCodec(connectorId);
                    connectorSplit = splitCodec.deserialize(protocol.readBinary().array());
                    break;
                case 4:
                    lifespan = readLifespan(protocol);
                    break;
                case 5:
                    splitContext = readSplitContext(protocol);
                    break;
                default:
                    throw new TProtocolException(format("Unexpected field id: %s", field.getId()));
            }
            protocol.readFieldEnd();
        }
        protocol.readStructEnd();

        if (jsonSplit != null) {
            return jsonCodec.fromJson(jsonSplit);
        }

        return new Split(connectorId, transactionHandle, connectorSplit, lifespan, splitContext);
    }

    private SplitContext readSplitContext(TProtocolReader protocol)
    {
        try {
            protocol.readStructBegin();
            boolean cacheable = false;

            while (true) {
                TField field = protocol.readFieldBegin();
                if (field.getType() == TType.STOP) {
                    break;
                }

                switch (field.getId()) {
                    case 1:
                        cacheable = protocol.readBool();
                        break;
                    default:
                        throw new TProtocolException(format("Unexpected field id: %s", field.getId()));
                }
                protocol.readFieldEnd();
            }
            protocol.readStructEnd();
            return new SplitContext(cacheable);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    private Lifespan readLifespan(TProtocolReader protocol)
    {
        try {
            protocol.readStructBegin();

            boolean grouped = false;
            int groupId = 0;

            while (true) {
                TField field = protocol.readFieldBegin();
                if (field.getType() == TType.STOP) {
                    break;
                }
                switch (field.getId()) {
                    case 1:
                        grouped = protocol.readBool();
                        break;
                    case 2:
                        groupId = protocol.readI32();
                        break;
                    default:
                        throw new TProtocolException(format("Unexpected field id: %s", field.getId()));
                }
                protocol.readFieldEnd();
            }
            protocol.readStructEnd();
            return new Lifespan(grouped, groupId);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Split split, TProtocolWriter protocol)
            throws Exception
    {
        if (connectorSpecificCodecManager.isConnectorSpecificCodecAvailable(split.getConnectorId())) {
            protocol.writeStructBegin(new TStruct("Split"));

            protocol.writeFieldBegin(new TField("connectorId", TType.STRING, (short) 1));
            protocol.writeString(split.getConnectorId().getCatalogName());
            protocol.writeFieldEnd();

            ConnectorSpecificCodec<ConnectorTransactionHandle> transactionHandleCodec = connectorSpecificCodecManager.getConnectorTransactionHandleCodec(split.getConnectorId());
            protocol.writeFieldBegin(new TField("transactionHandle", TType.STRING, (short) 2));
            protocol.writeBinary(ByteBuffer.wrap(transactionHandleCodec.serialize(split.getTransactionHandle())));
            protocol.writeFieldEnd();

            ConnectorSpecificCodec<ConnectorSplit> connectorSplitCodec = connectorSpecificCodecManager.getConnectorSplitCodec(split.getConnectorId());
            protocol.writeFieldBegin(new TField("connectorSplit", TType.STRING, (short) 3));
            protocol.writeBinary(ByteBuffer.wrap(connectorSplitCodec.serialize(split.getConnectorSplit())));
            protocol.writeFieldEnd();

            protocol.writeFieldBegin(new TField("lifespan", TType.STRUCT, (short) 4));
            protocol.writeStructBegin(new TStruct("Lifespan"));
            protocol.writeFieldBegin(new TField("grouped", TType.BOOL, (short) 1));
            protocol.writeBool(split.getLifespan().isGrouped());
            protocol.writeFieldEnd();
            protocol.writeFieldBegin(new TField("groupId", TType.I32, (short) 2));
            protocol.writeI32(split.getLifespan().getId());
            protocol.writeFieldEnd();
            protocol.writeFieldStop();
            protocol.writeStructEnd();
            protocol.writeFieldEnd();

            protocol.writeFieldBegin(new TField("splitContext", TType.STRUCT, (short) 5));
            protocol.writeStructBegin(new TStruct("SplitContext"));
            protocol.writeFieldBegin(new TField("cacheable", TType.BOOL, (short) 1));
            protocol.writeBool(split.getSplitContext().isCacheable());
            protocol.writeFieldEnd();
            protocol.writeFieldStop();
            protocol.writeStructEnd();
            protocol.writeFieldEnd();

            protocol.writeFieldStop();
            protocol.writeStructEnd();
            return;
        }

        // For internal connectors, we still use json blob
        writeSingleJsonField(split, protocol, jsonCodec, SPLIT_DATA_FIELD_ID, SPLIT_DATA_FIELD_NAME, SPLIT_DATA_STRUCT_NAME);
    }
}
