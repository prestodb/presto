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
import com.facebook.drift.annotations.ThriftField.Requiredness;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.DefaultThriftTypeReference;
import com.facebook.drift.codec.metadata.FieldKind;
import com.facebook.drift.codec.metadata.ThriftFieldMetadata;
import com.facebook.drift.codec.metadata.ThriftMethodInjection;
import com.facebook.drift.codec.metadata.ThriftStructMetadata;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TField;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.TStruct;
import com.facebook.drift.protocol.TType;
import com.facebook.presto.connector.ConnectorThriftCodecManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.server.thrift.ThriftCodecUtils.writeSingleJsonField;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTypedThriftCodec<T>
        implements ThriftCodec<T>
{
    private static final String TYPE_PROPERTY = "type";
    private static final String THRIFT_VALUE_PROPERTY = "value";
    private static final String JSON_FIELD_NAME = "json";
    private static final String JSON_STRUCT_NAME = "JSON";
    private static final short TYPE_FIELD_ID = 1;
    private static final short THRIFT_FIELD_ID = 2;
    private static final short JSON_FIELD_ID = 111;

    private final Class<T> baseClass;
    private final JsonCodec<T> jsonCodec;
    private final Function<T, String> nameResolver;
    private final Function<String, Class<? extends T>> classResolver;
    private final ConnectorThriftCodecManager connectorThriftCodecManager;

    protected AbstractTypedThriftCodec(Class<T> baseClass,
            JsonCodec<T> jsonCodec,
            Function<T, String> nameResolver,
            Function<String, Class<? extends T>> classResolver,
            ConnectorThriftCodecManager connectorThriftCodecManager)
    {
        this.baseClass = requireNonNull(baseClass, "baseClass is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.nameResolver = requireNonNull(nameResolver, "nameResolver is null");
        this.classResolver = requireNonNull(classResolver, "classResolver is null");
        this.connectorThriftCodecManager = requireNonNull(connectorThriftCodecManager, "connectorThriftCodecManager is null");
    }

    @Override
    public abstract ThriftType getType();

    protected static ThriftType createThriftType(Class<?> baseClass, boolean forIDL)
    {
        List<ThriftFieldMetadata> fields = new ArrayList<>();
        try {
            fields.add(new ThriftFieldMetadata(
                    TYPE_FIELD_ID,
                    false, false, Requiredness.NONE, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.STRING),
                    TYPE_PROPERTY,
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(AbstractTypedThriftCodec.class.getDeclaredMethod("getTypeField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
            fields.add(new ThriftFieldMetadata(
                    THRIFT_FIELD_ID,
                    false, false, Requiredness.NONE, ImmutableMap.of(),
                    forIDL ? new DefaultThriftTypeReference(ThriftType.BINARY) :
                            new DefaultThriftTypeReference(ThriftType.struct(new ThriftStructMetadata(
                                    baseClass.getSimpleName(),
                                    ImmutableMap.of(), Object.class, null, ThriftStructMetadata.MetadataType.STRUCT,
                                    Optional.empty(), ImmutableList.of(), ImmutableList.of(), Optional.empty(), ImmutableList.of()))),
                    THRIFT_VALUE_PROPERTY,
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(AbstractTypedThriftCodec.class.getDeclaredMethod("getThriftField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
        }
        catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Failed to create ThriftFieldMetadata", e);
        }

        return ThriftType.struct(new ThriftStructMetadata(
                baseClass.getSimpleName() + "Wrapper",
                ImmutableMap.of(), baseClass, null, ThriftStructMetadata.MetadataType.STRUCT,
                Optional.empty(), ImmutableList.of(), fields, Optional.empty(), ImmutableList.of()));
    }

    @Override
    public T read(TProtocolReader reader)
            throws Exception
    {
        String connectorId = null;
        T value = null;
        String jsonValue = null;

        reader.readStructBegin();
        while (true) {
            TField field = reader.readFieldBegin();
            if (field.getType() == TType.STOP) {
                break;
            }
            switch (field.getId()) {
                case JSON_FIELD_ID:
                    jsonValue = reader.readString();
                    break;
                case TYPE_FIELD_ID:
                    connectorId = reader.readString();
                    break;
                case THRIFT_FIELD_ID:
                    requireNonNull(connectorId, "connectorId is null");
                    Class<? extends T> concreteClass = classResolver.apply(connectorId);
                    requireNonNull(concreteClass, "concreteClass is null");
                    value = readConcreteValue(connectorId, reader);
                    break;
            }
            reader.readFieldEnd();
        }
        reader.readStructEnd();

        if (jsonValue != null) {
            return jsonCodec.fromJson(jsonValue);
        }
        if (value != null) {
            return value;
        }
        throw new IllegalStateException("Neither thrift nor json value was present");
    }

    public abstract T readConcreteValue(String connectorId, TProtocolReader reader);

    public abstract void writeConcreteValue(String connectorId, T value, TProtocolWriter writer);

    @Override
    public void write(T value, TProtocolWriter writer)
            throws Exception
    {
        if (value == null) {
            return;
        }
        String connectorId = nameResolver.apply(value);
        if (connectorThriftCodecManager.isConnectorThriftCodecAvailable(connectorId)) {
            writer.writeStructBegin(new TStruct(baseClass.getSimpleName()));

            writer.writeFieldBegin(new TField(TYPE_PROPERTY, TType.STRING, TYPE_FIELD_ID));
            requireNonNull(connectorId, "connectorId is null");
            writer.writeString(connectorId);
            writer.writeFieldEnd();

            writer.writeFieldBegin(new TField(THRIFT_VALUE_PROPERTY, TType.STRUCT, THRIFT_FIELD_ID));
            writeConcreteValue(connectorId, value, writer);
            writer.writeFieldEnd();

            writer.writeFieldStop();
            writer.writeStructEnd();
            return;
        }
        // For non-enabled connectors, we still use json blob
        writeSingleJsonField(value, writer, jsonCodec, JSON_FIELD_ID, JSON_FIELD_NAME, JSON_STRUCT_NAME);
    }

    private String getTypeField()
    {
        return "getTypeField";
    }

    private String getThriftField()
    {
        return "getThriftField";
    }
}
