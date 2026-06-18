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
import com.facebook.airlift.log.Logger;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public abstract class AbstractTypedThriftCodec<T>
        implements ThriftCodec<T>
{
    private static final Set<String> NON_THRIFT_CONNECTOR = new HashSet<>();
    private static final Logger log = Logger.get(AbstractTypedThriftCodec.class);
    private static final String TYPE_VALUE = "connectorId";
    private static final String CUSTOM_SERIALIZED_VALUE = "customSerializedValue";
    private static final String JSON_VALUE = "jsonValue";
    private static final short TYPE_FIELD_ID = 1;
    private static final short CUSTOM_FIELD_ID = 2;
    private static final short JSON_FIELD_ID = 3;

    private final Class<T> baseClass;
    private final JsonCodec<T> jsonCodec;
    private final Function<T, String> nameResolver;
    private final Function<String, Class<? extends T>> classResolver;

    protected AbstractTypedThriftCodec(Class<T> baseClass,
            JsonCodec<T> jsonCodec,
            Function<T, String> nameResolver,
            Function<String, Class<? extends T>> classResolver)
    {
        this.baseClass = requireNonNull(baseClass, "baseClass is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.nameResolver = requireNonNull(nameResolver, "nameResolver is null");
        this.classResolver = requireNonNull(classResolver, "classResolver is null");
    }

    @Override
    public abstract ThriftType getType();

    protected static ThriftType createThriftType(Class<?> baseClass)
    {
        List<ThriftFieldMetadata> fields = new ArrayList<>();
        try {
            fields.add(new ThriftFieldMetadata(
                    TYPE_FIELD_ID,
                    false, false, Requiredness.OPTIONAL, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.STRING),
                    TYPE_VALUE,
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    // Drift requires at least one of the three arguments below, so we provide a dummy method here as a workaround.
                    // https://github.com/airlift/drift/blob/master/drift-codec/src/main/java/io/airlift/drift/codec/metadata/ThriftFieldMetadata.java#L99
                    Optional.of(new ThriftMethodInjection(AbstractTypedThriftCodec.class.getDeclaredMethod("getTypeField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
            fields.add(new ThriftFieldMetadata(
                    CUSTOM_FIELD_ID,
                    false, false, Requiredness.OPTIONAL, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.BINARY),
                    CUSTOM_SERIALIZED_VALUE,
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    // Drift requires at least one of the three arguments below, so we provide a dummy method here as a workaround.
                    // https://github.com/airlift/drift/blob/master/drift-codec/src/main/java/io/airlift/drift/codec/metadata/ThriftFieldMetadata.java#L99
                    Optional.of(new ThriftMethodInjection(AbstractTypedThriftCodec.class.getDeclaredMethod("getCustomField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
            // TODO: This field will be cleaned up: https://github.com/prestodb/presto/issues/25671
            fields.add(new ThriftFieldMetadata(
                    JSON_FIELD_ID,
                    false, false, Requiredness.OPTIONAL, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.STRING),
                    JSON_VALUE,
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    // Drift requires at least one of the three arguments below, so we provide a dummy method here as a workaround.
                    // https://github.com/airlift/drift/blob/master/drift-codec/src/main/java/io/airlift/drift/codec/metadata/ThriftFieldMetadata.java#L99
                    Optional.of(new ThriftMethodInjection(AbstractTypedThriftCodec.class.getDeclaredMethod("getJsonField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
        }
        catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Failed to create ThriftFieldMetadata", e);
        }

        return ThriftType.struct(new ThriftStructMetadata(
                baseClass.getSimpleName(),
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
                case CUSTOM_FIELD_ID:
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

    public abstract T readConcreteValue(String connectorId, TProtocolReader reader)
            throws Exception;

    public abstract void writeConcreteValue(String connectorId, T value, TProtocolWriter writer)
            throws Exception;

    public abstract boolean isThriftCodecAvailable(String connectorId);

    @Override
    public void write(T value, TProtocolWriter writer)
            throws Exception
    {
        if (value == null) {
            return;
        }
        String connectorId = nameResolver.apply(value);
        requireNonNull(connectorId, "connectorId is null");

        writer.writeStructBegin(new TStruct(baseClass.getSimpleName()));
        if (isThriftCodecAvailable(connectorId)) {
            writer.writeFieldBegin(new TField(TYPE_VALUE, TType.STRING, TYPE_FIELD_ID));
            writer.writeString(connectorId);
            writer.writeFieldEnd();

            writer.writeFieldBegin(new TField(CUSTOM_SERIALIZED_VALUE, TType.STRING, CUSTOM_FIELD_ID));
            writeConcreteValue(connectorId, value, writer);
            writer.writeFieldEnd();
        }
        else {
            // If thrift codec is not available for this connector, fall back to its json
            writer.writeFieldBegin(new TField(JSON_VALUE, TType.STRING, JSON_FIELD_ID));
            writer.writeString(jsonCodec.toJson(value));
            writer.writeFieldEnd();
        }
        writer.writeFieldStop();
        writer.writeStructEnd();
    }

    private String getTypeField()
    {
        return "getTypeField";
    }

    private String getCustomField()
    {
        return "getCustomField";
    }

    private String getJsonField()
    {
        return "getJsonField";
    }
}
