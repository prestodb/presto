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

import com.facebook.drift.annotations.ThriftField.Requiredness;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.internal.ProtocolReader;
import com.facebook.drift.codec.internal.ProtocolWriter;
import com.facebook.drift.codec.metadata.DefaultThriftTypeReference;
import com.facebook.drift.codec.metadata.FieldKind;
import com.facebook.drift.codec.metadata.ThriftFieldMetadata;
import com.facebook.drift.codec.metadata.ThriftMethodInjection;
import com.facebook.drift.codec.metadata.ThriftStructMetadata;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolException;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public final class TypeCodec
        implements ThriftCodec<Type>
{
    private static final ThriftType THRIFT_TYPE = createThriftType();

    private final TypeManager typeManager;

    public TypeCodec(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ThriftType getType()
    {
        return THRIFT_TYPE;
    }

    @Override
    public Type read(TProtocolReader protocol)
            throws Exception
    {
        ProtocolReader reader = new ProtocolReader(protocol);
        reader.readStructBegin();
        String value = null;
        while (reader.nextField()) {
            if (reader.getFieldId() == 1) {
                value = reader.readStringField();
            }
            else {
                reader.skipFieldData();
            }
        }
        reader.readStructEnd();
        if (value == null) {
            throw new TProtocolException("Missing signature");
        }
        Type type = typeManager.getType(parseTypeSignature(value));
        if (type == null) {
            throw new TProtocolException("Unknown type signature: " + value);
        }
        return type;
    }

    @Override
    public void write(Type value, TProtocolWriter protocol)
            throws Exception
    {
        ProtocolWriter writer = new ProtocolWriter(protocol);
        writer.writeStructBegin("PrestoType");
        writer.writeStringField("signature", (short) 1, value.getTypeSignature().toString());
        writer.writeStructEnd();
    }

    @CodecThriftType
    public static ThriftType createThriftType()
    {
        try {
            ThriftFieldMetadata field = new ThriftFieldMetadata(
                    (short) 1,
                    false,
                    false,
                    Requiredness.REQUIRED,
                    ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.STRING),
                    "signature",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(TypeCodec.class.getMethod("getType"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty());
            return ThriftType.struct(new ThriftStructMetadata(
                    "PrestoType",
                    ImmutableMap.of(),
                    Type.class,
                    null,
                    ThriftStructMetadata.MetadataType.STRUCT,
                    Optional.empty(),
                    ImmutableList.of(),
                    ImmutableList.of(field),
                    Optional.empty(),
                    ImmutableList.of()));
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }
}
