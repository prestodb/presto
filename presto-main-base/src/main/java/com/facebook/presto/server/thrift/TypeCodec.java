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

import com.facebook.airlift.log.Logger;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.codec.CodecThriftType;
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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TypeCodec
        implements ThriftCodec<Type>
{
    private static final Logger log = Logger.get(BlockCodec.class);
    private TypeManager typeManager;
    private static final ThriftType THRIFT_TYPE;

    static {
        List<ThriftFieldMetadata> fields = new ArrayList<>();
        try {
            fields.add(new ThriftFieldMetadata(
                    (short) 1,
                    false,
                    false,
                    ThriftField.Requiredness.REQUIRED,
                    ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.STRING),
                    "signature",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(Type.class.getDeclaredMethod("getTypeSignature"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));

            fields.add(new ThriftFieldMetadata(
                    (short) 2,
                    false,
                    false,
                    ThriftField.Requiredness.REQUIRED,
                    ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.BOOL),
                    "ignore",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(Type.class.getDeclaredMethod("getTypeSignature"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
        }
        catch (Exception e) {
            throw new RuntimeException("Error building ThriftFieldMetadata for TypeCodec", e);
        }

        THRIFT_TYPE = ThriftType.struct(new ThriftStructMetadata(
                "PrestoType",
                ImmutableMap.of(),
                Type.class,
                null,
                ThriftStructMetadata.MetadataType.STRUCT,
                Optional.empty(),
                ImmutableList.of(),
                fields,
                Optional.empty(),
                ImmutableList.of()));
    }

    @Inject
    public TypeCodec(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public ThriftType getType()
    {
        return THRIFT_TYPE;
    }

    @CodecThriftType
    public static ThriftType getThriftType()
    {
        return THRIFT_TYPE;
    }

    @Override
    public Type read(TProtocolReader protocol) throws Exception
    {
        protocol.readStructBegin();
        protocol.readFieldBegin();
        String signature = protocol.readString();
        protocol.readFieldEnd();
        protocol.readFieldBegin();
        boolean ignore = protocol.readBool();
        protocol.readFieldEnd();
        protocol.readStructEnd();
        if (signature != null) {
            return typeManager.getType(new TypeSignature(signature, ignore));
        }
        throw new IllegalStateException("Type value invalid");
    }

    @Override
    public void write(Type value, TProtocolWriter protocol) throws Exception
    {
        protocol.writeStructBegin(new TStruct("PrestoType"));
        protocol.writeFieldBegin(new TField("signature", TType.STRING, (short) 1));
        protocol.writeString(value.getTypeSignature().toString());
        protocol.writeFieldEnd();
        protocol.writeFieldBegin(new TField("ignore", TType.BOOL, (short) 2));
        protocol.writeBool(value.getTypeSignature().getIgnore());
        protocol.writeFieldEnd();
        protocol.writeStructEnd();
    }
}
