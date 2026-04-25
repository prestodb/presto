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
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConstantExpressionCodec
        implements ThriftCodec<ConstantExpression>
{
    private static final Logger log = Logger.get(ConstantExpressionCodec.class);
    private final Provider<ThriftCodecManager> codecManagerProvider;
    private final TypeManager typeManager;

    @Inject
    public ConstantExpressionCodec(TypeManager typeManager, Provider<ThriftCodecManager> codecManagerProvider)
    {
        this.typeManager = typeManager;
        this.codecManagerProvider = codecManagerProvider;
    }

    @Override
    public ThriftType getType()
    {
        List<ThriftFieldMetadata> fields = new ArrayList<>();
        try {
            fields.add(new ThriftFieldMetadata(
                    (short) 1,
                    false, false, ThriftField.Requiredness.REQUIRED, ImmutableMap.of(),
                    new DefaultThriftTypeReference(TypeCodec.getThriftType()),
                    "type",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    // Drift requires at least one of the three arguments below, so we provide a dummy method here as a workaround.
                    // https://github.com/airlift/drift/blob/master/drift-codec/src/main/java/io/airlift/drift/codec/metadata/ThriftFieldMetadata.java#L99
                    Optional.of(new ThriftMethodInjection(ConstantExpressionCodec.class.getDeclaredMethod("getTypeField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
            fields.add(new ThriftFieldMetadata(
                    (short) 2,
                    false, false, ThriftField.Requiredness.REQUIRED, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.BINARY),
                    "block",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    // Drift requires at least one of the three arguments below, so we provide a dummy method here as a workaround.
                    // https://github.com/airlift/drift/blob/master/drift-codec/src/main/java/io/airlift/drift/codec/metadata/ThriftFieldMetadata.java#L99
                    Optional.of(new ThriftMethodInjection(ConstantExpressionCodec.class.getDeclaredMethod("getCustomField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Failed to create ThriftFieldMetadata", e);
        }
        return ThriftType.struct(new ThriftStructMetadata(
                "ConstantExpression",
                ImmutableMap.of(), ConstantExpression.class, null, ThriftStructMetadata.MetadataType.STRUCT,
                Optional.empty(), ImmutableList.of(), fields, Optional.empty(), ImmutableList.of()));
    }

    @Override
    public ConstantExpression read(TProtocolReader reader) throws Exception
    {
        Type type = null;
        Block block = null;

        reader.readStructBegin();
        while (true) {
            TField field = reader.readFieldBegin();
            if (field.getType() == TType.STOP) {
                break;
            }
            switch (field.getId()) {
                case 1:
                    type = codecManagerProvider.get().getCodec(Type.class).read(reader);
                    break;
                case 2:
                    block = codecManagerProvider.get().getCodec(Block.class).read(reader);
                    break;
                default:
                    break;
            }
            reader.readFieldEnd();
        }
        reader.readStructEnd();
        return new ConstantExpression(block, type);
    }

    @Override
    public void write(ConstantExpression expression, TProtocolWriter writer) throws Exception
    {
        writer.writeStructBegin(new TStruct("ConstantExpression"));
        ThriftCodec<Type> typeCodec = codecManagerProvider.get().getCodec(Type.class);
        ThriftCodec<Block> blockCodec = codecManagerProvider.get().getCodec(Block.class);

        writer.writeFieldBegin(new TField("type", typeCodec.getType().getProtocolType().getType(), (short) 1));
        typeCodec.write(expression.getType(), writer);
        writer.writeFieldEnd();
        writer.writeFieldBegin(new TField("block", blockCodec.getType().getProtocolType().getType(), (short) 2));
        typeCodec.write(expression.getType(), writer);
        writer.writeFieldEnd();

        writer.writeStructEnd();
    }

    private String getCustomField()
    {
        return "getCustomField";
    }

    private String getTypeField()
    {
        return "getTypeField";
    }
}
