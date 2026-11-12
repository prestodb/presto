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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;

import java.nio.ByteBuffer;
import java.util.Optional;

import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public final class BlockCodec
        implements ThriftCodec<Block>
{
    private static final ThriftType THRIFT_TYPE = createThriftType();

    private final BlockEncodingSerde blockEncodingSerde;

    public BlockCodec(BlockEncodingSerde blockEncodingSerde)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    @Override
    public ThriftType getType()
    {
        return THRIFT_TYPE;
    }

    @Override
    public Block read(TProtocolReader protocol)
            throws Exception
    {
        ProtocolReader reader = new ProtocolReader(protocol);
        reader.readStructBegin();
        ByteBuffer value = null;
        while (reader.nextField()) {
            if (reader.getFieldId() == 1) {
                value = reader.readBinaryField();
            }
            else {
                reader.skipFieldData();
            }
        }
        reader.readStructEnd();
        if (value == null) {
            throw new TProtocolException("Missing data");
        }
        return blockEncodingSerde.readBlock(wrappedBuffer(value).getInput());
    }

    @Override
    public void write(Block value, TProtocolWriter protocol)
            throws Exception
    {
        DynamicSliceOutput output = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(output, value);
        ProtocolWriter writer = new ProtocolWriter(protocol);
        writer.writeStructBegin("Block");
        writer.writeBinaryField("data", (short) 1, output.slice().toByteBuffer());
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
                    new DefaultThriftTypeReference(ThriftType.BINARY),
                    "data",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(BlockCodec.class.getMethod("getType"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty());
            return ThriftType.struct(new ThriftStructMetadata(
                    "Block",
                    ImmutableMap.of(),
                    Block.class,
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
