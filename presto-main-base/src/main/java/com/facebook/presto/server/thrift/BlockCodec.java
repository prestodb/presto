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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.inject.Inject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.Math.toIntExact;

public class BlockCodec
        implements ThriftCodec<Block>
{
    private static final ThriftType THRIFT_TYPE;

    private static final Logger log = Logger.get(BlockCodec.class);
    private BlockEncodingSerde blockEncodingSerde;

    @Inject
    public BlockCodec(BlockEncodingSerde blockEncodingSerde)
    {
        log.info("BlockCodec registered");
        this.blockEncodingSerde = blockEncodingSerde;
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
    public Block read(TProtocolReader protocol) throws Exception
    {
        protocol.readStructBegin();
        protocol.readFieldBegin();
        ByteBuffer byteBuffer = protocol.readBinary();
        Slice slice = Slices.wrappedBuffer(byteBuffer);
        protocol.readFieldEnd();
        protocol.readStructEnd();
        return blockEncodingSerde.readBlock(slice.getInput());
    }

    @Override
    public void write(Block value, TProtocolWriter protocol) throws Exception
    {
        protocol.writeStructBegin(new TStruct("Block"));
        protocol.writeFieldBegin(new TField("data", TType.STRING, (short) 1));
        int estimatedSize = toIntExact(value.getSizeInBytes())
                + value.getEncodingName().length()
                + (2 * Integer.BYTES);
        SliceOutput output = new DynamicSliceOutput(estimatedSize);
        blockEncodingSerde.writeBlock(output, value);
        protocol.writeBinary(output.slice().toByteBuffer());
        protocol.writeFieldEnd();
        protocol.writeStructEnd();
    }

    static {
        List<ThriftFieldMetadata> fields = new ArrayList<>();
        try {
            fields.add(new ThriftFieldMetadata(
                    (short) 1,
                    false,
                    false,
                    ThriftField.Requiredness.REQUIRED,
                    ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.BINARY),
                    "data",
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(Block.class.getDeclaredMethod("getRetainedSizeInBytes"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
        }
        catch (Exception e) {
            throw new RuntimeException("Error building ThriftFieldMetadata for BlockCodec", e);
        }

        THRIFT_TYPE = ThriftType.struct(new ThriftStructMetadata(
                "Block",
                ImmutableMap.of(),
                Block.class,
                null,
                ThriftStructMetadata.MetadataType.STRUCT,
                Optional.empty(),
                ImmutableList.of(),
                fields,
                Optional.empty(),
                ImmutableList.of()));
    }
}
