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
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.inject.Inject;

import java.nio.ByteBuffer;

public class BlockCodec
        implements ThriftCodec<Block>
{
    private static final ThriftType THRIFT_TYPE = new ThriftType(ThriftType.BINARY, Block.class);

    private static final Logger log = Logger.get(BlockCodec.class);
    private BlockEncodingSerde blockEncodingSerde;

    @Inject
    public BlockCodec(BlockEncodingSerde blockEncodingSerde)
    {
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
        ByteBuffer byteBuffer = protocol.readBinary();
        Slice slice = Slices.wrappedBuffer(byteBuffer);
        return blockEncodingSerde.readBlock(slice.getInput());
    }

    @Override
    public void write(Block value, TProtocolWriter protocol) throws Exception
    {
        Slice slice = Slices.allocate((int) value.getSizeInBytes());
        blockEncodingSerde.writeBlock(slice.getOutput(), value);
        protocol.writeBinary(slice.toByteBuffer());
    }
}
