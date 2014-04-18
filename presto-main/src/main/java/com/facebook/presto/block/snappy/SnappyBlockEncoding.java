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
package com.facebook.presto.block.snappy;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.serde.TypeSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkArgument;

public class SnappyBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<SnappyBlockEncoding> FACTORY = new SnappyBlockEncodingFactory();
    private static final String NAME = "SNAPPY";

    private final Type type;
    private final BlockEncoding uncompressedBlockEncoding;

    public SnappyBlockEncoding(Type type, BlockEncoding uncompressedBlockEncoding)
    {
        this.type = type;
        this.uncompressedBlockEncoding = uncompressedBlockEncoding;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        SnappyBlock snappyBlock = (SnappyBlock) block;
        checkArgument(block.getType().equals(type), "Invalid block");

        Slice compressedSlice = snappyBlock.getCompressedSlice();
        sliceOutput
                .appendInt(compressedSlice.length())
                .appendInt(snappyBlock.getPositionCount())
                .writeBytes(compressedSlice);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int blockSize = sliceInput.readInt();
        int positionCount = sliceInput.readInt();

        Slice compressedSlice = sliceInput.readSlice(blockSize);
        return new SnappyBlock(positionCount, type, compressedSlice, uncompressedBlockEncoding);
    }

    private static class SnappyBlockEncodingFactory
            implements BlockEncodingFactory<SnappyBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SnappyBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            Type type = TypeSerde.readType(manager, input);
            BlockEncoding valueBlockEncoding = serde.readBlockEncoding(input);
            return new SnappyBlockEncoding(type, valueBlockEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, SnappyBlockEncoding blockEncoding)
        {
            TypeSerde.writeInfo(output, blockEncoding.type);
            serde.writeBlockEncoding(output, blockEncoding.uncompressedBlockEncoding);
        }
    }
}
