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
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockEncoding;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.serde.TypeSerde;
import com.facebook.presto.type.FixedWidthType;
import com.facebook.presto.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class FixedWidthBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<FixedWidthBlockEncoding> FACTORY = new FixedWidthBlockEncodingFactory();
    private static final String NAME = "FIXED_WIDTH";

    private final FixedWidthType type;

    public FixedWidthBlockEncoding(Type type)
    {
        this.type = (FixedWidthType) checkNotNull(type, "type is null");
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
        AbstractFixedWidthBlock fixedWidthBlock = (AbstractFixedWidthBlock) block;
        checkArgument(block.getType().equals(type), "Invalid block");
        writeUncompressedBlock(sliceOutput,
                fixedWidthBlock.getPositionCount(),
                fixedWidthBlock.getRawSlice());
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int blockSize = sliceInput.readInt();
        int positionCount = sliceInput.readInt();

        Slice slice = sliceInput.readSlice(blockSize);
        return new FixedWidthBlock(type, positionCount, slice);
    }

    private static void writeUncompressedBlock(SliceOutput destination, int positionCount, Slice slice)
    {
        destination
                .appendInt(slice.length())
                .appendInt(positionCount)
                .writeBytes(slice);
    }

    private static class FixedWidthBlockEncodingFactory
            implements BlockEncodingFactory<FixedWidthBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public FixedWidthBlockEncoding readEncoding(BlockEncodingManager blockEncodingManager, SliceInput input)
        {
            Type type = TypeSerde.readType(input);
            return new FixedWidthBlockEncoding(type);
        }

        @Override
        public void writeEncoding(BlockEncodingManager blockEncodingManager, SliceOutput output, FixedWidthBlockEncoding blockEncoding)
        {
            TypeSerde.writeInfo(output, blockEncoding.type);
        }
    }
}
