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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.block.EncoderUtil.decodeNullBits;
import static com.facebook.presto.spi.block.EncoderUtil.encodeNullsAsBits;

public class FixedWidthBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<FixedWidthBlockEncoding> FACTORY = new FixedWidthBlockEncodingFactory();
    private static final String NAME = "FIXED_WIDTH";
    private final int fixedSize;

    public FixedWidthBlockEncoding(int fixedSize)
    {
        if (fixedSize < 0) {
            throw new IllegalArgumentException("fixedSize is negative");
        }
        this.fixedSize = fixedSize;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    public int getFixedSize()
    {
        return fixedSize;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        AbstractFixedWidthBlock fixedWidthBlock = (AbstractFixedWidthBlock) block;

        int positionCount = fixedWidthBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // write null bits 8 at a time
        encodeNullsAsBits(sliceOutput, fixedWidthBlock);

        Slice slice = fixedWidthBlock.getRawSlice();
        sliceOutput
                .appendInt(slice.length())
                .writeBytes(slice);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        int blockSize = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(blockSize);

        return new FixedWidthBlock(fixedSize, positionCount, slice, Slices.wrappedBooleanArray(valueIsNull));
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class FixedWidthBlockEncodingFactory
            implements BlockEncodingFactory<FixedWidthBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public FixedWidthBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            int entrySize = input.readInt();
            return new FixedWidthBlockEncoding(entrySize);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, FixedWidthBlockEncoding blockEncoding)
        {
            output.writeInt(blockEncoding.getFixedSize());
        }
    }
}
