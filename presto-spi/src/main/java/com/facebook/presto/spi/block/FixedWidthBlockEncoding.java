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
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = 0;
            value |= fixedWidthBlock.isNull(position)     ? 0b1000_0000 : 0;
            value |= fixedWidthBlock.isNull(position + 1) ? 0b0100_0000 : 0;
            value |= fixedWidthBlock.isNull(position + 2) ? 0b0010_0000 : 0;
            value |= fixedWidthBlock.isNull(position + 3) ? 0b0001_0000 : 0;
            value |= fixedWidthBlock.isNull(position + 4) ? 0b0000_1000 : 0;
            value |= fixedWidthBlock.isNull(position + 5) ? 0b0000_0100 : 0;
            value |= fixedWidthBlock.isNull(position + 6) ? 0b0000_0010 : 0;
            value |= fixedWidthBlock.isNull(position + 7) ? 0b0000_0001 : 0;
            sliceOutput.appendByte(value);
        }

        // write last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                value |= fixedWidthBlock.isNull(position) ? mask : 0;
                mask >>>= 1;
            }
            sliceOutput.appendByte(value);
        }

        Slice slice = fixedWidthBlock.getRawSlice();
        sliceOutput
                .appendInt(slice.length())
                .writeBytes(slice);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[]  valueIsNull = new boolean[positionCount];

        // read null bits 8 at a time
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = sliceInput.readByte();
            valueIsNull[position    ] = ((value & 0b1000_0000) != 0);
            valueIsNull[position + 1] = ((value & 0b0100_0000) != 0);
            valueIsNull[position + 2] = ((value & 0b0010_0000) != 0);
            valueIsNull[position + 3] = ((value & 0b0001_0000) != 0);
            valueIsNull[position + 4] = ((value & 0b0000_1000) != 0);
            valueIsNull[position + 5] = ((value & 0b0000_0100) != 0);
            valueIsNull[position + 6] = ((value & 0b0000_0010) != 0);
            valueIsNull[position + 7] = ((value & 0b0000_0001) != 0);
        }

        // read last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = sliceInput.readByte();
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                valueIsNull[position] = ((value & mask) != 0);
                mask >>>= 1;
            }
        }

        int blockSize = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(blockSize);

        return new FixedWidthBlock(fixedSize, positionCount, slice, valueIsNull);
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
