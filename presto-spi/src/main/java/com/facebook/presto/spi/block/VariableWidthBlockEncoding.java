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

public class VariableWidthBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<VariableWidthBlockEncoding> FACTORY = new VariableWidthBlockEncodingFactory();
    private static final String NAME = "VARIABLE_WIDTH";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;

        int positionCount = variableWidthBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // offsets
        int totalLength = 0;
        for (int position = 0; position < positionCount; position++) {
            int length = variableWidthBlock.getLength(position);
            sliceOutput.appendInt(length);
            totalLength += length;
        }

        // write null bits 8 at a time
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = 0;
            value |= variableWidthBlock.isNull(position)     ? 0b1000_0000 : 0;
            value |= variableWidthBlock.isNull(position + 1) ? 0b0100_0000 : 0;
            value |= variableWidthBlock.isNull(position + 2) ? 0b0010_0000 : 0;
            value |= variableWidthBlock.isNull(position + 3) ? 0b0001_0000 : 0;
            value |= variableWidthBlock.isNull(position + 4) ? 0b0000_1000 : 0;
            value |= variableWidthBlock.isNull(position + 5) ? 0b0000_0100 : 0;
            value |= variableWidthBlock.isNull(position + 6) ? 0b0000_0010 : 0;
            value |= variableWidthBlock.isNull(position + 7) ? 0b0000_0001 : 0;
            sliceOutput.appendByte(value);
        }

        // write last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                value |= variableWidthBlock.isNull(position) ? mask : 0;
                mask >>>= 1;
            }
            sliceOutput.appendByte(value);
        }

        sliceOutput
                .appendInt(totalLength)
                .writeBytes(variableWidthBlock.getRawSlice(0), variableWidthBlock.getPositionOffset(0), totalLength);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        // offsets
        int[] offsets = new int[positionCount + 1];
        int offset = 0;
        for (int position = 0; position < positionCount; position++) {
            offset += sliceInput.readInt();
            offsets[position + 1] = offset;
        }

        // read null bits 8 at a time
        boolean[]  valueIsNull = new boolean[positionCount];
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

        return new VariableWidthBlock(positionCount, slice, offsets, valueIsNull);
    }

    public static class VariableWidthBlockEncodingFactory
            implements BlockEncodingFactory<VariableWidthBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public VariableWidthBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            return new VariableWidthBlockEncoding();
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, VariableWidthBlockEncoding blockEncoding)
        {
        }
    }
}
