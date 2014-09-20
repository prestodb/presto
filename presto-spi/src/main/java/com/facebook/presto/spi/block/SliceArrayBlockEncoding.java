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

public class SliceArrayBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<SliceArrayBlockEncoding> FACTORY = new SliceArrayBlockEncodingFactory();
    private static final String NAME = "SLICE_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        SliceArrayBlock sliceArrayBlock = (SliceArrayBlock) block;

        int positionCount = sliceArrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // offsets
        for (int position = 0; position < positionCount; position++) {
            int length = 0;
            if (!sliceArrayBlock.isNull(position)) {
                length = sliceArrayBlock.getLength(position);
            }
            sliceOutput.appendInt(length);
        }

        // write null bits 8 at a time
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = 0;
            value |= sliceArrayBlock.isNull(position)     ? 0b1000_0000 : 0;
            value |= sliceArrayBlock.isNull(position + 1) ? 0b0100_0000 : 0;
            value |= sliceArrayBlock.isNull(position + 2) ? 0b0010_0000 : 0;
            value |= sliceArrayBlock.isNull(position + 3) ? 0b0001_0000 : 0;
            value |= sliceArrayBlock.isNull(position + 4) ? 0b0000_1000 : 0;
            value |= sliceArrayBlock.isNull(position + 5) ? 0b0000_0100 : 0;
            value |= sliceArrayBlock.isNull(position + 6) ? 0b0000_0010 : 0;
            value |= sliceArrayBlock.isNull(position + 7) ? 0b0000_0001 : 0;
            sliceOutput.appendByte(value);
        }

        // write last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                value |= sliceArrayBlock.isNull(position) ? mask : 0;
                mask >>>= 1;
            }
            sliceOutput.appendByte(value);
        }

        for (Slice value : sliceArrayBlock.getValues()) {
            if (value != null) {
                sliceOutput.writeBytes(value);
            }
        }
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
        boolean[] valueIsNull = new boolean[positionCount];
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = sliceInput.readByte();
            valueIsNull[position] = ((value & 0b1000_0000) != 0);
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

        Slice[] values = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (!valueIsNull[position]) {
                values[position] = sliceInput.readSlice(offsets[position + 1] - offsets[position]);
            }
        }

        return new SliceArrayBlock(positionCount, values);
    }

    public static class SliceArrayBlockEncodingFactory
            implements BlockEncodingFactory<SliceArrayBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SliceArrayBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            return new SliceArrayBlockEncoding();
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, SliceArrayBlockEncoding blockEncoding)
        {
        }
    }
}
