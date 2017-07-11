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
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

public class ArrayBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<ArrayBlockEncoding> FACTORY = new ArrayBlockEncodingFactory();
    private static final String NAME = "ARRAY";

    private final BlockEncoding valueBlockEncoding;

    public ArrayBlockEncoding(BlockEncoding valueBlockEncoding)
    {
        this.valueBlockEncoding = valueBlockEncoding;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;

        int positionCount = arrayBlock.getPositionCount();

        int offsetBase = arrayBlock.getOffsetBase();
        int[] offsets = arrayBlock.getOffsets();

        int valuesStartOffset = offsets[offsetBase];
        int valuesEndOffset = offsets[offsetBase + positionCount];
        Block values = arrayBlock.getValues().getRegion(valuesStartOffset, valuesEndOffset - valuesStartOffset);
        valueBlockEncoding.writeBlock(sliceOutput, values);

        sliceOutput.appendInt(positionCount);
        for (int position = 0; position < positionCount + 1; position++) {
            sliceOutput.writeInt(offsets[offsetBase + position] - valuesStartOffset);
        }
        EncoderUtil.encodeNullsAsBits(sliceOutput, block);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        Block values = valueBlockEncoding.readBlock(sliceInput);

        int positionCount = sliceInput.readInt();
        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(Slices.wrappedIntArray(offsets));
        boolean[] valueIsNull = EncoderUtil.decodeNullBits(sliceInput, positionCount);
        return new ArrayBlock(positionCount, valueIsNull, offsets, values);
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class ArrayBlockEncodingFactory
            implements BlockEncodingFactory<ArrayBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public ArrayBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            BlockEncoding valueBlockEncoding = serde.readBlockEncoding(input);
            return new ArrayBlockEncoding(valueBlockEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, ArrayBlockEncoding blockEncoding)
        {
            serde.writeBlockEncoding(output, blockEncoding.valueBlockEncoding);
        }
    }
}
