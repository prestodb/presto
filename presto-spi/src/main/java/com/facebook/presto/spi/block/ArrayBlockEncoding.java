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

        valueBlockEncoding.writeBlock(sliceOutput, arrayBlock.getValues());
        sliceOutput.appendInt(arrayBlock.getOffsetBase());
        int positionCount = arrayBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);
        sliceOutput.writeBytes(arrayBlock.getOffsets(), 0, positionCount * 4);
        EncoderUtil.encodeNullsAsBits(sliceOutput, block);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        Block values = valueBlockEncoding.readBlock(sliceInput);
        int offsetBase = sliceInput.readInt();
        int positionCount = sliceInput.readInt();
        byte[] offsets = new byte[positionCount * 4];
        sliceInput.readBytes(offsets);
        boolean[] valueIsNull = EncoderUtil.decodeNullBits(sliceInput, positionCount);
        return new ArrayBlock(values, Slices.wrappedBuffer(offsets), offsetBase, Slices.wrappedBooleanArray(valueIsNull));
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
