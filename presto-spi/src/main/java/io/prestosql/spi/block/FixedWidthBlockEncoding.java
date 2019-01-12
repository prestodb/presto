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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.block.EncoderUtil.decodeNullBits;
import static com.facebook.presto.spi.block.EncoderUtil.encodeNullsAsBits;

public class FixedWidthBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "FIXED_WIDTH";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        AbstractFixedWidthBlock fixedWidthBlock = (AbstractFixedWidthBlock) block;

        sliceOutput.appendInt(fixedWidthBlock.getFixedSize());
        sliceOutput.appendInt(fixedWidthBlock.getPositionCount());

        // write null bits 8 at a time
        encodeNullsAsBits(sliceOutput, fixedWidthBlock);

        Slice slice = fixedWidthBlock.getRawSlice();
        sliceOutput
                .appendInt(slice.length())
                .writeBytes(slice);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int fixedSize = sliceInput.readInt();
        int positionCount = sliceInput.readInt();

        Slice valueIsNull = decodeNullBits(sliceInput, positionCount)
                .map(Slices::wrappedBooleanArray)
                .orElse(null);

        int blockSize = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(blockSize);

        return new FixedWidthBlock(fixedSize, positionCount, slice, valueIsNull);
    }
}
