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
package com.facebook.presto.common.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.common.block.EncoderUtil.decodeNullBits;
import static com.facebook.presto.common.block.EncoderUtil.encodeNullsAsBits;
import static com.facebook.presto.common.block.Fixed12Block.encodeFixed12;

public class Fixed12BlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "FIXED12_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        boolean mayHaveNull = block.mayHaveNull();
        if (block instanceof Fixed12Block) {
            Fixed12Block fixed12Block = (Fixed12Block) block;
            for (int position = 0; position < positionCount; position++) {
                if (!mayHaveNull || !block.isNull(position)) {
                    sliceOutput.writeLong(fixed12Block.getFixed12First(position));
                    sliceOutput.writeInt(fixed12Block.getFixed12Second(position));
                }
            }
        }
        else {
            for (int position = 0; position < positionCount; position++) {
                if (!mayHaveNull || !block.isNull(position)) {
                    sliceOutput.writeLong(block.getLong(position, 0));
                    sliceOutput.writeInt(block.getInt(position));
                }
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int[] values = new int[positionCount * 3];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                long first = sliceInput.readLong();
                int second = sliceInput.readInt();
                encodeFixed12(first, second, values, position);
            }
        }

        return new Fixed12Block(0, positionCount, valueIsNull, values);
    }
}
