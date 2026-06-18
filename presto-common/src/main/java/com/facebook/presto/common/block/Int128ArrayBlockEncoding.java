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
import io.airlift.slice.Slices;

import static com.facebook.presto.common.block.EncoderUtil.decodeNullBits;
import static com.facebook.presto.common.block.EncoderUtil.encodeNullsAsBits;

public class Int128ArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "INT128_ARRAY";

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
        for (int position = 0; position < positionCount; position++) {
            if (!mayHaveNull || !block.isNull(position)) {
                sliceOutput.writeLong(block.getLong(position, 0));
                sliceOutput.writeLong(block.getLong(position, 8));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount * 2];
        if (valueIsNull == null) {
            // No nulls present, read values array directly from input
            sliceInput.readBytes(Slices.wrappedLongArray(values));
        }
        else {
            for (int position = 0, index = 0; index < values.length; position++, index += 2) {
                if (!valueIsNull[position]) {
                    values[index] = sliceInput.readLong();
                    values[index + 1] = sliceInput.readLong();
                }
            }
        }

        return new Int128ArrayBlock(0, positionCount, valueIsNull, values);
    }
}
