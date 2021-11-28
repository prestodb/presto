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

public class ByteArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "BYTE_ARRAY";

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
                sliceOutput.writeByte(block.getByte(position));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        byte[] values = new byte[positionCount];
        if (valueIsNull == null) {
            // No nulls present, read values array directly from input
            sliceInput.readBytes(values, 0, values.length);
        }
        else {
            for (int position = 0; position < values.length; position++) {
                if (!valueIsNull[position]) {
                    values[position] = sliceInput.readByte();
                }
            }
        }

        return new ByteArrayBlock(0, positionCount, valueIsNull, values);
    }
}
