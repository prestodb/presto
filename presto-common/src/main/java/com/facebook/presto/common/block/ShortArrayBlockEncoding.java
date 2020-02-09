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
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.Slices.wrappedShortArray;

public class ShortArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "SHORT_ARRAY";

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

        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                sliceOutput.writeShort(block.getShort(position));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        short[] values = new short[positionCount];

        // Fast track if no nulls present
        if (!sliceInput.readBoolean()) {
            sliceInput.readBytes(wrappedShortArray(values));
            return new ShortArrayBlock(0, positionCount, null, values);
        }

        boolean[] valueIsNull = new boolean[positionCount];
        int nullPositions = decodeNullBits(sliceInput, valueIsNull);
        int nonNullPositions = positionCount - nullPositions;

        // Read compact values array and redistribute to non-null positions
        sliceInput.readBytes(wrappedShortArray(values), nonNullPositions * SIZE_OF_SHORT);
        int writePosition = values.length - 1;
        int readPosition = nonNullPositions - 1;
        while (readPosition >= 0) {
            values[writePosition] = values[readPosition];
            readPosition -= valueIsNull[writePosition] ? 0 : 1;
            writePosition--;
        }
        return new ShortArrayBlock(0, positionCount, valueIsNull, values);
    }
}
