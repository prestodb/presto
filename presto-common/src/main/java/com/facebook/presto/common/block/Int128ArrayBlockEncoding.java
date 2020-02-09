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
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.wrappedLongArray;

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

        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                sliceOutput.writeLong(block.getLong(position, 0));
                sliceOutput.writeLong(block.getLong(position, 8));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        int valuesSize = positionCount * 2;
        long[] values = new long[valuesSize];

        // Fast track if no nulls present
        if (!sliceInput.readBoolean()) {
            sliceInput.readBytes(wrappedLongArray(values));
            return new Int128ArrayBlock(0, positionCount, null, values);
        }

        boolean[] valueIsNull = new boolean[positionCount];
        int nullPositions = decodeNullBits(sliceInput, valueIsNull);
        int nonNullPositions = (positionCount - nullPositions) * 2;

        // Read compact values array and redistribute to non-null positions
        sliceInput.readBytes(wrappedLongArray(values), nonNullPositions * SIZE_OF_LONG);
        int writePosition = values.length - 2;
        int readPosition = nonNullPositions - 2;
        while (readPosition >= 0) {
            values[writePosition] = values[readPosition];
            values[writePosition + 1] = values[readPosition + 1];
            readPosition -= valueIsNull[writePosition >> 1] ? 0 : 2;
            writePosition -= 2;
        }
        return new Int128ArrayBlock(0, positionCount, valueIsNull, values);
    }
}
