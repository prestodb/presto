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

import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.ByteArrayUtils.writeLengthPrefixedString;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class LongArrayBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    public static final String NAME = LongArrayBlockEncoding.NAME;
    public static final int DEFAULT_MAX_ELEMENT_COUNT = 128 * 1024;

    // These are single piece buffers for now. They will be requested from buffer pool dynamically
    private byte[] nullsBuffer;
    private int nullsBufferIndex;
    private byte[] valuesBuffer;
    private int valuesBufferIndex;

    public LongArrayBlockEncodingBuffers(int[] positions)
    {
        this.positions = positions;
        prepareBuffers();
    }

    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        nullsBufferIndex = ARRAY_BYTE_BASE_OFFSET;
        valuesBufferIndex = ARRAY_BYTE_BASE_OFFSET;
    }

    @Override
    protected void prepareBuffers()
    {
        // TODO: These local buffers will be requested from the buffer pools in the future.
        if (nullsBuffer == null) {
            nullsBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT / ARRAY_BYTE_INDEX_SCALE];
        }
        nullsBufferIndex = ARRAY_BYTE_BASE_OFFSET;

        if (valuesBuffer == null) {
            valuesBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT * ARRAY_LONG_INDEX_SCALE];
        }
        valuesBufferIndex = ARRAY_BYTE_BASE_OFFSET;
    }

    public void appendFixedWidthValues(Object values, boolean[] nulls, boolean mayHaveNull, int offsetBase)
    {
        long[] longValues = (long[]) values;
        valuesBufferIndex = ByteArrayUtils.putLongValuesToBuffer(longValues, positions, positionsOffset, batchSize, offsetBase, valuesBuffer, valuesBufferIndex);

//        if (!mayHaveNull) {
//            valuesBufferIndex = ByteArrayUtils.putLongValuesToBuffer(longValues, positions, positionsOffset, batchSize, offsetBase, valuesBuffer, valuesBufferIndex);
//        }
//        else {
//            // TODO: Write the version if nulls are present. values array contains the nulls.
//            // Also add the nulls to the buffer.
//            nullsBufferIndex = ByteArrayUtils.encodeNullsAsBits(nulls, positions, positionsOffset, batchSize, nullsBuffer, nullsBufferIndex);
//        }
    }

    public void appendNulls(boolean mayHaveNull, boolean[] nulls, int offsetBase)
    {
        //TODO: ensure nullsBuffer has enough space for these rows.
        if (mayHaveNull) {
            nullsBufferIndex = ByteArrayUtils.encodeNullsAsBits(nulls, positions, positionsOffset, batchSize, nullsBuffer, nullsBufferIndex);
        }
    }

    public void appendOffsets(int[] offsets, int offsetBase)
    {
        throw new UnsupportedOperationException(getClass().getName() + " doesn't support appendOffsets");
    }

    public void appendBlock(Block block, int offsetBase)
    {
        throw new UnsupportedOperationException(getClass().getName() + " doesn't support appendBlock");
    }

    public void writeTo(SliceOutput sliceOutput)
    {
       // System.out.println("Writing encoding Name " + NAME + " at " + sliceOutput.size());
        writeLengthPrefixedString(sliceOutput, NAME);
       // System.out.println("Writing bufferedPositionCount(positionCount) " + bufferedPositionCount + " at " + sliceOutput.size());
        sliceOutput.writeInt(bufferedPositionCount);

       // System.out.println("Writing nullsBuffer at " + sliceOutput.size());
        // TODO: When the buffers are requested from buffer pool, they would be linked lists of buffers, then we need to copy them one by one to sliceOutput.
        if (nullsBufferIndex > ARRAY_BYTE_BASE_OFFSET) {
            sliceOutput.writeBoolean(true);
            sliceOutput.appendBytes(nullsBuffer, 0, nullsBufferIndex - ARRAY_BYTE_BASE_OFFSET);
        }
        else {
            sliceOutput.writeBoolean(false);
        }

       // System.out.println("Writing valuesBuffer at " + sliceOutput.size());
        sliceOutput.appendBytes(valuesBuffer, 0, valuesBufferIndex - ARRAY_BYTE_BASE_OFFSET);
       // System.out.println("Writing Block finishes at " + sliceOutput.size());
    }

    public int getSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +  // NAME
                SIZE_OF_INT +  // positionCount
                max(nullsBufferIndex - ARRAY_BYTE_BASE_OFFSET, 0) + 1 + // nulls uses 1 byte for mayHaveNull
                max(valuesBufferIndex - ARRAY_BYTE_BASE_OFFSET, 0);  // valuesBuffer
    }
}
