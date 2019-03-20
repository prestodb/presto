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

import io.airlift.slice.ByteArrays;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.BlockEncodingSerde.createBlockEncodingBuffers;
import static com.facebook.presto.spi.block.ByteArrayUtils.writeLengthPrefixedString;
import static com.facebook.presto.spi.block.ColumnarArray.toColumnarArray;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class ArrayBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    public static final String NAME = ArrayBlockEncoding.NAME;
    public static final int DEFAULT_MAX_ELEMENT_COUNT = 8 * 1024;

    // TODO: These are single piece buffers for now. They will become linked list of pages requested from buffer pool.
    // TODO: we need to decide how we can operate on the buffers. Slice itself doesn't allow appending, and DynamicSliceOutput does capacity check for every value. If we modify DynamicSliceOutput we need to modify Airlift.
    // If we directly operate on byte[] we need to decide the interface. Currently we'll use ByteArrayUtils for updating batch of rows, and ByteArrays in Airlift to update single value
    private byte[] nullsBuffer;
    private int nullsBufferIndex;  //The next byte address if new values to be added.
    private byte[] offsetsBuffer;
    private int offsetsBufferIndex;  //The next byte address if new positions to be added.
    private BlockEncodingBuffers rawBlockBuffer;

    public ArrayBlockEncodingBuffers(Block block, int[] positions)
    {
        this.positions = positions;
        Block rawBlock = toColumnarArray(block).getElementsBlock();
        rawBlockBuffer = createBlockEncodingBuffers(rawBlock, positions);
        prepareBuffers();
    }

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        nullsBufferIndex = ARRAY_BYTE_BASE_OFFSET;
        offsetsBufferIndex = ARRAY_BYTE_BASE_OFFSET;
        rawBlockBuffer.resetBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        // TODO: These local buffers will be requested from the buffer pools in the future.
        if (nullsBuffer == null) {
            nullsBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT / ARRAY_BYTE_INDEX_SCALE];
        }
        nullsBufferIndex = ARRAY_BYTE_BASE_OFFSET;

        if (offsetsBuffer == null) {
            offsetsBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT * ARRAY_LONG_INDEX_SCALE];
        }
        offsetsBufferIndex = ARRAY_BYTE_BASE_OFFSET;

        rawBlockBuffer.prepareBuffers();
    }

    public void appendFixedWidthValues(Object values, boolean[] nulls, boolean mayHaveNull, int offsetBase)
    {
        throw new UnsupportedOperationException(getClass().getName() + " doesn't support appendLongValues");
    }

    public void appendNulls(boolean mayHaveNull, boolean[] nulls, int offsetBase)
    {
        //TODO: ensure valuesBuffer has enough space for these rows. We will make it to request a new buffer and append to the end of current buffer if the current buffer has no enough room
        // Also needs to evaluate the performance using DynamicSliceInput vs Slice vs byte[]
        if (mayHaveNull) {
            nullsBufferIndex = ByteArrayUtils.encodeNullsAsBits(nulls, positions, positionsOffset, batchSize, nullsBuffer, nullsBufferIndex);
        }
    }

    public void appendOffsets(int[] offsets, int offsetBase)
    {
        // TODO: validate offsets size
        int lastOffset = 0;
        if (offsetsBufferIndex >= ARRAY_BYTE_BASE_OFFSET + ARRAY_INT_INDEX_SCALE) {
            // There're already some values in the buffer
            int lastOffsetIndex = offsetsBufferIndex - ARRAY_INT_INDEX_SCALE;
            lastOffset = ByteArrays.getInt(offsetsBuffer, lastOffsetIndex);
        }

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            // Append the new positions value to the buffer
            int position = positions[i] + offsetBase;  // positions are the logical row numbers. The real index of position 0 into the offsets array shall be offsetBase

            // TODO: optimize this
            int currentRowSize = offsets[position + 1] - offsets[position];
            int currentOffset = lastOffset + currentRowSize;

            ByteArrayUtils.setInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;

            // One row correspond to a range of rows of the next level. Add these row positions to the next level positions
            rawBlockBuffer.appendPositionRange(offsets[position], currentRowSize);

            lastOffset = currentOffset;
        }
    }

    public void appendBlock(Block block, int offsetBase)
    {
        block.writeTo(rawBlockBuffer);
    }

    public void writeTo(SliceOutput sliceOutput)
    {
        writeLengthPrefixedString(sliceOutput, NAME);
        //

        // TODO: When the buffers are requested from buffer pool, they would be linked lists of buffers, then we need to copy them one by one to sliceOutput.
        rawBlockBuffer.writeTo(sliceOutput);

        sliceOutput.writeInt(bufferedPositionCount); //positionCount

        sliceOutput.writeInt(0);  // the base position
        sliceOutput.appendBytes(offsetsBuffer, 0, offsetsBufferIndex - ARRAY_BYTE_BASE_OFFSET);

        if (nullsBufferIndex > ARRAY_BYTE_BASE_OFFSET) {
            sliceOutput.writeBoolean(true);
            sliceOutput.appendBytes(nullsBuffer, 0, nullsBufferIndex - ARRAY_BYTE_BASE_OFFSET);
        }
        else {
            sliceOutput.writeBoolean(false);
        }
    }

    public int getSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +  // encoding name
                rawBlockBuffer.getSizeInBytes() +
                SIZE_OF_INT +  // positionCount
                SIZE_OF_INT + max(offsetsBufferIndex - ARRAY_BYTE_BASE_OFFSET, 0) + // offsets
                SIZE_OF_BYTE + max(nullsBufferIndex - ARRAY_BYTE_BASE_OFFSET, 0) + 1; //nulls
    }
}
