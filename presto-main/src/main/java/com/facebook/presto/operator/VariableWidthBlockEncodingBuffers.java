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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.AbstractVariableWidthBlock;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;

import static com.facebook.presto.operator.ByteArrayUtils.writeLengthPrefixedString;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class VariableWidthBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    public static final String NAME = "VARIABLE_WIDTH";

    private byte[] sliceBuffer;
    private int sliceBufferIndex;

    private byte[] offsetsBuffer;
    private int offsetsBufferIndex;

    VariableWidthBlockEncodingBuffers(int initialBufferSize)
    {
        this.initialBufferSize = initialBufferSize;
        prepareBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        if (sliceBuffer == null) {
            sliceBuffer = new byte[initialBufferSize * ARRAY_LONG_INDEX_SCALE * 2];
        }
        sliceBufferIndex = 0;

        if (offsetsBuffer == null) {
            offsetsBuffer = new byte[initialBufferSize * ARRAY_INT_INDEX_SCALE];
        }
        offsetsBufferIndex = 0;
    }

    @Override
    protected void resetBuffers()
    {
        bufferedPositionCount = 0;
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        nullsBufferContainsNull = false;
        sliceBufferIndex = 0;
        offsetsBufferIndex = 0;
    }

    @Override
    protected void accumulateRowSizes(int[] rowSizes)
    {
        int averageElementSize = Integer.BYTES + Byte.BYTES;

        int[] positions = isPositionsMapped ? mappedPositions : this.positions;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            rowSizes[i] += decodedBlock.getSliceLength(position);
            rowSizes[i] += averageElementSize;
        }
    }

    @Override
    protected void accumulateRowSizes(int[] positionOffsets, int positionCount, int[] rowSizes)
    {
        // The nested level positionCount could be 0.
        if (this.positionCount == 0) {
            return;
        }

        int averageElementSize = Integer.BYTES + Byte.BYTES;

        int[] positions = isPositionsMapped ? mappedPositions : this.positions;

        int largestPosition = positionOffsets[positionCount - 1];
        verify(positions.length >= largestPosition);
        verify(decodedBlock.getPositionCount() >= largestPosition);

        int lastOffset = 0;
        for (int i = 0; i < positionCount; i++) {
            int currentOffset = positionOffsets[i];
            int entryCount = currentOffset - lastOffset;

            int entrySize = 0;
            for (int j = 0; j < entryCount; j++) {
                int position = positions[lastOffset + j];
                entrySize += decodedBlock.getSliceLength(position);
            }

            rowSizes[i] += (averageElementSize + entrySize);

            lastOffset = currentOffset;
        }
    }

    @Override
    protected void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        int[] positions = isPositionsMapped ? mappedPositions : this.positions;

        verify(positionsOffset + batchSize - 1 < positions.length && positions[positionsOffset] >= 0 &&
                positions[positionsOffset + batchSize - 1] < decodedBlock.getPositionCount());

        appendOffsetsAndSlices();
        appendNulls();
        bufferedPositionCount += batchSize;
    }

    @Override
    protected void writeTo(SliceOutput sliceOutput)
    {
        // TODO: getSizeInBytes() was calculated in flush and doesnt need to recalculated
        verify(getSizeInBytes() <= sliceOutput.writableBytes());

        writeLengthPrefixedString(sliceOutput, NAME);
        sliceOutput.writeInt(bufferedPositionCount);

        // offsets
        // note that VariableWidthBlock doesn't write the initial offset 0
        sliceOutput.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);
        // nulls
        writeNullsTo(sliceOutput);
        sliceOutput.writeInt(sliceBufferIndex);  // totalLength
        sliceOutput.appendBytes(sliceBuffer, 0, sliceBufferIndex);
    }

    @Override
    protected int getSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +    // NAME
                SIZE_OF_INT +                   // positionCount
                offsetsBufferIndex +            // offsets buffer.
                SIZE_OF_BYTE +                  // nulls uses 1 byte for mayHaveNull
                nullsBufferIndex +              // nulls buffer
                (remainingNullsCount > 0 ? SIZE_OF_BYTE : 0) +  // the remaining nulls not serialized yet
                SIZE_OF_INT +                   // sliceBuffer size.
                sliceBufferIndex;               // sliceBuffer
    }

    // This implementation requires variableWidthBlock.getPositionOffset() to be public but it's much faster than the below one
    private void appendOffsetsAndSlices()
    {
        verify(offsetsBufferIndex == bufferedPositionCount * ARRAY_INT_INDEX_SCALE);

        int lastOffset = 0;
        if (offsetsBufferIndex > 0) {
            // There're already some values in the buffer
            lastOffset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
        }

        ensureOffsetsBufferSize();

        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) decodedBlock;
        int[] positions = isPositionsMapped ? mappedPositions : this.positions;

        // Get the whole slice then access specific rows to allow faster copy
        int sliceLength = variableWidthBlock.getPositionOffset(variableWidthBlock.getPositionCount()) - variableWidthBlock.getPositionOffset(0);
        byte[] sliceBase = (byte[]) variableWidthBlock.getSlice(0, 0, sliceLength).getBase();

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            int beginOffsetInBlock = variableWidthBlock.getPositionOffset(position);
            int endOffsetInBlock = variableWidthBlock.getPositionOffset(position + 1);
            int currentRowSize = endOffsetInBlock - beginOffsetInBlock;
            int currentOffset = lastOffset + currentRowSize;

            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;

            ensureSliceBufferSize(currentRowSize);

            sliceBufferIndex = ByteArrayUtils.copyBytes(sliceBuffer, sliceBufferIndex, sliceBase, beginOffsetInBlock, currentRowSize);

            lastOffset = currentOffset;
        }

        verify(offsetsBufferIndex == (bufferedPositionCount + batchSize) * ARRAY_INT_INDEX_SCALE);
        verify(getLastOffsetInBuffer() == sliceBufferIndex);
    }

    // This implementation doesn't need to expose variableWidthBlock.getPositionOffset() but it's too slow, because it's calling getSlice()
    // for every row and has too much overhead in constructing the Slice object
//    private void appendOffsetsAndSlices()
//    {
//        verify(offsetsBufferIndex == bufferedPositionCount * ARRAY_INT_INDEX_SCALE);
//        // format("offsetsBufferIndex %d should equal to bufferedPositionCount %d * ARRAY_INT_INDEX_SCALE %d.", offsetsBufferIndex, bufferedPositionCount, ARRAY_INT_INDEX_SCALE));
//
//        int lastOffset = 0;
//        if (offsetsBufferIndex > 0) {
//            // There're already some values in the buffer
//            //// System.out.println(offsetsBufferIndex);
//            try {
//                lastOffset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        ensureOffsetsBufferSize();
//        int[] positions = isPositionsMapped ? mappedPositions : this.positions;
//
//        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) decodedBlock;
//        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
//            int position = positions[i];
//            int currentRowSize = variableWidthBlock.getSliceLength(position);
//            int currentOffset = lastOffset + currentRowSize;
//
//            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
//            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;
//
//            //// // System.out.println("positionsOffset " + positionsOffset + " batchSize " + batchSize + " i " + i +
//            //      " position " + position + " sliceBufferIndex " + sliceBufferIndex + " slice.length " + currentRowSize);
//
//            ensureSliceBufferSize(currentRowSize);
//            sliceBufferIndex = ByteArrayUtils.writeSlice(sliceBuffer, sliceBufferIndex, variableWidthBlock.getSlice(position, 0, currentRowSize));
//
//            lastOffset = currentOffset;
//        }
//    }

    private int getLastOffsetInBuffer()
    {
        int offset = 0;
        if (offsetsBufferIndex > 0) {
            // There're already some values in the buffer
            offset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
        }
        return offset;
    }

    private void ensureSliceBufferSize(int currentRowSize)
    {
        int requiredSize = sliceBufferIndex + currentRowSize;
        if (requiredSize > sliceBuffer.length) {
            sliceBuffer = Arrays.copyOf(sliceBuffer, max(sliceBuffer.length * 2, requiredSize));
        }
    }

    private void ensureOffsetsBufferSize()
    {
        int requiredSize = offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE;
        if (requiredSize > offsetsBuffer.length) {
            offsetsBuffer = Arrays.copyOf(offsetsBuffer, max(requiredSize, offsetsBuffer.length * 2));
        }
    }
}
