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
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.operator.UncheckedByteArrays.setBytes;
import static com.facebook.presto.operator.UncheckedByteArrays.setInt;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class VariableWidthBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    @VisibleForTesting
    public static final int POSITION_SIZE = Integer.BYTES + Byte.BYTES;

    private static final String NAME = "VARIABLE_WIDTH";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlockEncodingBuffers.class).instanceSize();

    // The buffer for the slice for all incoming blocks so far
    private byte[] sliceBuffer;

    // The address that the next slice will be written to.
    private int sliceBufferIndex;

    // The buffer for the offsets for all incoming blocks so far
    private byte[] offsetsBuffer;

    // The address that the next offset value will be written to.
    private int offsetsBufferIndex;

    // The last offset in the offsets buffer
    private int lastOffset;

    public VariableWidthBlockEncodingBuffers(int initialPositionCount)
    {
        super(initialPositionCount);
        prepareBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        sliceBuffer = new byte[initialPositionCount * ARRAY_LONG_INDEX_SCALE * 2];
        sliceBufferIndex = 0;

        offsetsBuffer = new byte[initialPositionCount * ARRAY_INT_INDEX_SCALE];
        offsetsBufferIndex = 0;
    }

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        sliceBufferIndex = 0;
        offsetsBufferIndex = 0;
        lastOffset = 0;
        resetNullsBuffer();
    }

    @Override
    public void accumulateRowSizes(int[] rowSizes)
    {
        int[] positions = getPositions();
        for (int i = 0; i < positionCount; i++) {
            rowSizes[i] += decodedBlock.getSliceLength(positions[i]);
            rowSizes[i] += POSITION_SIZE;
        }
    }

    @Override
    protected void accumulateRowSizes(int[] offsetsOffsets, int positionCount, int[] rowSizes)
    {
        // The nested level positionCount could be 0.
        if (this.positionCount == 0) {
            return;
        }

        int[] positions = getPositions();

        for (int i = 0; i < positionCount; i++) {
            int length = 0;

            int positionOffset = offsetsOffsets[i];
            int entryCount = offsetsOffsets[i + 1] - positionOffset;
            for (int j = 0; j < entryCount; j++) {
                length += decodedBlock.getSliceLength(positions[positionOffset++]);
                length += POSITION_SIZE;
            }

            rowSizes[i] += length;
        }
    }

    @Override
    public void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        appendOffsetsAndSlices();
        appendNulls();
        bufferedPositionCount += batchSize;
    }

    public void serializeTo(SliceOutput sliceOutput)
    {
        writeLengthPrefixedString(sliceOutput, NAME);

        sliceOutput.writeInt(bufferedPositionCount);

        // offsets
        // note that VariableWidthBlock doesn't write the initial offset 0
        sliceOutput.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);

        // nulls
        serializeNullsTo(sliceOutput);
        sliceOutput.writeInt(sliceBufferIndex);  // totalLength
        sliceOutput.appendBytes(sliceBuffer, 0, sliceBufferIndex);
    }

    @Override
    public long getSizeInBytes()
    {
        return getPositionsSizeInBytes() + // positions and mappedPositions
                offsetsBufferIndex +
                getNullsBufferSizeInBytes() +
                sliceBufferIndex;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                getPostionsRetainedSizeInBytes() +
                sizeOf(offsetsBuffer) +
                getNullsBufferRetainedSizeInBytes() +
                sizeOf(sliceBuffer);
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +    // NAME
                SIZE_OF_INT +                   // positionCount
                offsetsBufferIndex +            // offsets buffer.
                SIZE_OF_INT +                   // sliceBuffer size.
                sliceBufferIndex +               // sliceBuffer
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    // This implementation requires variableWidthBlock.getPositionOffset() to be public but it's much faster than the below one
    private void appendOffsetsAndSlices()
    {
        ensureOffsetsBufferCapacity();

        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) decodedBlock;
        int[] positions = getPositions();

        // Get the whole slice then access specific rows to allow faster copy
        int sliceLength = variableWidthBlock.getPositionOffset(variableWidthBlock.getPositionCount()) - variableWidthBlock.getPositionOffset(0);

        byte[] sliceBase = (byte[]) variableWidthBlock.getSlice(0, 0, sliceLength).getBase();

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            int beginOffset = variableWidthBlock.getPositionOffset(position);
            int endOffset = variableWidthBlock.getPositionOffset(position + 1);
            int currentRowSize = endOffset - beginOffset;

            lastOffset += currentRowSize;

            offsetsBufferIndex = setInt(offsetsBuffer, offsetsBufferIndex, lastOffset);

            ensureSliceBufferCapacity(currentRowSize);

            if (currentRowSize > 0) {
                sliceBufferIndex = setBytes(sliceBuffer, sliceBufferIndex, sliceBase, beginOffset, currentRowSize);
            }
        }
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
//        ensureOffsetsBufferCapacity();
//        int[] positions = getPositions();
//
//        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) decodedBlock;
//        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
//            int position = positions[i];
//            int currentRowSize = variableWidthBlock.getSliceLength(position);
//            int currentOffset = lastOffset + currentRowSize;
//
//            ByteArrayUtils.setInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
//            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;
//
//            //// // System.out.println("positionsOffset " + positionsOffset + " batchSize " + batchSize + " i " + i +
//            //      " position " + position + " sliceBufferIndex " + sliceBufferIndex + " slice.length " + currentRowSize);
//
//            ensureSliceBufferCapacity(currentRowSize);
//            sliceBufferIndex = ByteArrayUtils.writeSlice(sliceBuffer, sliceBufferIndex, variableWidthBlock.getSlice(position, 0, currentRowSize));
//
//            lastOffset = currentOffset;
//        }
//    }

    private void ensureSliceBufferCapacity(int currentRowSize)
    {
        int capacity = sliceBufferIndex + currentRowSize;
        if (capacity > sliceBuffer.length) {
            sliceBuffer = Arrays.copyOf(sliceBuffer, max(sliceBuffer.length * 2, capacity));
        }
    }

    private void ensureOffsetsBufferCapacity()
    {
        int capacity = offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE;
        if (offsetsBuffer.length < capacity) {
            offsetsBuffer = Arrays.copyOf(offsetsBuffer, max(capacity, offsetsBuffer.length * 2));
        }
    }
}
