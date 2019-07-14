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

import com.facebook.presto.spi.block.ColumnarArray;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.operator.UncheckedByteArrays.setInt;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class ArrayBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    @VisibleForTesting
    public static final int POSITION_SIZE = SIZE_OF_INT + SIZE_OF_BYTE;

    private static final String NAME = "ARRAY";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBlockEncodingBuffers.class).instanceSize();

    // The buffer for the offsets for all incoming blocks so far
    private byte[] offsetsBuffer;

    // The address that the next offset value will be written to.
    private int offsetsBufferIndex;

    // This array holds the condensed offsets for each position for the incoming block.
    private int[] offsets;

    // The last offset in the offsets buffer
    private int lastOffset;

    // This array holds the offsets into its nested raw block for each top level row.
    private int[] offsetsOffsets;

    // The current incoming ArrayBlock is converted into ColumnarArray
    private ColumnarArray columnarArray;

    private final BlockEncodingBuffers elementsBuffers;

    public ArrayBlockEncodingBuffers(DecodedBlockNode decodedBlockNode, int initialPositionCount)
    {
        super(initialPositionCount);
        elementsBuffers = createBlockEncodingBuffers(decodedBlockNode.getChildren().get(0), initialPositionCount);
        prepareBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        offsetsBuffer = new byte[initialPositionCount * ARRAY_INT_INDEX_SCALE];
        offsetsBufferIndex = 0;

        elementsBuffers.prepareBuffers();
    }

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        offsetsBufferIndex = 0;
        lastOffset = 0;
        resetNullsBuffer();

        elementsBuffers.resetBuffers();
    }

    @Override
    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        decodedBlockNode = mapPositions(decodedBlockNode);
        columnarArray = (ColumnarArray) decodedBlockNode.getDecodedBlock();
        decodedBlock = columnarArray.getNullCheckBlock();

        populateNestedPositions();

        elementsBuffers.setupDecodedBlockAndMapPositions(decodedBlockNode.getChildren().get(0));
    }

    @Override
    public void accumulateRowSizes(int[] rowSizes)
    {
        for (int i = 0; i < positionCount; i++) {
            rowSizes[i] += POSITION_SIZE;
        }

        ensureOffsetsOffsetsCapacity();
        System.arraycopy(offsets, 0, offsetsOffsets, 0, positionCount + 1);

        elementsBuffers.accumulateRowSizes(offsetsOffsets, positionCount, rowSizes);
    }

    @Override
    protected void accumulateRowSizes(int[] offsetsOffsets, int positionCount, int[] rowSizes)
    {
        // The nested level positionCount could be 0.
        if (this.positionCount == 0) {
            return;
        }

        int lastOffset = offsetsOffsets[0];
        for (int i = 0; i < positionCount; i++) {
            int offset = offsetsOffsets[i + 1];
            rowSizes[i] += POSITION_SIZE * (offset - lastOffset);
            lastOffset = offset;
            offsetsOffsets[i + 1] = offsets[offset];
        }

        // this.offsetsBuffer and Next level positions should have already be written
        elementsBuffers.accumulateRowSizes(offsetsOffsets, positionCount, rowSizes);
    }

    @Override
    public void setNextBatch(int positionsOffset, int batchSize)
    {
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;

        // The nested level positionCount could be 0.
        if (this.positionCount == 0) {
            return;
        }

        int offset = offsets[positionsOffset];

        elementsBuffers.setNextBatch(offset, offsets[positionsOffset + batchSize] - offset);
    }

    @Override
    public void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        appendNulls();
        appendOffsets();
        elementsBuffers.copyValues();

        bufferedPositionCount += batchSize;
    }

    @Override
    public void serializeTo(SliceOutput output)
    {
        writeLengthPrefixedString(output, NAME);

        elementsBuffers.serializeTo(output);

        output.writeInt(bufferedPositionCount);

        output.writeInt(0);  // the base position
        output.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);

        serializeNullsTo(output);
    }

    @Override
    public long getSizeInBytes()
    {
        return getPositionsSizeInBytes() +      // positions and mappedPositions
                offsetsBufferIndex +
                positionCount * SIZE_OF_INT * 2 +   // offsets and offsetsOffsets
                getNullsBufferSizeInBytes() +
                columnarArray.getSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                getPostionsRetainedSizeInBytes() +
                sizeOf(offsetsBuffer) +
                sizeOf(offsets) +
                sizeOf(offsetsOffsets) +
                getNullsBufferRetainedSizeInBytes() +
                columnarArray.getRetainedSizeInBytes();
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +            // encoding name
                elementsBuffers.getSerializedSizeInBytes() +   // nested block
                SIZE_OF_INT +                           // positionCount
                SIZE_OF_INT +                           // offset 0. The offsetsBuffer doesn't contain the offset 0 so we need to add it here.
                offsetsBufferIndex +                    // offsets buffer.
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    private void populateNestedPositions()
    {
        // Nested level positions always start from 0.
        elementsBuffers.positionsOffset = 0;
        elementsBuffers.positionCount = 0;
        elementsBuffers.isPositionsMapped = false;

        if (positionCount == 0) {
            return;
        }

        ensureOffsetsCapacity();

        int[] positions = getPositions();

        int columnarArrayBaseOffset = columnarArray.getOffset(0);
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            int beginOffset = columnarArray.getOffset(position);
            int endOffset = columnarArray.getOffset(position + 1);
            int currentRowSize = endOffset - beginOffset;

            offsets[i + 1] = offsets[i] + currentRowSize;

            if (currentRowSize > 0) {
                // beginOffsetInBlock is the absolute position in the nested block. We need to subtract the base offset from it to get the logical position.
                elementsBuffers.appendPositionRange(beginOffset - columnarArrayBaseOffset, currentRowSize);
            }
        }
    }

    private void appendOffsets()
    {
        ensureOffsetsBufferCapacity();

        int baseOffset = lastOffset - offsets[positionsOffset];
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            offsetsBufferIndex = setInt(offsetsBuffer, offsetsBufferIndex, offsets[i + 1] + baseOffset);
        }
        lastOffset = offsets[positionsOffset + batchSize] + baseOffset;
    }

    private void ensureOffsetsCapacity()
    {
        if (offsets == null || offsets.length < positionCount + 1) {
            offsets = new int[positionCount * 2];
        }
    }

    private void ensureOffsetsOffsetsCapacity()
    {
        if (offsetsOffsets == null || offsetsOffsets.length < positionCount + 1) {
            offsetsOffsets = new int[positionCount * 2];
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
