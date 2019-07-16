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

import com.facebook.presto.spi.block.ColumnarRow;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.UncheckedByteArrays.setInt;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class RowBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    @VisibleForTesting
    public static final int POSITION_SIZE = SIZE_OF_INT + SIZE_OF_BYTE;

    private static final String NAME = "ROW";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowBlockEncodingBuffers.class).instanceSize();

    // The buffer for the offsets for all incoming blocks so far
    private byte[] offsetsBuffer;

    // The address next offset value will be written to.
    private int offsetsBufferIndex;

    // This array holds the condensed offsets for each position for the incoming block.
    private int[] offsets;

    // The last offset in the offsets buffer
    private int lastOffset;

    // This array holds the offsets into its nested raw block for each top level row.
    private int[][] offsetsOffsets;

    // The current incoming RowBlock is converted into ColumnarRow
    private ColumnarRow columnarRow;

    private final BlockEncodingBuffers[] fieldBuffers;

    public RowBlockEncodingBuffers(DecodedBlockNode decodedBlockNode, int initialPositionCount)
    {
        super(initialPositionCount);

        List<DecodedBlockNode> childrenNodes = decodedBlockNode.getChildren();
        fieldBuffers = new BlockEncodingBuffers[childrenNodes.size()];
        for (int i = 0; i < childrenNodes.size(); i++) {
            fieldBuffers[i] = createBlockEncodingBuffers(decodedBlockNode.getChildren().get(i), initialPositionCount);
        }

        prepareBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        offsetsBuffer = new byte[initialPositionCount * ARRAY_INT_INDEX_SCALE];
        offsetsBufferIndex = 0;

        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].prepareBuffers();
        }
    }

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        offsetsBufferIndex = 0;
        lastOffset = 0;
        resetNullsBuffer();

        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].resetBuffers();
        }
    }

    @Override
    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        decodedBlockNode = mapPositions(decodedBlockNode);
        columnarRow = (ColumnarRow) decodedBlockNode.getDecodedBlock();
        decodedBlock = columnarRow.getNullCheckBlock();

        populateNestedPositions();

        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].setupDecodedBlockAndMapPositions(decodedBlockNode.getChildren().get(i));
        }
    }

    @Override
    public void accumulateRowSizes(int[] rowSizes)
    {
        for (int i = 0; i < positionCount; i++) {
            rowSizes[i] += POSITION_SIZE;
        }

        ensureOffsetsOffsetsCapacity(positionCount);

        for (int i = 0; i < fieldBuffers.length; i++) {
            // Nested level BlockEncodingBuffers will modify the offsetsOffsets array in place,
            // so we need to make a copy for each fieldBuffers.
            System.arraycopy(offsets, 0, offsetsOffsets[i], 0, positionCount + 1);
            fieldBuffers[i].accumulateRowSizes(offsetsOffsets[i], positionCount, rowSizes);
        }
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
            offsetsOffsets[i + 1] = this.offsets[offset];
        }

        ensureOffsetsOffsetsCapacity(positionCount);
        for (int i = 0; i < fieldBuffers.length; i++) {
            // Nested level BlockEncodingBuffers will modify the offsetsOffsets array in place,
            // so we need to make a copy for each fieldBuffers.
            System.arraycopy(offsetsOffsets, 0, this.offsetsOffsets[i], 0, positionCount + 1);
            fieldBuffers[i].accumulateRowSizes(this.offsetsOffsets[i], positionCount, rowSizes);
        }
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

        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].setNextBatch(offset, offsets[positionsOffset + batchSize] - offset);
        }
    }

    @Override
    public void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        appendNulls();
        appendOffsets();

        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].copyValues();
        }

        bufferedPositionCount += batchSize;
    }

    public void serializeTo(SliceOutput sliceOutput)
    {
        writeLengthPrefixedString(sliceOutput, NAME);

        sliceOutput.writeInt(fieldBuffers.length);
        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].serializeTo(sliceOutput);
        }

        sliceOutput.writeInt(bufferedPositionCount); //positionCount
        sliceOutput.writeInt(0);  // the base position
        sliceOutput.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);

        serializeNullsTo(sliceOutput);
    }

    @Override
    public long getSizeInBytes()
    {
        int fieldsSize = 0;
        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldsSize += fieldBuffers[i].getSizeInBytes();
        }

        return getPositionsSizeInBytes() +                          // positions and mappedPositions
                offsetsBufferIndex +
                positionCount * SIZE_OF_INT +                       // offsets
                positionCount * fieldBuffers.length * SIZE_OF_INT + // offsetsOffsets
                getNullsBufferSizeInBytes() +
                fieldsSize +
                columnarRow.getSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        int fieldsRetainedSize = 0;
        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldsRetainedSize += fieldBuffers[i].getRetainedSizeInBytes();
        }

        return INSTANCE_SIZE +
                getPostionsRetainedSizeInBytes() +
                sizeOf(offsetsBuffer) +
                sizeOf(offsets) +
                sizeOf(offsetsOffsets) +
                getNullsBufferRetainedSizeInBytes() +
                fieldsRetainedSize +
                columnarRow.getRetainedSizeInBytes();
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        int fieldsSerializedSize = 0;
        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldsSerializedSize += fieldBuffers[i].getSerializedSizeInBytes();
        }

        return NAME.length() + SIZE_OF_INT +            // encoding name
                SIZE_OF_INT +                           // field count
                fieldsSerializedSize +                  // field blocks
                SIZE_OF_INT +                           // positionCount
                SIZE_OF_INT +                           // offset 0. The offsetsBuffer doesn't contain the offset 0 so we need to add it here.
                offsetsBufferIndex +                    // offsets buffer.
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    private void populateNestedPositions()
    {
        // Nested level positions always start from 0.
        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].positionsOffset = 0;
            fieldBuffers[i].positionCount = 0;
            fieldBuffers[i].isPositionsMapped = false;
        }

        if (positionCount == 0) {
            return;
        }

        ensureOffsetsCapacity();

        int[] positions = getPositions();

        int columnarArrayBaseOffset = columnarRow.getOffset(0);
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            int beginOffset = columnarRow.getOffset(position);
            int endOffset = columnarRow.getOffset(position + 1);  // if the row is null, endOffsetInBlock == beginOffsetInBlock
            int currentRowSize = endOffset - beginOffset;
            offsets[i + 1] = offsets[i] + currentRowSize;

            if (currentRowSize > 0) {
                for (int j = 0; j < fieldBuffers.length; j++) {
                    fieldBuffers[j].appendPositionRange(beginOffset - columnarArrayBaseOffset, currentRowSize);
                }
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

    private void ensureOffsetsOffsetsCapacity(int positionCount)
    {
        if (offsetsOffsets == null || offsetsOffsets[0].length < positionCount + 1) {
            offsetsOffsets = new int[fieldBuffers.length][positionCount * 2];
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
