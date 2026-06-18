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
package com.facebook.presto.operator.repartition;

import com.facebook.presto.common.block.ArrayAllocator;
import com.facebook.presto.common.block.ColumnarRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects.ToStringHelper;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.LARGE;
import static com.facebook.presto.common.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.NONE;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.UncheckedByteArrays.setIntUnchecked;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class RowBlockEncodingBuffer
        extends AbstractBlockEncodingBuffer
{
    @VisibleForTesting
    static final int POSITION_SIZE = SIZE_OF_INT + SIZE_OF_BYTE;

    private static final String NAME = "ROW";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowBlockEncodingBuffer.class).instanceSize();

    // The buffer for the offsets for all incoming blocks so far
    private byte[] offsetsBuffer;

    // The address next offset value will be written to.
    private int offsetsBufferIndex;

    // The estimated maximum size for offsetsBuffer
    private int estimatedOffsetBufferMaxCapacity;

    // This array holds the condensed offsets for each position for the incoming block.
    private int[] offsets;

    // The last offset in the offsets buffer
    private int lastOffset;

    // The AbstractBlockEncodingBuffer for the nested field blocks of the RowBlock
    private final AbstractBlockEncodingBuffer[] fieldBuffers;

    public RowBlockEncodingBuffer(DecodedBlockNode decodedBlockNode, ArrayAllocator bufferAllocator, boolean isNested)
    {
        super(bufferAllocator, isNested);

        List<DecodedBlockNode> childrenNodes = decodedBlockNode.getChildren();
        fieldBuffers = new AbstractBlockEncodingBuffer[childrenNodes.size()];
        for (int i = 0; i < childrenNodes.size(); i++) {
            fieldBuffers[i] = (AbstractBlockEncodingBuffer) createBlockEncodingBuffers(decodedBlockNode.getChildren().get(i), bufferAllocator, true);
        }
    }

    @Override
    public void accumulateSerializedRowSizes(int[] serializedRowSizes)
    {
        for (int i = 0; i < positionCount; i++) {
            serializedRowSizes[i] += POSITION_SIZE;
        }

        int[] offsetsCopy = ensureCapacity(null, positionCount + 1, SMALL, NONE, bufferAllocator);
        try {
            for (int i = 0; i < fieldBuffers.length; i++) {
                System.arraycopy(offsets, 0, offsetsCopy, 0, positionCount + 1);
                ((AbstractBlockEncodingBuffer) fieldBuffers[i]).accumulateSerializedRowSizes(offsetsCopy, positionCount, serializedRowSizes);
            }
        }
        finally {
            bufferAllocator.returnArray(offsetsCopy);
        }
    }

    @Override
    public void setNextBatch(int positionsOffset, int batchSize)
    {
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;
        this.flushed = false;

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
    public void appendDataInBatch()
    {
        if (batchSize == 0) {
            return;
        }

        appendNulls();
        appendOffsets();

        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].appendDataInBatch();
        }

        bufferedPositionCount += batchSize;
    }

    @Override
    public void serializeTo(SliceOutput output)
    {
        writeLengthPrefixedString(output, NAME);

        output.writeInt(fieldBuffers.length);
        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].serializeTo(output);
        }

        // positionCount
        output.writeInt(bufferedPositionCount);

        // offsets
        output.writeInt(0);  // the base position
        if (offsetsBufferIndex > 0) {
            output.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);
        }

        serializeNullsTo(output);
    }

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        offsetsBufferIndex = 0;
        lastOffset = 0;
        flushed = true;
        resetNullsBuffer();

        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].resetBuffers();
        }
    }

    @Override
    public void noMoreBatches()
    {
        for (int i = fieldBuffers.length - 1; i >= 0; i--) {
            fieldBuffers[i].noMoreBatches();
        }

        if (flushed && offsetsBuffer != null) {
            bufferAllocator.returnArray(offsetsBuffer);
            offsetsBuffer = null;
        }

        super.noMoreBatches();

        if (offsets != null) {
            bufferAllocator.returnArray(offsets);
            offsets = null;
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        int size = INSTANCE_SIZE;
        for (int i = 0; i < fieldBuffers.length; i++) {
            size += fieldBuffers[i].getRetainedSizeInBytes();
        }

        return size;
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        int size = 0;
        for (int i = 0; i < fieldBuffers.length; i++) {
            size += fieldBuffers[i].getSerializedSizeInBytes();
        }

        return NAME.length() + SIZE_OF_INT +            // encoding name
                SIZE_OF_INT +                           // field count
                size +                                  // field blocks
                SIZE_OF_INT +                           // positionCount
                SIZE_OF_INT +                           // offset 0. The offsetsBuffer doesn't contain the offset 0 so we need to add it here.
                offsetsBufferIndex +                    // offsets buffer.
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    @Override
    public String toString()
    {
        ToStringHelper stringHelper = toStringHelper(this)
                .add("super", super.toString())
                .add("estimatedOffsetBufferMaxCapacity", estimatedOffsetBufferMaxCapacity)
                .add("offsetsBufferCapacity", offsetsBuffer == null ? 0 : offsetsBuffer.length)
                .add("offsetsBufferIndex", offsetsBufferIndex)
                .add("offsetsCapacity", offsets == null ? 0 : offsets.length)
                .add("lastOffset", lastOffset);

        for (int i = 0; i < fieldBuffers.length; i++) {
            stringHelper.add("fieldBuffer_" + i, fieldBuffers[i]);
        }

        return stringHelper.toString();
    }

    @Override
    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode, int partitionBufferCapacity, double decodedBlockPageSizeFraction)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        decodedBlockNode = mapPositionsToNestedBlock(decodedBlockNode);
        ColumnarRow columnarRow = (ColumnarRow) decodedBlockNode.getDecodedBlock();
        decodedBlock = columnarRow.getNullCheckBlock();

        populateNestedPositions(columnarRow);

        long estimatedSerializedSizeInBytes = decodedBlockNode.getEstimatedSerializedSizeInBytes();
        long childrenEstimatedSerializedSizeInBytes = 0;

        for (int i = 0; i < fieldBuffers.length; i++) {
            DecodedBlockNode childDecodedBlockNode = decodedBlockNode.getChildren().get(i);
            long childEstimatedSerializedSizeInBytes = childDecodedBlockNode.getEstimatedSerializedSizeInBytes();
            childrenEstimatedSerializedSizeInBytes += childEstimatedSerializedSizeInBytes;
            fieldBuffers[i].setupDecodedBlockAndMapPositions(childDecodedBlockNode, partitionBufferCapacity, decodedBlockPageSizeFraction * childEstimatedSerializedSizeInBytes / estimatedSerializedSizeInBytes);
        }

        double targetBufferSize = partitionBufferCapacity * decodedBlockPageSizeFraction * (estimatedSerializedSizeInBytes - childrenEstimatedSerializedSizeInBytes) / estimatedSerializedSizeInBytes;

        setEstimatedNullsBufferMaxCapacity(getEstimatedBufferMaxCapacity(targetBufferSize, Byte.BYTES, POSITION_SIZE));
        estimatedOffsetBufferMaxCapacity = getEstimatedBufferMaxCapacity(targetBufferSize, Integer.BYTES, POSITION_SIZE);
    }

    @Override
    protected void accumulateSerializedRowSizes(int[] positionOffsets, int positionCount, int[] serializedRowSizes)
    {
        // If all positions for the RowBlock to be copied are null, the number of positions to copy for its
        // nested field blocks could be 0. In such case we don't need to proceed.
        if (this.positionCount == 0) {
            return;
        }

        int lastOffset = positionOffsets[0];
        for (int i = 0; i < positionCount; i++) {
            int offset = positionOffsets[i + 1];
            serializedRowSizes[i] += POSITION_SIZE * (offset - lastOffset);
            lastOffset = offset;
            positionOffsets[i + 1] = this.offsets[offset];
        }

        int[] offsetsCopy = ensureCapacity(null, positionCount + 1, SMALL, NONE, bufferAllocator);
        try {
            for (int i = 0; i < fieldBuffers.length; i++) {
                System.arraycopy(positionOffsets, 0, offsetsCopy, 0, positionCount + 1);
                fieldBuffers[i].accumulateSerializedRowSizes(offsetsCopy, positionCount, serializedRowSizes);
            }
        }
        finally {
            bufferAllocator.returnArray(offsetsCopy);
        }
    }

    @Override
    int getEstimatedValueBufferMaxCapacity()
    {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    int getEstimatedOffsetBufferMaxCapacity()
    {
        return estimatedOffsetBufferMaxCapacity;
    }

    @VisibleForTesting
    BlockEncodingBuffer[] getFieldBuffers()
    {
        return fieldBuffers;
    }

    private void populateNestedPositions(ColumnarRow columnarRow)
    {
        // Reset nested level positions before checking positionCount. Failing to do so may result in elementsBuffers having stale values when positionCount is 0.
        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].resetPositions();
        }

        if (positionCount == 0) {
            return;
        }

        offsets = ensureCapacity(offsets, positionCount + 1, SMALL, NONE, bufferAllocator);
        offsets[0] = 0;

        int[] positions = getPositions();

        int columnarRowBaseOffset = columnarRow.getOffset(0);
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            offsets[i + 1] = offsets[i] + columnarRow.getOffset(position + 1) - columnarRow.getOffset(position);
        }

        for (int j = 0; j < fieldBuffers.length; j++) {
            fieldBuffers[j].ensurePositionsCapacity(offsets[positionCount]);
        }

        for (int i = 0; i < positionCount; i++) {
            int beginOffset = columnarRow.getOffset(positions[i]);
            int currentRowSize = offsets[i + 1] - offsets[i];

            if (currentRowSize > 0) {
                for (int j = 0; j < fieldBuffers.length; j++) {
                    fieldBuffers[j].appendPositionRange(beginOffset - columnarRowBaseOffset, currentRowSize);
                }
            }
        }
    }

    private void appendOffsets()
    {
        offsetsBuffer = ensureCapacity(offsetsBuffer, offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE, estimatedOffsetBufferMaxCapacity, LARGE, PRESERVE, bufferAllocator);

        int baseOffset = lastOffset - offsets[positionsOffset];
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            offsetsBufferIndex = setIntUnchecked(offsetsBuffer, offsetsBufferIndex, offsets[i + 1] + baseOffset);
        }
        lastOffset = offsets[positionsOffset + batchSize] + baseOffset;
    }
}
