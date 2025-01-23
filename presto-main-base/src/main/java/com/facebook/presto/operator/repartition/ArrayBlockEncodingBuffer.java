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
import com.facebook.presto.common.block.ColumnarArray;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

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

public class ArrayBlockEncodingBuffer
        extends AbstractBlockEncodingBuffer
{
    @VisibleForTesting
    static final int POSITION_SIZE = SIZE_OF_INT + SIZE_OF_BYTE;

    private static final String NAME = "ARRAY";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBlockEncodingBuffer.class).instanceSize();

    // The buffer for the offsets for all incoming blocks so far
    private byte[] offsetsBuffer;

    // The address that the next offset value will be written to.
    private int offsetsBufferIndex;

    // The estimated maximum size for offsetsBuffer
    private int estimatedOffsetBufferMaxCapacity;

    // This array holds the condensed offsets for each position for the incoming block.
    private int[] offsets;

    // The last offset in the offsets buffer
    private int lastOffset;

    // The AbstractBlockEncodingBuffer for the nested values Block of the ArrayBlock
    private final AbstractBlockEncodingBuffer valuesBuffers;

    public ArrayBlockEncodingBuffer(DecodedBlockNode decodedBlockNode, ArrayAllocator bufferAllocator, boolean isNested)
    {
        super(bufferAllocator, isNested);
        valuesBuffers = (AbstractBlockEncodingBuffer) createBlockEncodingBuffers(decodedBlockNode.getChildren().get(0), bufferAllocator, true);
    }

    @Override
    public void accumulateSerializedRowSizes(int[] serializedRowSizes)
    {
        for (int i = 0; i < positionCount; i++) {
            serializedRowSizes[i] += POSITION_SIZE;
        }

        int[] offsetsCopy = ensureCapacity(null, positionCount + 1, SMALL, NONE, bufferAllocator);
        try {
            System.arraycopy(offsets, 0, offsetsCopy, 0, positionCount + 1);
            valuesBuffers.accumulateSerializedRowSizes(offsetsCopy, positionCount, serializedRowSizes);
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

        // If all positions for the ArrayBlock to be copied are null, the number of positions to copy for its
        // nested values block could be 0. In such case we don't need to proceed.
        if (this.positionCount == 0) {
            return;
        }

        int offset = offsets[positionsOffset];
        valuesBuffers.setNextBatch(offset, offsets[positionsOffset + batchSize] - offset);
    }

    @Override
    public void appendDataInBatch()
    {
        if (batchSize == 0) {
            return;
        }

        appendNulls();
        appendOffsets();
        valuesBuffers.appendDataInBatch();

        bufferedPositionCount += batchSize;
    }

    @Override
    public void serializeTo(SliceOutput output)
    {
        writeLengthPrefixedString(output, NAME);

        valuesBuffers.serializeTo(output);

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

        valuesBuffers.resetBuffers();
    }

    @Override
    public void noMoreBatches()
    {
        valuesBuffers.noMoreBatches();

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
        return INSTANCE_SIZE +
                valuesBuffers.getRetainedSizeInBytes();
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +            // encoding name
                valuesBuffers.getSerializedSizeInBytes() +   // nested block
                SIZE_OF_INT +                           // positionCount
                SIZE_OF_INT +                           // offset 0. The offsetsBuffer doesn't contain the offset 0 so we need to add it here.
                offsetsBufferIndex +                    // offsets buffer.
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("super", super.toString())
                .add("estimatedOffsetBufferMaxCapacity", estimatedOffsetBufferMaxCapacity)
                .add("offsetsBufferCapacity", offsetsBuffer == null ? 0 : offsetsBuffer.length)
                .add("offsetsBufferIndex", offsetsBufferIndex)
                .add("offsetsCapacity", offsets == null ? 0 : offsets.length)
                .add("lastOffset", lastOffset)
                .add("valueBuffers", valuesBuffers)
                .toString();
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
    BlockEncodingBuffer getValuesBuffers()
    {
        return valuesBuffers;
    }

    @Override
    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode, int partitionBufferCapacity, double decodedBlockPageSizeFraction)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        decodedBlockNode = mapPositionsToNestedBlock(decodedBlockNode);
        ColumnarArray columnarArray = (ColumnarArray) decodedBlockNode.getDecodedBlock();
        decodedBlock = columnarArray.getNullCheckBlock();

        long estimatedSerializedSizeInBytes = decodedBlockNode.getEstimatedSerializedSizeInBytes();
        long childrenEstimatedSerializedSizeInBytes = decodedBlockNode.getChildren().get(0).getEstimatedSerializedSizeInBytes();

        double targetBufferSize = partitionBufferCapacity * decodedBlockPageSizeFraction *
                (estimatedSerializedSizeInBytes - childrenEstimatedSerializedSizeInBytes) / estimatedSerializedSizeInBytes;

        setEstimatedNullsBufferMaxCapacity(getEstimatedBufferMaxCapacity(targetBufferSize, Byte.BYTES, POSITION_SIZE));
        estimatedOffsetBufferMaxCapacity = getEstimatedBufferMaxCapacity(targetBufferSize, Integer.BYTES, POSITION_SIZE);

        populateNestedPositions(columnarArray);

        valuesBuffers.setupDecodedBlockAndMapPositions(decodedBlockNode.getChildren().get(0), partitionBufferCapacity, decodedBlockPageSizeFraction * childrenEstimatedSerializedSizeInBytes / estimatedSerializedSizeInBytes);
    }

    @Override
    protected void accumulateSerializedRowSizes(int[] positionOffsets, int positionCount, int[] serializedRowSizes)
    {
        if (this.positionCount == 0) {
            return;
        }

        int lastOffset = positionOffsets[0];
        for (int i = 0; i < positionCount; i++) {
            int offset = positionOffsets[i + 1];
            serializedRowSizes[i] += POSITION_SIZE * (offset - lastOffset);
            lastOffset = offset;
            positionOffsets[i + 1] = offsets[offset];
        }

        valuesBuffers.accumulateSerializedRowSizes(positionOffsets, positionCount, serializedRowSizes);
    }

    private void populateNestedPositions(ColumnarArray columnarArray)
    {
        // Reset nested level positions before checking positionCount. Failing to do so may result in valuesBuffers having stale values when positionCount is 0.
        valuesBuffers.resetPositions();

        if (positionCount == 0) {
            return;
        }

        offsets = ensureCapacity(offsets, positionCount + 1, SMALL, NONE, bufferAllocator);
        offsets[0] = 0;

        int[] positions = getPositions();

        for (int i = 0; i < positionCount; i++) {
            offsets[i + 1] = offsets[i] + columnarArray.getLength(positions[i]);
        }

        valuesBuffers.ensurePositionsCapacity(offsets[positionCount]);

        for (int i = 0; i < positionCount; i++) {
            valuesBuffers.appendPositionRange(columnarArray.getOffset(positions[i]), offsets[i + 1] - offsets[i]);
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
