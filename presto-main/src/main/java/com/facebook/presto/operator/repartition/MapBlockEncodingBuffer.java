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
import com.facebook.presto.common.block.ColumnarMap;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.LARGE;
import static com.facebook.presto.common.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.NONE;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.MoreByteArrays.setInts;
import static com.facebook.presto.operator.UncheckedByteArrays.setIntUnchecked;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class MapBlockEncodingBuffer
        extends AbstractBlockEncodingBuffer
{
    @VisibleForTesting
    static final int POSITION_SIZE = SIZE_OF_INT + SIZE_OF_BYTE;

    @VisibleForTesting
    static final int HASH_MULTIPLIER = 2;

    private static final int POSITION_SIZE_WITH_HASHTABLE = SIZE_OF_INT + SIZE_OF_BYTE + SIZE_OF_INT * HASH_MULTIPLIER;
    private static final String NAME = "MAP";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapBlockEncodingBuffer.class).instanceSize();

    // The buffer for the hashtables for all incoming blocks so far
    private byte[] hashTablesBuffer;

    // The address that the next hashtable entry will be written to.
    private int hashTableBufferIndex;

    // The estimated maximum size for hashTablesBuffer
    private int estimatedHashTableBufferMaxCapacity;

    // If any of the incoming blocks do not contain the hashtable, noHashTables is set to true.
    private boolean noHashTables;

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

    // The current incoming MapBlock is converted into ColumnarMap
    private ColumnarMap columnarMap;

    // The AbstractBlockEncodingBuffer for the nested key and value Block of the MapBlock
    private final AbstractBlockEncodingBuffer keyBuffers;
    private final AbstractBlockEncodingBuffer valueBuffers;

    public MapBlockEncodingBuffer(DecodedBlockNode decodedBlockNode, ArrayAllocator bufferAllocator, boolean isNested)
    {
        super(bufferAllocator, isNested);
        keyBuffers = (AbstractBlockEncodingBuffer) createBlockEncodingBuffers(decodedBlockNode.getChildren().get(0), bufferAllocator, true);
        valueBuffers = (AbstractBlockEncodingBuffer) createBlockEncodingBuffers(decodedBlockNode.getChildren().get(1), bufferAllocator, true);
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
            keyBuffers.accumulateSerializedRowSizes(offsetsCopy, positionCount, serializedRowSizes);

            System.arraycopy(offsets, 0, offsetsCopy, 0, positionCount + 1);
            valueBuffers.accumulateSerializedRowSizes(offsetsCopy, positionCount, serializedRowSizes);
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

        if (this.positionCount == 0) {
            return;
        }

        int beginOffset = offsets[positionsOffset];
        int endOffset = offsets[positionsOffset + batchSize];

        keyBuffers.setNextBatch(beginOffset, endOffset - beginOffset);
        valueBuffers.setNextBatch(beginOffset, endOffset - beginOffset);
    }

    @Override
    public void appendDataInBatch()
    {
        if (batchSize == 0) {
            return;
        }

        appendNulls();
        appendOffsets();
        appendHashTables();

        keyBuffers.appendDataInBatch();
        valueBuffers.appendDataInBatch();

        bufferedPositionCount += batchSize;
    }

    @Override
    public void serializeTo(SliceOutput output)
    {
        writeLengthPrefixedString(output, NAME);

        keyBuffers.serializeTo(output);
        valueBuffers.serializeTo(output);

        // Hash tables
        if (hashTableBufferIndex == 0) {
            output.appendInt(-1);
        }
        else {
            output.appendInt(lastOffset * HASH_MULTIPLIER); // Hash table length
            output.appendBytes(hashTablesBuffer, 0, hashTableBufferIndex);
        }

        output.writeInt(bufferedPositionCount);

        // offsets
        output.appendInt(0);
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
        hashTableBufferIndex = 0;
        noHashTables = false;
        flushed = true;
        resetNullsBuffer();

        keyBuffers.resetBuffers();
        valueBuffers.resetBuffers();
    }

    @Override
    public void noMoreBatches()
    {
        valueBuffers.noMoreBatches();
        keyBuffers.noMoreBatches();

        if (flushed) {
            if (hashTablesBuffer != null) {
                bufferAllocator.returnArray(hashTablesBuffer);
                hashTablesBuffer = null;
            }

            if (offsetsBuffer != null) {
                bufferAllocator.returnArray(offsetsBuffer);
                offsetsBuffer = null;
            }
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
        // columnarMap is counted as part of DecodedBlockNode in OptimizedPartitionedOutputOperator and won't be counted here.
        // This is because the same columnarMap would be hold in all partitions/AbstractBlockEncodingBuffer and thus counting it here would be counting it multiple times.
        return INSTANCE_SIZE +
                keyBuffers.getRetainedSizeInBytes() +
                valueBuffers.getRetainedSizeInBytes();
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +            // length prefixed encoding name
                keyBuffers.getSerializedSizeInBytes() +    // nested key block
                valueBuffers.getSerializedSizeInBytes() +  // nested value block
                SIZE_OF_INT +                           // hash tables size
                hashTableBufferIndex +                  // hash tables
                SIZE_OF_INT +                           // positionCount
                offsetsBufferIndex + SIZE_OF_INT +      // offsets buffer.
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("super", super.toString())
                .add("estimatedHashTableBufferMaxCapacity", estimatedHashTableBufferMaxCapacity)
                .add("hashTablesBufferCapacity", hashTablesBuffer == null ? 0 : hashTablesBuffer.length)
                .add("hashTableBufferIndex", hashTableBufferIndex)
                .add("estimatedOffsetBufferMaxCapacity", estimatedOffsetBufferMaxCapacity)
                .add("offsetsBufferCapacity", offsetsBuffer == null ? 0 : offsetsBuffer.length)
                .add("offsetsBufferIndex", offsetsBufferIndex)
                .add("offsetsCapacity", offsets == null ? 0 : offsets.length)
                .add("lastOffset", lastOffset)
                .add("keyBuffers", keyBuffers)
                .add("valueBuffers", valueBuffers)
                .toString();
    }

    @Override
    protected int getEstimatedValueBufferMaxCapacity()
    {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    int getEstimatedOffsetBufferMaxCapacity()
    {
        return estimatedOffsetBufferMaxCapacity;
    }

    @VisibleForTesting
    int getEstimatedHashTableBufferMaxCapacity()
    {
        return estimatedHashTableBufferMaxCapacity;
    }

    @VisibleForTesting
    BlockEncodingBuffer getKeyBuffers()
    {
        return keyBuffers;
    }

    @VisibleForTesting
    BlockEncodingBuffer getValueBuffers()
    {
        return valueBuffers;
    }

    @Override
    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode, int partitionBufferCapacity, double decodedBlockPageSizeFraction)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        decodedBlockNode = mapPositionsToNestedBlock(decodedBlockNode);
        columnarMap = (ColumnarMap) decodedBlockNode.getDecodedBlock();
        decodedBlock = columnarMap.getNullCheckBlock();

        long estimatedSerializedSizeInBytes = decodedBlockNode.getEstimatedSerializedSizeInBytes();
        long keyBufferEstimatedSerializedSizeInBytes = decodedBlockNode.getChildren().get(0).getEstimatedSerializedSizeInBytes();
        long valueBufferEstimatedSerializedSizeInBytes = decodedBlockNode.getChildren().get(1).getEstimatedSerializedSizeInBytes();

        double targetBufferSize = partitionBufferCapacity * decodedBlockPageSizeFraction *
                (estimatedSerializedSizeInBytes - keyBufferEstimatedSerializedSizeInBytes - valueBufferEstimatedSerializedSizeInBytes) / estimatedSerializedSizeInBytes;

        estimatedHashTableBufferMaxCapacity = getEstimatedBufferMaxCapacity(targetBufferSize, Integer.BYTES * HASH_MULTIPLIER, POSITION_SIZE_WITH_HASHTABLE);
        setEstimatedNullsBufferMaxCapacity(getEstimatedBufferMaxCapacity(targetBufferSize, Byte.BYTES, POSITION_SIZE_WITH_HASHTABLE));
        estimatedOffsetBufferMaxCapacity = getEstimatedBufferMaxCapacity(targetBufferSize, Integer.BYTES, POSITION_SIZE_WITH_HASHTABLE);

        populateNestedPositions();

        keyBuffers.setupDecodedBlockAndMapPositions(decodedBlockNode.getChildren().get(0), partitionBufferCapacity, decodedBlockPageSizeFraction * keyBufferEstimatedSerializedSizeInBytes / estimatedSerializedSizeInBytes);

        valueBuffers.setupDecodedBlockAndMapPositions(decodedBlockNode.getChildren().get(1), partitionBufferCapacity, decodedBlockPageSizeFraction * valueBufferEstimatedSerializedSizeInBytes / estimatedSerializedSizeInBytes);
    }

    @Override
    protected void accumulateSerializedRowSizes(int[] positionOffsets, int positionCount, int[] serializedRowSizes)
    {
        // If all positions for the MapBlock to be copied are null, the number of positions to copy for its
        // nested key and value blocks could be 0. In such case we don't need to proceed.
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

        // positionOffsets might be modified by the next level. Save it for the valueBuffers first.
        int[] offsetsCopy = ensureCapacity(null, positionCount + 1, SMALL, NONE, bufferAllocator);
        try {
            System.arraycopy(positionOffsets, 0, offsetsCopy, 0, positionCount + 1);

            keyBuffers.accumulateSerializedRowSizes(positionOffsets, positionCount, serializedRowSizes);
            valueBuffers.accumulateSerializedRowSizes(offsetsCopy, positionCount, serializedRowSizes);
        }
        finally {
            bufferAllocator.returnArray(offsetsCopy);
        }
    }

    private void populateNestedPositions()
    {
        // Reset nested level positions before checking positionCount. Failing to do so may result in elementsBuffers having stale values when positionCount is 0.
        keyBuffers.resetPositions();
        valueBuffers.resetPositions();

        if (positionCount == 0) {
            return;
        }

        offsets = ensureCapacity(offsets, positionCount + 1, SMALL, NONE, bufferAllocator);
        offsets[0] = 0;

        int[] positions = getPositions();

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            offsets[i + 1] = offsets[i] + columnarMap.getOffset(position + 1) - columnarMap.getOffset(position);
        }

        keyBuffers.ensurePositionsCapacity(offsets[positionCount]);
        valueBuffers.ensurePositionsCapacity(offsets[positionCount]);

        for (int i = 0; i < positionCount; i++) {
            int beginOffset = columnarMap.getOffset(positions[i]);
            int currentRowSize = offsets[i + 1] - offsets[i];
            keyBuffers.appendPositionRange(beginOffset, currentRowSize);
            valueBuffers.appendPositionRange(beginOffset, currentRowSize);
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

    private void appendHashTables()
    {
        // MergingPageOutput may build hash tables for some of the small blocks. But if there're some blocks
        // without hash tables, it means hash tables are not needed so far. In this case we don't send the hash tables.
        if (noHashTables) {
            return;
        }

        int[] hashTables = columnarMap.getHashTables();
        if (hashTables == null) {
            noHashTables = true;
            hashTableBufferIndex = 0;
            return;
        }

        int hashTablesSize = (offsets[positionsOffset + batchSize] - offsets[positionsOffset]) * HASH_MULTIPLIER;
        hashTablesBuffer = ensureCapacity(hashTablesBuffer, hashTableBufferIndex + hashTablesSize * ARRAY_INT_INDEX_SCALE, estimatedHashTableBufferMaxCapacity, LARGE, PRESERVE, bufferAllocator);

        int[] positions = getPositions();

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];

            int beginOffset = columnarMap.getAbsoluteOffset(position);
            int endOffset = columnarMap.getAbsoluteOffset(position + 1);

            hashTableBufferIndex = setInts(
                    hashTablesBuffer,
                    hashTableBufferIndex,
                    hashTables,
                    beginOffset * HASH_MULTIPLIER,
                    (endOffset - beginOffset) * HASH_MULTIPLIER);
        }
    }
}
