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

import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.type.TypeSerde;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.operator.UncheckedByteArrays.setInt;
import static com.facebook.presto.operator.UncheckedByteArrays.setInts;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class MapBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    @VisibleForTesting
    public static final int POSITION_SIZE = SIZE_OF_INT + SIZE_OF_BYTE;

    private static final String NAME = "MAP";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapBlockEncodingBuffers.class).instanceSize();

    private static final int HASH_MULTIPLIER = 2;

    // The buffer for the hashtables for all incoming blocks so far
    private byte[] hashTablesBuffer;

    // The address that the next hashtable entry will be written to.
    private int hashTableBufferIndex;

    // If any of the incoming blocks do not contain the hashtable, noHashTables is set to true.
    private boolean noHashTables;

    // The buffer for the offsets for all incoming blocks so far
    private byte[] offsetsBuffer;

    // The address that the next offset value will be written to.
    private int offsetsBufferIndex;

    // This array holds the condensed offsets for each position for the incoming block.
    private int[] offsets;

    // The last offset in the offsets buffer
    private int lastOffset;

    // This array holds the offsets into its nested raw block for each top level row.
    private int[] keyOffsetsOffsets;
    private int[] valueOffsetsOffsets;

    // The current incoming MapBlock is converted into ColumnarMap
    private ColumnarMap columnarMap;

    private final BlockEncodingBuffers keyBuffers;
    private final BlockEncodingBuffers valueBuffers;

    public MapBlockEncodingBuffers(DecodedBlockNode decodedBlockNode, int initialPositionCount)
    {
        super(initialPositionCount);

        keyBuffers = createBlockEncodingBuffers(decodedBlockNode.getChildren().get(0), initialPositionCount);
        valueBuffers = createBlockEncodingBuffers(decodedBlockNode.getChildren().get(1), initialPositionCount);

        prepareBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        hashTablesBuffer = new byte[initialPositionCount * ARRAY_INT_INDEX_SCALE * HASH_MULTIPLIER];
        hashTableBufferIndex = 0;

        offsetsBuffer = new byte[initialPositionCount * ARRAY_INT_INDEX_SCALE];
        offsetsBufferIndex = 0;

        keyBuffers.prepareBuffers();
        valueBuffers.prepareBuffers();
    }

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        offsetsBufferIndex = 0;
        lastOffset = 0;
        hashTableBufferIndex = 0;
        noHashTables = false;
        resetNullsBuffer();

        keyBuffers.resetBuffers();
        valueBuffers.resetBuffers();
    }

    @Override
    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        decodedBlockNode = mapPositions(decodedBlockNode);
        columnarMap = (ColumnarMap) decodedBlockNode.getDecodedBlock();
        decodedBlock = columnarMap.getNullCheckBlock();

        populateNestedPositions();

        keyBuffers.setupDecodedBlockAndMapPositions(decodedBlockNode.getChildren().get(0));
        valueBuffers.setupDecodedBlockAndMapPositions(decodedBlockNode.getChildren().get(1));
    }

    @Override
    public void accumulateRowSizes(int[] rowSizes)
    {
        for (int i = 0; i < positionCount; i++) {
            rowSizes[i] += POSITION_SIZE;
        }

        ensureOffsetsOffsetsCapacity(positionCount);
        System.arraycopy(offsets, 0, keyOffsetsOffsets, 0, positionCount + 1);
        System.arraycopy(offsets, 0, valueOffsetsOffsets, 0, positionCount + 1);

        keyBuffers.accumulateRowSizes(keyOffsetsOffsets, positionCount, rowSizes);
        valueBuffers.accumulateRowSizes(valueOffsetsOffsets, positionCount, rowSizes);
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

        // offsetsOffsets might be modified by the next level. Save it for the valueBuffers first.
        ensureOffsetsOffsetsCapacity(positionCount);
        System.arraycopy(offsetsOffsets, 0, valueOffsetsOffsets, 0, positionCount + 1);

        keyBuffers.accumulateRowSizes(offsetsOffsets, positionCount, rowSizes);
        valueBuffers.accumulateRowSizes(offsetsOffsets, positionCount, rowSizes);
    }

    @Override
    public void setNextBatch(int positionsOffset, int batchSize)
    {
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;

        if (this.positionCount == 0) {
            return;
        }

        int beginOffset = offsets[positionsOffset];
        int endOffset = offsets[positionsOffset + batchSize];

        keyBuffers.setNextBatch(beginOffset, endOffset - beginOffset);
        valueBuffers.setNextBatch(beginOffset, endOffset - beginOffset);
    }

    @Override
    public void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        appendNulls();
        appendOffsets();
        appendHashTables();

        keyBuffers.copyValues();
        valueBuffers.copyValues();

        bufferedPositionCount += batchSize;
    }

    public void serializeTo(SliceOutput sliceOutput)
    {
        writeLengthPrefixedString(sliceOutput, NAME);

        TypeSerde.writeType(sliceOutput, columnarMap.getKeyType());

        keyBuffers.serializeTo(sliceOutput);
        valueBuffers.serializeTo(sliceOutput);

        // Hashtables
        int hashTableSize = lastOffset * HASH_MULTIPLIER;

        if (hashTableBufferIndex == 0) {
            sliceOutput.appendInt(-1);
        }
        else {
            sliceOutput.appendInt(hashTableSize);
            sliceOutput.appendBytes(hashTablesBuffer, 0, hashTableBufferIndex);
        }

        sliceOutput.writeInt(bufferedPositionCount); //positionCount
        sliceOutput.appendInt(0);
        sliceOutput.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);

        serializeNullsTo(sliceOutput);
    }

    @Override
    public long getSizeInBytes()
    {
        return getPositionsSizeInBytes() +          // positions and mappedPositions
                offsetsBufferIndex +
                SIZE_OF_INT * positionCount +       // offsets array
                SIZE_OF_INT * positionCount * 2 +   // keyOffsetsOffsets and valueOffsetsOffsets
                getNullsBufferSizeInBytes() +
                keyBuffers.getSizeInBytes() +
                valueBuffers.getSizeInBytes() +
                hashTableBufferIndex +
                columnarMap.getSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                getPostionsRetainedSizeInBytes() +
                sizeOf(offsets) +
                sizeOf(offsetsBuffer) +
                sizeOf(keyOffsetsOffsets) +
                sizeOf(valueOffsetsOffsets) +
                getNullsBufferRetainedSizeInBytes() +
                keyBuffers.getRetainedSizeInBytes() +
                valueBuffers.getRetainedSizeInBytes() +
                sizeOf(hashTablesBuffer) +
                columnarMap.getRetainedSizeInBytes();
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +            // length prefixed encoding name
                columnarMap.getKeyType().getTypeSignature().toString().length() + SIZE_OF_INT + // length prefixed type string
                keyBuffers.getSerializedSizeInBytes() +    // nested key block
                valueBuffers.getSerializedSizeInBytes() +  // nested value block
                SIZE_OF_INT +                           // hash tables size
                hashTableBufferIndex +                  // hash tables
                SIZE_OF_INT +                           // positionCount
                offsetsBufferIndex + SIZE_OF_INT +      // offsets buffer.
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    private void populateNestedPositions()
    {
        // Nested level positions always start from 0.
        keyBuffers.positionsOffset = 0;
        keyBuffers.positionCount = 0;
        keyBuffers.isPositionsMapped = false;

        valueBuffers.positionsOffset = 0;
        valueBuffers.positionCount = 0;
        valueBuffers.isPositionsMapped = false;

        if (positionCount == 0) {
            return;
        }

        ensureOffsetsCapacity();

        int[] positions = getPositions();

        int columnarArrayBaseOffset = columnarMap.getOffset(0);
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            int beginOffset = columnarMap.getOffset(position);
            int endOffset = columnarMap.getOffset(position + 1);
            int currentRowSize = endOffset - beginOffset;

            offsets[i + 1] = offsets[i] + currentRowSize;

            beginOffset -= columnarArrayBaseOffset;
            if (currentRowSize > 0) {
                keyBuffers.appendPositionRange(beginOffset, currentRowSize);
                valueBuffers.appendPositionRange(beginOffset, currentRowSize);
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

    private void appendHashTables()
    {
        // MergingPageOutput may build hash tables for some of the smallers blocks. But if there're some blocks
        // without hash tables, it means hash tables are not needed so far. In this case we don't send the hash tables.
        if (noHashTables) {
            return;
        }

        Optional<int[]> hashTables = columnarMap.getHashTables();
        if (!hashTables.isPresent()) {
            noHashTables = true;
            hashTableBufferIndex = 0;
            return;
        }

        int hashTablesSize = (offsets[positionsOffset + batchSize] - offsets[positionsOffset]) * HASH_MULTIPLIER;
        ensureHashTablesBufferSize(hashTableBufferIndex + hashTablesSize * ARRAY_INT_INDEX_SCALE);

        int[] positions = getPositions();

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];

            int beginOffset = columnarMap.getOffset(position);
            int endOffset = columnarMap.getOffset(position + 1);

            hashTableBufferIndex = setInts(
                    hashTablesBuffer,
                    hashTableBufferIndex,
                    hashTables.get(),
                    beginOffset * HASH_MULTIPLIER,
                    (endOffset - beginOffset) * HASH_MULTIPLIER);
        }
    }

    private void ensureOffsetsCapacity()
    {
        if (offsets == null || offsets.length < positionCount + 1) {
            offsets = new int[positionCount * 2];
        }
    }

    private void ensureOffsetsOffsetsCapacity(int positionCount)
    {
        if (keyOffsetsOffsets == null || keyOffsetsOffsets.length < positionCount + 1) {
            keyOffsetsOffsets = new int[positionCount * 2];
        }

        if (valueOffsetsOffsets == null || valueOffsetsOffsets.length < positionCount + 1) {
            valueOffsetsOffsets = new int[positionCount * 2];
        }
    }

    private void ensureOffsetsBufferCapacity()
    {
        int capacity = offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE;
        if (offsetsBuffer.length < capacity) {
            offsetsBuffer = Arrays.copyOf(offsetsBuffer, max(capacity, offsetsBuffer.length * 2));
        }
    }

    private void ensureHashTablesBufferSize(int capacity)
    {
        if (hashTablesBuffer.length < capacity) {
            hashTablesBuffer = Arrays.copyOf(hashTablesBuffer, max(capacity, hashTablesBuffer.length * 2));
        }
    }
}
