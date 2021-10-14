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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.Int128ArrayBlock;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.block.ShortArrayBlock;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;

import javax.annotation.Nullable;

import java.util.Arrays;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.LARGE;
import static com.facebook.presto.common.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.INITIALIZE;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.NONE;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.MoreByteArrays.fill;
import static com.facebook.presto.operator.UncheckedByteArrays.setByteUnchecked;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public abstract class AbstractBlockEncodingBuffer
        implements BlockEncodingBuffer
{
    @VisibleForTesting
    public static final double GRACE_FACTOR_FOR_MAX_BUFFER_CAPACITY = 1.2f;

    // The allocator for internal buffers
    protected final ArrayAllocator bufferAllocator;

    // Boolean indicating whether this is a buffer for a nested level block.
    protected final boolean isNested;

    // The block after peeling off the Dictionary or RLE wrappings.
    protected Block decodedBlock;

    // The number of positions (rows) to be copied from the incoming block.
    protected int positionCount;

    // The number of positions (rows) to be copied from the incoming block within current batch.
    protected int batchSize;

    // The offset into positions/mappedPositions array that the current batch starts copying.
    // I.e. in each batch we copy the values of rows from positions[positionsOffset] to positions[positionsOffset + batchSize]
    protected int positionsOffset;

    // The number of positions (rows) buffered so far
    protected int bufferedPositionCount;

    // Whether the positions array has already been mapped to mappedPositions
    protected boolean positionsMapped;

    // Whether the buffers have been flushed
    protected boolean flushed;

    // The positions (row numbers) of the incoming Block to be copied to this buffer.
    // All top level AbstractBlockEncodingBuffer for the same partition share the same positions array.
    private int[] positions;

    // The mapped positions of the incoming Dictionary or RLE Block. For other blocks, mappedPosition
    // is either null or meaningless.
    //
    // For example, a VARCHAR column with 5 rows - ['b', 'a', 'b', 'a', 'c'] is represented as
    // a DictionaryBlock with dictionary ['a', 'b', 'c'] and ids [1, 0, 1, 0, 2]. The mappedPositions
    // array for positions [0, 1, 4] for this VARCHAR column will be [1, 0, 2].
    @Nullable
    private int[] mappedPositions;

    @Nullable
    private byte[] nullsBuffer;

    // The address offset in the nullsBuffer if new values are to be added.
    private int nullsBufferIndex;

    // The target size for nullsBuffer
    private int estimatedNullsBufferMaxCapacity;

    // For each batch we put the nulls values up to a multiple of 8 into nullsBuffer. The rest is kept in remainingNulls.
    private final boolean[] remainingNulls = new boolean[Byte.SIZE];

    // Number of null values not put into nullsBuffer from last batch
    private int remainingNullsCount;

    // Boolean indicating whether there are any null values in the nullsBuffer. It is possible that all values are non-null.
    private boolean hasEncodedNulls;

    protected abstract void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode, int partitionBufferCapacity, double decodedBlockPageSizeFraction);

    protected AbstractBlockEncodingBuffer(ArrayAllocator bufferAllocator, boolean isNested)
    {
        this.bufferAllocator = requireNonNull(bufferAllocator, "bufferAllocator is null");
        this.isNested = isNested;
    }

    public static BlockEncodingBuffer createBlockEncodingBuffers(DecodedBlockNode decodedBlockNode, ArrayAllocator bufferAllocator, boolean isNested)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");
        requireNonNull(bufferAllocator, "bufferAllocator is null");

        // decodedBlock could be a block or ColumnarArray/Map/Row
        Object decodedBlock = decodedBlockNode.getDecodedBlock();

        // Skip the Dictionary/Rle block node. The mapping info is not needed when creating buffers.
        // This is because the AbstractBlockEncodingBuffer is only created once, while position mapping for Dictionar/Rle blocks
        // need to be done for every incoming block.
        if (decodedBlock instanceof DictionaryBlock) {
            decodedBlockNode = decodedBlockNode.getChildren().get(0);
            decodedBlock = decodedBlockNode.getDecodedBlock();
        }
        else if (decodedBlock instanceof RunLengthEncodedBlock) {
            decodedBlockNode = decodedBlockNode.getChildren().get(0);
            decodedBlock = decodedBlockNode.getDecodedBlock();
        }

        verify(!(decodedBlock instanceof DictionaryBlock), "Nested RLEs and dictionaries are not supported");
        verify(!(decodedBlock instanceof RunLengthEncodedBlock), "Nested RLEs and dictionaries are not supported");

        if (decodedBlock instanceof LongArrayBlock) {
            return new LongArrayBlockEncodingBuffer(bufferAllocator, isNested);
        }

        if (decodedBlock instanceof Int128ArrayBlock) {
            return new Int128ArrayBlockEncodingBuffer(bufferAllocator, isNested);
        }

        if (decodedBlock instanceof IntArrayBlock) {
            return new IntArrayBlockEncodingBuffer(bufferAllocator, isNested);
        }

        if (decodedBlock instanceof ShortArrayBlock) {
            return new ShortArrayBlockEncodingBuffer(bufferAllocator, isNested);
        }

        if (decodedBlock instanceof ByteArrayBlock) {
            return new ByteArrayBlockEncodingBuffer(bufferAllocator, isNested);
        }

        if (decodedBlock instanceof VariableWidthBlock) {
            return new VariableWidthBlockEncodingBuffer(bufferAllocator, isNested);
        }

        if (decodedBlock instanceof ColumnarArray) {
            return new ArrayBlockEncodingBuffer(decodedBlockNode, bufferAllocator, isNested);
        }

        if (decodedBlock instanceof ColumnarMap) {
            return new MapBlockEncodingBuffer(decodedBlockNode, bufferAllocator, isNested);
        }

        if (decodedBlock instanceof ColumnarRow) {
            return new RowBlockEncodingBuffer(decodedBlockNode, bufferAllocator, isNested);
        }

        throw new IllegalArgumentException("Unsupported encoding: " + decodedBlock.getClass().getSimpleName());
    }

    @Override
    public void setupDecodedBlocksAndPositions(DecodedBlockNode decodedBlockNode, int[] positions, int positionCount, int partitionBufferCapacity, long estimatedSerializedPageSize)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");
        requireNonNull(positions, "positions is null");

        this.positions = positions;
        this.positionCount = positionCount;
        this.positionsOffset = 0;
        this.positionsMapped = false;

        double decodedBlockPageSizeFraction = (decodedBlockNode.getEstimatedSerializedSizeInBytes()) / ((double) estimatedSerializedPageSize);

        setupDecodedBlockAndMapPositions(decodedBlockNode, partitionBufferCapacity, decodedBlockPageSizeFraction);
    }

    @Override
    public void setNextBatch(int positionsOffset, int batchSize)
    {
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;
        this.flushed = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("isNested", isNested)
                .add("decodedBlock", mappedPositions == null ? "null" : decodedBlock)
                .add("positionCount", positionCount)
                .add("batchSize", batchSize)
                .add("positionsOffset", positionsOffset)
                .add("bufferedPositionCount", bufferedPositionCount)
                .add("positionsMapped", positionsMapped)
                .add("flushed", flushed)
                .add("positionsCapacity", positions == null ? 0 : positions.length)
                .add("mappedPositionsCapacity", mappedPositions == null ? 0 : mappedPositions.length)
                .add("estimatedNullsBufferMaxCapacity", estimatedNullsBufferMaxCapacity)
                .add("nullsBufferCapacity", nullsBuffer == null ? 0 : nullsBuffer.length)
                .add("nullsBufferIndex", nullsBufferIndex)
                .add("remainingNullsCount", remainingNullsCount)
                .add("hasEncodedNulls", hasEncodedNulls)
                .toString();
    }

    @VisibleForTesting
    abstract int getEstimatedValueBufferMaxCapacity();

    @VisibleForTesting
    int getEstimatedNullsBufferMaxCapacity()
    {
        return estimatedNullsBufferMaxCapacity;
    }

    protected void setEstimatedNullsBufferMaxCapacity(int estimatedNullsBufferMaxCapacity)
    {
        this.estimatedNullsBufferMaxCapacity = estimatedNullsBufferMaxCapacity;
    }

    protected static int getEstimatedBufferMaxCapacity(double targetBufferSize, int unitSize, int positionSize)
    {
        return (int) (targetBufferSize * unitSize / positionSize * GRACE_FACTOR_FOR_MAX_BUFFER_CAPACITY);
    }

    /**
     * Map the positions for DictionaryBlock to its nested dictionaryBlock, and positions for RunLengthEncodedBlock
     * to its nested value block. For example, positions [0, 2, 5] on DictionaryBlock with dictionary block ['a', 'b', 'c']
     * and ids [1, 1, 2, 0, 2, 1] will be mapped to [1, 2, 1], and the copied data will be ['b', 'c', 'b'].
     */
    protected DecodedBlockNode mapPositionsToNestedBlock(DecodedBlockNode decodedBlockNode)
    {
        Object decodedObject = decodedBlockNode.getDecodedBlock();

        if (decodedObject instanceof DictionaryBlock) {
            DictionaryBlock dictionaryBlock = (DictionaryBlock) decodedObject;
            mappedPositions = ensureCapacity(mappedPositions, positionCount, SMALL, NONE, bufferAllocator);

            for (int i = 0; i < positionCount; i++) {
                mappedPositions[i] = dictionaryBlock.getId(positions[i]);
            }
            positionsMapped = true;
            return decodedBlockNode.getChildren().get(0);
        }

        if (decodedObject instanceof RunLengthEncodedBlock) {
            mappedPositions = ensureCapacity(mappedPositions, positionCount, SMALL, INITIALIZE, bufferAllocator);
            positionsMapped = true;
            return decodedBlockNode.getChildren().get(0);
        }

        positionsMapped = false;
        return decodedBlockNode;
    }

    protected void ensurePositionsCapacity(int capacity)
    {
        positions = ensureCapacity(positions, capacity, SMALL, NONE, bufferAllocator);
    }

    protected void appendPositionRange(int offset, int length)
    {
        for (int i = 0; i < length; i++) {
            positions[positionCount++] = offset + i;
        }
    }

    /**
     * Called by accumulateSerializedRowSizes(int[] serializedRowSizes) for composite types to add row sizes for nested blocks.
     * <p>
     * For example, a AbstractBlockEncodingBuffer for an array(int) column with 3 rows - [{1, 2}, {3}, {4, 5, 6}] - will call accumulateSerializedRowSizes
     * with positionOffsets = [0, 2, 3, 6] and serializedRowSizes = [5, 5, 5] (5 = 4 + 1 where 4 bytes for offset and 1 for null flag).
     * After the method returns, serializedRowSizes will be [15, 10, 20].
     *
     * @param positionOffsets The offsets of the top level rows
     * @param positionCount Number of top level rows
     */
    protected abstract void accumulateSerializedRowSizes(int[] positionOffsets, int positionCount, int[] serializedRowSizes);

    protected void resetPositions()
    {
        positionsOffset = 0;
        positionCount = 0;
        positionsMapped = false;
    }

    protected int[] getPositions()
    {
        if (positionsMapped) {
            verify(mappedPositions != null);
            return mappedPositions;
        }

        return positions;
    }

    protected void appendNulls()
    {
        if (decodedBlock.mayHaveNull()) {
            // Write to nullsBuffer if there is a possibility to have nulls. It is possible that the
            // decodedBlock contains nulls, but rows that go into this partition don't. Write to nullsBuffer anyway.
            nullsBuffer = ensureCapacity(nullsBuffer, (bufferedPositionCount + batchSize) / Byte.SIZE + 1, estimatedNullsBufferMaxCapacity, LARGE, PRESERVE, bufferAllocator);

            int bufferedNullsCount = nullsBufferIndex * Byte.SIZE + remainingNullsCount;
            if (bufferedPositionCount > bufferedNullsCount) {
                // There are no nulls in positions (bufferedNullsCount, bufferedPositionCount]
                encodeNonNullsAsBits(bufferedPositionCount - bufferedNullsCount);
            }

            // Append this batch
            encodeNullsAsBits(decodedBlock);
        }
        else if (containsNull()) {
            // There were nulls in previously buffered rows, but for this batch there can't be any nulls.
            // Any how we need to append 0's for this batch.
            nullsBuffer = ensureCapacity(nullsBuffer, (bufferedPositionCount + batchSize) / Byte.SIZE + 1, estimatedNullsBufferMaxCapacity, LARGE, PRESERVE, bufferAllocator);

            encodeNonNullsAsBits(batchSize);
        }
    }

    @Override
    public void noMoreBatches()
    {
        // Only the positions for nested level need to be recycled.
        if (isNested && positions != null) {
            bufferAllocator.returnArray(positions);
            positions = null;
        }

        if (mappedPositions != null) {
            bufferAllocator.returnArray(mappedPositions);
            mappedPositions = null;
        }

        // Only when this is the last batch in the page and it filled the buffer, the buffers can be recycled.
        if (flushed && nullsBuffer != null) {
            bufferAllocator.returnArray(nullsBuffer);
            nullsBuffer = null;
        }
    }

    protected void serializeNullsTo(SliceOutput output)
    {
        encodeRemainingNullsAsBits();

        if (hasEncodedNulls) {
            output.writeBoolean(true);
            output.appendBytes(nullsBuffer, 0, nullsBufferIndex);
        }
        else {
            output.writeBoolean(false);
        }
    }

    protected static void writeLengthPrefixedString(SliceOutput output, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    protected void resetNullsBuffer()
    {
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        hasEncodedNulls = false;
    }

    protected long getNullsBufferSerializedSizeInBytes()
    {
        long size = SIZE_OF_BYTE;       // nulls uses 1 byte for mayHaveNull
        if (containsNull()) {
            size += nullsBufferIndex +  // nulls buffer
                    (remainingNullsCount > 0 ? SIZE_OF_BYTE : 0);   // The remaining nulls not serialized yet. We have to add it here because at the time of calling this function, the remaining nulls was not put into the nullsBuffer yet.
        }
        return size;
    }

    private void encodeNullsAsBits(Block block)
    {
        int[] positions = getPositions();

        if (remainingNullsCount + batchSize < Byte.SIZE) {
            // just put all of this batch to remainingNulls
            for (int i = 0; i < batchSize; i++) {
                remainingNulls[remainingNullsCount++] = block.isNull(positions[positionsOffset + i]);
            }

            return;
        }

        // Process the remaining nulls from last batch
        int offset = positionsOffset;

        if (remainingNullsCount > 0) {
            byte value = 0;

            for (int i = 0; i < remainingNullsCount; i++) {
                value |= remainingNulls[i] ? 0b1000_0000 >>> i : 0;
            }

            // process a few more nulls to make up one byte
            for (int i = remainingNullsCount; i < Byte.SIZE; i++) {
                value |= block.isNull(positions[offset++]) ? 0b1000_0000 >>> i : 0;
            }

            hasEncodedNulls |= (value != 0);
            nullsBufferIndex = setByteUnchecked(nullsBuffer, nullsBufferIndex, value);

            remainingNullsCount = 0;
        }

        // Process the next Byte.SIZE * n positions. We now have processed (offset - positionsOffset) positions
        int remainingPositions = batchSize - (offset - positionsOffset);
        int positionsToEncode = remainingPositions & ~0b111;
        for (int i = 0; i < positionsToEncode; i += Byte.SIZE) {
            byte value = 0;
            value |= block.isNull(positions[offset]) ? 0b1000_0000 : 0;
            value |= block.isNull(positions[offset + 1]) ? 0b0100_0000 : 0;
            value |= block.isNull(positions[offset + 2]) ? 0b0010_0000 : 0;
            value |= block.isNull(positions[offset + 3]) ? 0b0001_0000 : 0;
            value |= block.isNull(positions[offset + 4]) ? 0b0000_1000 : 0;
            value |= block.isNull(positions[offset + 5]) ? 0b0000_0100 : 0;
            value |= block.isNull(positions[offset + 6]) ? 0b0000_0010 : 0;
            value |= block.isNull(positions[offset + 7]) ? 0b0000_0001 : 0;

            hasEncodedNulls |= (value != 0);
            nullsBufferIndex = setByteUnchecked(nullsBuffer, nullsBufferIndex, value);
            offset += Byte.SIZE;
        }

        // Process the remaining positions
        remainingNullsCount = remainingPositions & 0b111;
        for (int i = 0; i < remainingNullsCount; i++) {
            remainingNulls[i] = block.isNull(positions[offset++]);
        }
    }

    private void encodeNonNullsAsBits(int count)
    {
        if (remainingNullsCount + count < Byte.SIZE) {
            // just put all of this batch to remainingNulls
            for (int i = 0; i < count; i++) {
                remainingNulls[remainingNullsCount++] = false;
            }
            return;
        }

        int remainingPositions = count - encodeRemainingNullsAsBits();

        nullsBufferIndex = fill(nullsBuffer, nullsBufferIndex, remainingPositions >>> 3, (byte) 0);

        remainingNullsCount = remainingPositions & 0b111;
        Arrays.fill(remainingNulls, false);
    }

    private int encodeRemainingNullsAsBits()
    {
        if (remainingNullsCount == 0) {
            return 0;
        }

        byte value = 0;
        for (int i = 0; i < remainingNullsCount; i++) {
            value |= remainingNulls[i] ? 0b1000_0000 >>> i : 0;
        }

        hasEncodedNulls |= (value != 0);
        nullsBufferIndex = setByteUnchecked(nullsBuffer, nullsBufferIndex, value);

        int padding = Byte.SIZE - remainingNullsCount;
        remainingNullsCount = 0;
        return padding;
    }

    private boolean containsNull()
    {
        if (hasEncodedNulls) {
            return true;
        }

        for (int i = 0; i < remainingNullsCount; i++) {
            if (remainingNulls[i]) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    void checkValidPositions()
    {
        verify(decodedBlock != null, "decodedBlock should have been set up");

        int positionCount = decodedBlock.getPositionCount();

        int[] positions = getPositions();
        for (int i = 0; i < this.positionCount; i++) {
            if (positions[i] < 0 || positions[i] >= positionCount) {
                throw new IndexOutOfBoundsException(format("Invalid position %d for decodedBlock with %d positions", positions[i], positionCount));
            }
        }
    }
}
