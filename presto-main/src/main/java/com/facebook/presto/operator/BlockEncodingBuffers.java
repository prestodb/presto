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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ByteArrayBlock;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.Int128ArrayBlock;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.block.ShortArrayBlock;
import io.airlift.slice.SliceOutput;

import javax.annotation.Nullable;

import java.util.Arrays;

import static com.facebook.presto.operator.UncheckedByteArrays.fill;
import static com.facebook.presto.operator.UncheckedByteArrays.setByte;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public abstract class BlockEncodingBuffers
{
    private static final int BITS_IN_BYTE = 8;
    private static final int INITIAL_POSITION_COUNT = 1024;

    // The initial buffer size in terms of number of elements
    protected final int initialPositionCount;

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

    // Whether the positions array was mapped to mappedPositions
    protected boolean isPositionsMapped;

    // The positions (row numbers) of the incoming Block to be copied to this buffer.
    // All top level BlockEncodingBuffers for the same partition share the same positions array.
    private int[] positions;

    // The mapped positions of the incoming Dictionary or RLE Block
    @Nullable
    private int[] mappedPositions;

    @Nullable
    private byte[] nullsBuffer;

    // The address offset in the nullsBuffer if new values are to be added.
    private int nullsBufferIndex;

    // For each batch we put the nulls values up to a multiple of 8 into nullsBuffer. The rest is kept in remainingNullsFromLastBatch.
    private final boolean[] remainingNullsFromLastBatch = new boolean[BITS_IN_BYTE];

    // Number of null values not put into nullsBuffer from last batch
    private int remainingNullsCount;

    // Whether nullsBuffer contains nulls. It is possible that the nullsBuffer contains all non-nulls.
    private boolean nullsBufferContainsNull;

    public static BlockEncodingBuffers createBlockEncodingBuffers(DecodedBlockNode decodedBlockNode, int initialPositionCount)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        Object decodedBlock = decodedBlockNode.getDecodedBlock();

        // Skip the Dictionary/Rle block node. The mapping info is not needed when creating buffers.
        if (decodedBlock instanceof DictionaryBlock) {
            decodedBlockNode = decodedBlockNode.getChildren().get(0);
            decodedBlock = decodedBlockNode.getDecodedBlock();
        }

        if (decodedBlock instanceof RunLengthEncodedBlock) {
            decodedBlockNode = decodedBlockNode.getChildren().get(0);
            decodedBlock = decodedBlockNode.getDecodedBlock();
        }

        if (decodedBlock instanceof LongArrayBlock) {
            return new LongArrayBlockEncodingBuffers(initialPositionCount);
        }

        if (decodedBlock instanceof IntArrayBlock) {
            return new IntArrayBlockEncodingBuffers(initialPositionCount);
        }

        if (decodedBlock instanceof ByteArrayBlock) {
            return new ByteArrayBlockEncodingBuffers(initialPositionCount);
        }

        if (decodedBlock instanceof ShortArrayBlock) {
            return new ShortArrayBlockEncodingBuffers(initialPositionCount);
        }

        if (decodedBlock instanceof Int128ArrayBlock) {
            return new Int128ArrayBlockEncodingBuffers(initialPositionCount);
        }

        throw new IllegalArgumentException("Unsupported encoding: " + decodedBlock.getClass().getSimpleName());
    }

    public BlockEncodingBuffers(int initialPositionCount)
    {
        this.initialPositionCount = initialPositionCount;
    }

    protected static void writeLengthPrefixedString(SliceOutput output, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    protected abstract void prepareBuffers();

    public abstract void resetBuffers();

    protected void resetNullsBuffer()
    {
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        nullsBufferContainsNull = false;
    }

    public void setNextBatch(int positionsOffset, int batchSize)
    {
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;
    }

    public abstract void copyValues();

    public abstract void serializeTo(SliceOutput sliceOutput);

    public abstract long getSizeInBytes();

    public abstract long getRetainedSizeInBytes();

    public abstract long getSerializedSizeInBytes();

    public abstract void accumulateRowSizes(int[] rowSizes);

    protected abstract void accumulateRowSizes(int[] positionOffsets, int positionCount, int[] rowSizes);

    public void setupDecodedBlocksAndPositions(DecodedBlockNode decodedBlockNode, int[] positions, int positionCount)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");
        requireNonNull(positions, "positions is null");

        this.positions = positions;
        this.positionCount = positionCount;
        this.positionsOffset = 0;

        setupDecodedBlockAndMapPositions(decodedBlockNode);
    }

    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");
        decodedBlock = (Block) mapPositions(decodedBlockNode).getDecodedBlock();
    }

    protected DecodedBlockNode mapPositions(DecodedBlockNode decodedBlockNode)
    {
        Object decodedObject = decodedBlockNode.getDecodedBlock();

        if (decodedObject instanceof DictionaryBlock) {
            mapPositionsForDictionary((DictionaryBlock) decodedObject);
            isPositionsMapped = true;
            return decodedBlockNode.getChildren().get(0);
        }

        if (decodedObject instanceof RunLengthEncodedBlock) {
            mapPositionsForRle();
            isPositionsMapped = true;
            return decodedBlockNode.getChildren().get(0);
        }

        isPositionsMapped = false;
        return decodedBlockNode;
    }

    protected void mapPositionsForDictionary(DictionaryBlock dictionaryBlock)
    {
        requireNonNull(dictionaryBlock, "decodedBlockNode is null");

        ensureMappedPositionsCapacity(positionCount);
        for (int i = 0; i < positionCount; i++) {
            mappedPositions[i] = dictionaryBlock.getId(positions[i]);
        }
    }

    protected void mapPositionsForRle()
    {
        ensureMappedPositionsCapacity(positionCount);
        Arrays.fill(mappedPositions, 0, positionCount, 0);
    }

    protected void appendPositionRange(int offset, int addedLength)
    {
        ensurePositionsCapacity(positionCount + addedLength);
        for (int i = 0; i < addedLength; i++) {
            positions[positionCount++] = offset + i;
        }
    }

    protected int[] getPositions()
    {
        if (isPositionsMapped) {
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
            ensureNullsValueBufferCapacity();
            int bufferedNullsCount = nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount;

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
            ensureNullsValueBufferCapacity();
            encodeNonNullsAsBits(batchSize);
        }
    }

    protected void serializeNullsTo(SliceOutput output)
    {
        encodeRemainingNullsAsBits();

        if (nullsBufferContainsNull) {
            output.writeBoolean(true);
            output.appendBytes(nullsBuffer, 0, nullsBufferIndex);
        }
        else {
            output.writeBoolean(false);
        }
    }

    private boolean containsNull()
    {
        if (nullsBufferContainsNull) {
            return true;
        }
        for (int i = 0; i < remainingNullsCount; i++) {
            if (remainingNullsFromLastBatch[i]) {
                return true;
            }
        }
        return false;
    }

    private void encodeNullsAsBits(Block block)
    {
        int[] positions = getPositions();

        if (remainingNullsCount + batchSize < BITS_IN_BYTE) {
            // just put all of this batch to remainingNullsFromLastBatch
            for (int offset = positionsOffset; offset < positionsOffset + batchSize; offset++) {
                remainingNullsFromLastBatch[remainingNullsCount++] = block.isNull(positions[offset]);
            }
            return;
        }

        // Process the remaining nulls from last batch
        int offset = positionsOffset;

        if (remainingNullsCount > 0) {
            byte value = 0;

            for (int i = 0; i < remainingNullsCount; i++) {
                value |= remainingNullsFromLastBatch[i] ? 0b1000_0000 >>> i : 0;
            }

            // process a few more nulls to make up one byte
            for (int i = remainingNullsCount; i < BITS_IN_BYTE; i++) {
                value |= block.isNull(positions[offset++]) ? 0b1000_0000 >>> i : 0;
            }

            nullsBufferContainsNull |= (value != 0);
            nullsBufferIndex = setByte(nullsBuffer, nullsBufferIndex, value);

            remainingNullsCount = 0;
        }

        // Process the next BITS_IN_BYTE * n positions. We now have processed (offset - positionsOffset) positions
        int remainingPositions = batchSize - (offset - positionsOffset);
        int positionsToEncode = remainingPositions & ~0b111;
        for (int i = 0; i < positionsToEncode; i += BITS_IN_BYTE) {
            byte value = 0;
            value |= block.isNull(positions[offset]) ? 0b1000_0000 : 0;
            value |= block.isNull(positions[offset + 1]) ? 0b0100_0000 : 0;
            value |= block.isNull(positions[offset + 2]) ? 0b0010_0000 : 0;
            value |= block.isNull(positions[offset + 3]) ? 0b0001_0000 : 0;
            value |= block.isNull(positions[offset + 4]) ? 0b0000_1000 : 0;
            value |= block.isNull(positions[offset + 5]) ? 0b0000_0100 : 0;
            value |= block.isNull(positions[offset + 6]) ? 0b0000_0010 : 0;
            value |= block.isNull(positions[offset + 7]) ? 0b0000_0001 : 0;

            nullsBufferContainsNull |= (value != 0);
            nullsBufferIndex = setByte(nullsBuffer, nullsBufferIndex, value);
            offset += BITS_IN_BYTE;
        }

        // Process the remaining positions
        remainingPositions &= 0b111;
        while (remainingNullsCount < remainingPositions) {
            remainingNullsFromLastBatch[remainingNullsCount++] = block.isNull(positions[offset++]);
        }
    }

    private void encodeNonNullsAsBits(int count)
    {
        if (remainingNullsCount + count < BITS_IN_BYTE) {
            // just put all of this batch to remainingNullsFromLastBatch
            for (int i = 0; i < count; i++) {
                remainingNullsFromLastBatch[remainingNullsCount++] = false;
            }
            return;
        }

        int remainingPositions = count - encodeRemainingNullsAsBits();
        int bytesCount = remainingPositions >>> 3;

        nullsBufferIndex = fill(nullsBuffer, nullsBufferIndex, bytesCount, (byte) 0);

        remainingPositions &= 0b111;
        while (remainingNullsCount < remainingPositions) {
            remainingNullsFromLastBatch[remainingNullsCount++] = false;
        }
    }

    private int encodeRemainingNullsAsBits()
    {
        if (remainingNullsCount == 0) {
            return 0;
        }

        byte value = 0;
        for (int i = 0; i < remainingNullsCount; i++) {
            value |= remainingNullsFromLastBatch[i] ? 0b1000_0000 >>> i : 0;
        }

        nullsBufferContainsNull |= (value != 0);
        nullsBufferIndex = setByte(nullsBuffer, nullsBufferIndex, value);

        int padding = BITS_IN_BYTE - remainingNullsCount;
        remainingNullsCount = 0;
        return padding;
    }

    protected long getNullsBufferSizeInBytes()
    {
        return nullsBufferIndex +                               // nulls buffer
                remainingNullsCount;   // the remaining nulls not serialized yet
    }

    protected long getNullsBufferRetainedSizeInBytes()
    {
        return sizeOf(nullsBuffer) +                    // nulls buffer
                sizeOf(remainingNullsFromLastBatch);    // the remaining nulls not serialized yet
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

    protected long getPositionsSizeInBytes()
    {
        if (mappedPositions != null) {
            return 2 * SIZE_OF_INT * positionCount;
        }
        return SIZE_OF_INT * positionCount;
    }

    protected long getPostionsRetainedSizeInBytes()
    {
        return sizeOf(positions) + sizeOf(mappedPositions);
    }

    private void ensureMappedPositionsCapacity(int capacity)
    {
        if (mappedPositions == null || this.mappedPositions.length < capacity) {
            mappedPositions = new int[max(INITIAL_POSITION_COUNT, capacity)];
        }
    }

    private void ensurePositionsCapacity(int capacity)
    {
        // This is for the nested level buffers and the positions array could be null at the beginning
        if (positions == null) {
            positions = new int[max(INITIAL_POSITION_COUNT, capacity)];
        }
        else if (this.positions.length < capacity) {
            positions = Arrays.copyOf(positions, max(positions.length * 2, capacity));
        }
    }

    private void ensureNullsValueBufferCapacity()
    {
        int capacity = (bufferedPositionCount + batchSize) / BITS_IN_BYTE + 1;

        if (nullsBuffer == null) {
            nullsBuffer = new byte[max(initialPositionCount / BITS_IN_BYTE, capacity)];
        }
        else if (nullsBuffer.length < capacity) {
            nullsBuffer = Arrays.copyOf(nullsBuffer, max(nullsBuffer.length * 2, capacity));
        }
    }
}
