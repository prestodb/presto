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
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.Int128ArrayBlock;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.block.ShortArrayBlock;
import com.facebook.presto.spi.block.VariableWidthBlock;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.max;

public abstract class BlockEncodingBuffers
{
    private static final int BITS_IN_BYTE = 8;
    private static final int INITIAL_POSITION_COUNT = 1024;

    protected int[] positions;
    protected int[] mappedPositions;
    protected int positionCount;
    protected boolean isPositionsMapped;

    protected int positionsOffset;  // each batch we copy the values of rows from positions[positionsOffset] to positions[positionsOffset + batchSize]
    protected int batchSize;
    protected int bufferedPositionCount;

    protected int initialBufferSize;

    private byte[] nullsBuffer;
    protected int nullsBufferIndex;  //The next byte address if new values to be added.
    protected boolean[] remainingNullsFromLastBatch = new boolean[BITS_IN_BYTE];
    protected int remainingNullsCount;
    protected boolean nullsBufferContainsNull;

    protected Block decodedBlock;

    public static BlockEncodingBuffers createBlockEncodingBuffers(PartitionedOutputOperator.DecodedObjectNode decodedObjectNode, int initialBufferSize)
    {
        Object decodedBlock = decodedObjectNode.getDecodedBlock();

        // Skip the Dictionary/Rle block node. The mapping info is not needed when creating buffers.
        if (decodedBlock instanceof DictionaryBlock) {
            DictionaryBlock dictionaryBlock = (DictionaryBlock) decodedBlock;
            decodedBlock = dictionaryBlock.getDictionary();
        }
        else if (decodedBlock instanceof RunLengthEncodedBlock) {
            decodedBlock = ((RunLengthEncodedBlock) decodedBlock).getValue();
        }

        if (decodedBlock instanceof LongArrayBlock) {
            return new LongArrayBlockEncodingBuffers(initialBufferSize);
        }
        else if (decodedBlock instanceof VariableWidthBlock) {
            return new VariableWidthBlockEncodingBuffers(initialBufferSize);
        }
        else if (decodedBlock instanceof IntArrayBlock) {
            return new IntArrayBlockEncodingBuffers(initialBufferSize);
        }
        else if (decodedBlock instanceof ByteArrayBlock) {
            return new ByteArrayBlockEncodingBuffers(initialBufferSize);
        }
        else if (decodedBlock instanceof ShortArrayBlock) {
            return new ShortArrayBlockEncodingBuffers(initialBufferSize);
        }
        else if (decodedBlock instanceof ColumnarArray) {
            return new ArrayBlockEncodingBuffers(decodedObjectNode, initialBufferSize);
        }
        else if (decodedBlock instanceof ColumnarMap) {
            return new MapBlockEncodingBuffers(decodedObjectNode, initialBufferSize);
        }
        else if (decodedBlock instanceof ColumnarRow) {
            return new RowBlockEncodingBuffers(decodedObjectNode, initialBufferSize);
        }
        else if (decodedBlock instanceof Int128ArrayBlock) {
            return new Int128ArrayBlockEncodingBuffers(initialBufferSize);
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding: " + ((Block) decodedBlock).getEncodingName());
        }
    }

    protected abstract void prepareBuffers();

    protected abstract void resetBuffers();

    protected abstract void copyValues();

    protected abstract void writeTo(SliceOutput sliceOutput);

    protected abstract int getSizeInBytes();

    protected abstract void accumulateRowSizes(int[] rowSizes);

    protected abstract void accumulateRowSizes(int[] offsetsBuffer, int positionCount, int[] rowSizes);

    protected void setupDecodedBlocksAndPositions(PartitionedOutputOperator.DecodedObjectNode decodedObjectNode, int[] positions, int positionCount)
    {
        checkArgument(decodedObjectNode != null);

        this.positions = positions;
        this.positionCount = positionCount;
        this.positionsOffset = 0;

        setupDecodedBlockAndMapPositions(decodedObjectNode);
    }

    protected void setupDecodedBlockAndMapPositions(PartitionedOutputOperator.DecodedObjectNode decodedObjectNode)
    {
        Object decodedObject = decodedObjectNode.getDecodedBlock();

        if (decodedObject instanceof DictionaryBlock) {
            DictionaryBlock dictionaryBlock = (DictionaryBlock) decodedObject;
            mapPositionsForDictionary(dictionaryBlock);

            decodedBlock = dictionaryBlock.getDictionary();
            isPositionsMapped = true;
        }
        else if (decodedObject instanceof RunLengthEncodedBlock) {
            RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) decodedObject;
            mapPositionsForRle();

            decodedBlock = rleBlock.getValue();
            isPositionsMapped = true;
        }
        else {
            decodedBlock = (Block) decodedObject;
            isPositionsMapped = false;
        }
    }

    protected void setPositions(int[] positions)
    {
        this.positions = positions;
    }

    protected void mapPositionsForDictionary(DictionaryBlock dictionaryBlock)
    {
        ensureMappedPositionsCapacity();
        for (int i = 0; i < positionCount; i++) {
            mappedPositions[i] = dictionaryBlock.getId(positions[i]);
        }
    }

    protected void mapPositionsForRle()
    {
        ensureMappedPositionsCapacity();
        Arrays.fill(mappedPositions, 0, positionCount, 0);
    }

    protected void setNextBatch(int positionsOffset, int batchSize)
    {
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;
    }

    protected void appendPositionRange(int offset, int addedLength)
    {
        ensurePositionsCapacity(positionCount + addedLength);
        for (int i = 0; i < addedLength; i++) {
            positions[positionCount++] = offset + i;
        }
    }

    protected void appendNulls()
    {
        if (decodedBlock.mayHaveNull()) {
            // We write to the buffer if there're potential nulls. Note that even though the decodedBlock may contains null, the rows that go to this partition may not.
            // But we still write them to nullsBuffer anyways because of performance considerations.
            ensureNullsValueBufferSize();
            int bufferedNullsCount = nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount;

            if (bufferedPositionCount > bufferedNullsCount) {
                // THere were no nulls for the rows in (bufferedNullsCount, bufferedPositionCount]. Backfill them as all 0's
                int falsesToWrite = bufferedPositionCount - bufferedNullsCount;
                encodeFalseValuesAsBits(falsesToWrite);
                verify(nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount == bufferedPositionCount);
            }

            // Append this batch
            encodeNullsAsBits(decodedBlock);

            verify(nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount == bufferedPositionCount + batchSize);
        }
        else {
            if (containsNull()) {
                // There were nulls in previously buffered rows, but for this batch there can't be any nulls. Any how we need to append 0's for this batch.
                verify(nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount == bufferedPositionCount);

                ensureNullsValueBufferSize();
                encodeFalseValuesAsBits(batchSize);

                verify(nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount == bufferedPositionCount + batchSize);
            }
        }
    }

    protected void writeNullsTo(SliceOutput sliceOutput)
    {
        if (remainingNullsCount > 0) {
            encodeRemainingNullsAsBits();
            remainingNullsCount = 0;
        }

        if (nullsBufferContainsNull) {
            if (!((nullsBufferIndex - 1) * BITS_IN_BYTE < bufferedPositionCount && nullsBufferIndex * BITS_IN_BYTE >= bufferedPositionCount)) {
            }
            verify((nullsBufferIndex - 1) * BITS_IN_BYTE < bufferedPositionCount && nullsBufferIndex * BITS_IN_BYTE >= bufferedPositionCount);

            sliceOutput.writeBoolean(true);
            sliceOutput.appendBytes(nullsBuffer, 0, nullsBufferIndex);
        }
        else {
            sliceOutput.writeBoolean(false);
        }
    }

    private boolean containsNull()
    {
        return nullsBufferContainsNull || remainingNullsContainsNulls();
    }

    private boolean remainingNullsContainsNulls()
    {
        boolean result = false;
        for (int i = 0; i < remainingNullsCount; i++) {
            result |= remainingNullsFromLastBatch[i];
        }
        return result;
    }

    private void encodeNullsAsBits(Block block)
    {
        int[] positions = isPositionsMapped ? mappedPositions : this.positions;

        if (remainingNullsCount + batchSize < BITS_IN_BYTE) {
            // just put all of this batch to remainingNullsFromLastBatch
            for (int offset = positionsOffset; offset < positionsOffset + batchSize; offset++) {
                int position = positions[offset];
                remainingNullsFromLastBatch[remainingNullsCount++] = block.isNull(position);
            }
            return;
        }

        // Process the remaining nulls from last batch
        int offset = positionsOffset;

        if (remainingNullsCount > 0) {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int i = 0; i < remainingNullsCount; i++) {
                value |= remainingNullsFromLastBatch[i] ? mask : 0;
                mask >>>= 1;
            }

            // Process the next a few nulls to make up one byte
            int positionCountForFirstByte = BITS_IN_BYTE - remainingNullsCount;
            int endPositionOffset = positionsOffset + positionCountForFirstByte;
            while (offset < endPositionOffset) {
                int position = positions[offset];
                value |= block.isNull(position) ? mask : 0;
                mask >>>= 1;
                offset++;
            }
            nullsBufferContainsNull |= (value != 0);
            ByteArrayUtils.writeByte(nullsBuffer, nullsBufferIndex++, value);
        }

        // Process the next BITS_IN_BYTE * n positions. We now have processed (offset - positionsOffset) positions
        int remainingPositions = batchSize - (offset - positionsOffset);
        int positionsToEncode = remainingPositions & ~0b111;
        int endPositionOffset = offset + positionsToEncode;
        while (offset < endPositionOffset) {
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
            ByteArrayUtils.writeByte(nullsBuffer, nullsBufferIndex, value);
            nullsBufferIndex++;
            offset += BITS_IN_BYTE;
        }

        remainingPositions &= 0b111;
        verify(remainingPositions < BITS_IN_BYTE);
        remainingNullsCount = 0;
        while (remainingNullsCount < remainingPositions) {
            int position = positions[offset++];
            remainingNullsFromLastBatch[remainingNullsCount++] = block.isNull(position);
        }
    }

    private void encodeFalseValuesAsBits(int count)
    {
        if (remainingNullsCount + count < BITS_IN_BYTE) {
            // just put all of this batch to remainingNullsFromLastBatch
            for (int i = 0; i < count; i++) {
                remainingNullsFromLastBatch[remainingNullsCount++] = false;
            }
            return;
        }

        // Have to do this before calling encodeRemainingNullsAsBits() because it resets remainingNullsCount
        int remainingPositions = count;

        // Process the remaining nulls from last batch
        if (remainingNullsCount > 0) {
            encodeRemainingNullsAsBits();
            remainingPositions -= (BITS_IN_BYTE - remainingNullsCount);
            remainingNullsCount = 0;
        }

        int bytesCount = remainingPositions >>> 3;

        ByteArrayUtils.fill(nullsBuffer, nullsBufferIndex, bytesCount, (byte) 0);
        nullsBufferIndex += bytesCount;

        remainingPositions &= 0b111;
        remainingNullsCount = 0;
        while (remainingNullsCount < remainingPositions) {
            remainingNullsFromLastBatch[remainingNullsCount++] = false;
        }
    }

    private void encodeRemainingNullsAsBits()
    {
        byte value = 0;
        int mask = 0b1000_0000;
        for (int i = 0; i < remainingNullsCount; i++) {
            value |= remainingNullsFromLastBatch[i] ? mask : 0;
            mask >>>= 1;
        }

        nullsBufferContainsNull |= (value != 0);
        ByteArrayUtils.writeByte(nullsBuffer, nullsBufferIndex, value);
        nullsBufferIndex++;
    }

    private void ensurePositionsCapacity(int length)
    {
        if (positions == null) {
            positions = new int[INITIAL_POSITION_COUNT];
        }
        else if (this.positions.length < length) {
            positions = Arrays.copyOf(positions, max(positions.length * 2, length));
        }
    }

    private void ensureMappedPositionsCapacity()
    {
        if (this.mappedPositions == null || this.mappedPositions.length < positionCount) {
            mappedPositions = new int[positionCount * 2];
        }
    }

    private void ensureNullsValueBufferSize()
    {
        // This may overestimate the size by 1 byte to allow faster computation
        int requiredBytes = (bufferedPositionCount + batchSize) / BITS_IN_BYTE + 1;

        if (nullsBuffer == null) {
            int newSize = max(requiredBytes, initialBufferSize / BITS_IN_BYTE);
            nullsBuffer = new byte[newSize];
        }
        else if (nullsBuffer.length < requiredBytes) {
            int newSize = max(requiredBytes, nullsBuffer.length * 2);
            nullsBuffer = Arrays.copyOf(nullsBuffer, newSize);
        }
    }
}
