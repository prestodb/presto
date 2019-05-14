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
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.TypeSerde;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.operator.ByteArrayUtils.writeLengthPrefixedString;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class MapBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    public static final String NAME = "MAP";

    private static final int HASH_MULTIPLIER = 2;

    private byte[] hashTablesBuffer;
    private int hashTableBufferIndex;
    private boolean noHashTables;

    private byte[] offsetsBuffer;
    private int offsetsBufferIndex;

    private int[] offsets;          // This array holds the condensed offsets for each position
    private int[] positionOffsets;  // This array holds the offsets into the above offsets array to mark the top level row boundaries for the nested block

    private ColumnarMap columnarMap;
    private BlockEncodingBuffers rawKeyBlockBuffer;
    private BlockEncodingBuffers rawValueBlockBuffer;

    MapBlockEncodingBuffers(PartitionedOutputOperator.DecodedObjectNode columnarMapBlockNode, int initialBufferSize)
    {
        verify(columnarMapBlockNode.getChildren().size() == 2);

        this.initialBufferSize = initialBufferSize;

        rawKeyBlockBuffer = createBlockEncodingBuffers(columnarMapBlockNode.getChild(0), initialBufferSize);
        rawValueBlockBuffer = createBlockEncodingBuffers(columnarMapBlockNode.getChild(1), initialBufferSize);

        prepareBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        if (hashTablesBuffer == null) {
            hashTablesBuffer = new byte[initialBufferSize * ARRAY_INT_INDEX_SCALE * HASH_MULTIPLIER];
        }
        hashTableBufferIndex = 0;

        if (offsetsBuffer == null) {
            offsetsBuffer = new byte[initialBufferSize * ARRAY_INT_INDEX_SCALE];
        }
        offsetsBufferIndex = 0;

        rawKeyBlockBuffer.prepareBuffers();
        rawValueBlockBuffer.prepareBuffers();
    }

    @Override
    protected void resetBuffers()
    {
        bufferedPositionCount = 0;
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        nullsBufferContainsNull = false;
        offsetsBufferIndex = 0;
        hashTableBufferIndex = 0;
        noHashTables = false;

        rawKeyBlockBuffer.resetBuffers();
        rawValueBlockBuffer.resetBuffers();
    }

    @Override
    protected void setupDecodedBlockAndMapPositions(PartitionedOutputOperator.DecodedObjectNode columnarMapBlockNode)
    {
        verify(columnarMapBlockNode.getChildren().size() == 2);

        mapPositions(columnarMapBlockNode);

        populateNestedPositions();

        rawKeyBlockBuffer.setupDecodedBlockAndMapPositions(columnarMapBlockNode.getChild(0));
        rawValueBlockBuffer.setupDecodedBlockAndMapPositions(columnarMapBlockNode.getChild(1));
    }

    @Override
    protected void accumulateRowSizes(int[] rowSizes)
    {
        // Top level positionCount should be greater than 0
        verify(positionCount > 0);

        int averageElementSize = Integer.BYTES + Byte.BYTES;

        for (int i = 0; i < positionCount; i++) {
            rowSizes[i] += averageElementSize;
        }

        ensurePositionOffsetsSize();
        System.arraycopy(offsets, 0, positionOffsets, 0, positionCount);

        rawKeyBlockBuffer.accumulateRowSizes(positionOffsets, positionCount, rowSizes);
        rawValueBlockBuffer.accumulateRowSizes(positionOffsets, positionCount, rowSizes);
    }

    @Override
    protected void accumulateRowSizes(int[] positionOffsets, int positionCount, int[] rowSizes)
    {
        // The nested level positionCount could be 0.
        if (this.positionCount == 0) {
            return;
        }

        int averageElementSize = Integer.BYTES + Byte.BYTES;

        int lastOffset = 0;
        for (int i = 0; i < positionCount; i++) {
            int currentOffset = positionOffsets[i];

            int entryCount = currentOffset - lastOffset;
            rowSizes[i] += averageElementSize * entryCount;

            positionOffsets[i] = currentOffset == 0 ? 0 : this.offsets[currentOffset - 1];

            lastOffset = currentOffset;
        }

        // this.offsetsBuffer and Next level positions should have already be written
        rawKeyBlockBuffer.accumulateRowSizes(positionOffsets, positionCount, rowSizes);
        rawValueBlockBuffer.accumulateRowSizes(positionOffsets, positionCount, rowSizes);
    }

    @Override
    protected void setNextBatch(int positionsOffset, int batchSize)
    {
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;

        // The nested level positionCount could be 0.
        if (this.positionCount == 0) {
            return;
        }

        int nestedLevelPositionBegin = positionsOffset == 0 ? 0 : offsets[positionsOffset - 1];
        int nestedLevelPositionEnd = offsets[positionsOffset + batchSize - 1];

        rawKeyBlockBuffer.setNextBatch(nestedLevelPositionBegin, nestedLevelPositionEnd - nestedLevelPositionBegin);
        rawValueBlockBuffer.setNextBatch(nestedLevelPositionBegin, nestedLevelPositionEnd - nestedLevelPositionBegin);
    }

    @Override
    protected void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        int[] positions = isPositionsMapped ? mappedPositions : this.positions;
        verify(positionsOffset + batchSize - 1 < positions.length && positions[positionsOffset] >= 0 &&
                positions[positionsOffset + batchSize - 1] < columnarMap.getPositionCount());

        appendNulls();
        appendOffsets();
        appendHashTables();

        rawKeyBlockBuffer.copyValues();
        rawValueBlockBuffer.copyValues();

        bufferedPositionCount += batchSize;
    }

    @Override
    protected void writeTo(SliceOutput sliceOutput)
    {
        // TODO: getSizeInBytes() was calculated in flush and doesnt need to recalculated
        verify(getSizeInBytes() <= sliceOutput.writableBytes());
        writeLengthPrefixedString(sliceOutput, NAME);
        TypeSerde.writeType(sliceOutput, columnarMap.getKeyType());
        rawKeyBlockBuffer.writeTo(sliceOutput);
        rawValueBlockBuffer.writeTo(sliceOutput);

        // Hashtables
        int hashTableSize = getLastOffsetInBuffer() * HASH_MULTIPLIER;
        verify(hashTableBufferIndex == 0 || hashTableSize * ARRAY_INT_INDEX_SCALE == hashTableBufferIndex);

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

        writeNullsTo(sliceOutput);
    }

    @Override
    protected int getSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +            // length prefixed encoding name
                columnarMap.getKeyType().getTypeSignature().toString().length() + SIZE_OF_INT + // length prefixed type string
                rawKeyBlockBuffer.getSizeInBytes() +    // nested key block
                rawValueBlockBuffer.getSizeInBytes() +  // nested value block
                SIZE_OF_INT +                           // hash tables size
                hashTableBufferIndex +                  // hash tables
                SIZE_OF_INT +                           // positionCount
                offsetsBufferIndex + SIZE_OF_INT +      // offsets buffer.
                SIZE_OF_BYTE +                          // nulls uses 1 byte for mayHaveNull
                nullsBufferIndex +                      // nulls buffer
                (remainingNullsCount > 0 ? SIZE_OF_BYTE : 0); // the remaining nulls not serialized yet
    }

    private void mapPositions(PartitionedOutputOperator.DecodedObjectNode decodedObjectNode)
    {
        // Remap positions
        Object decodedObject = decodedObjectNode.getDecodedBlock();

        if (decodedObject instanceof DictionaryBlock) {
            DictionaryBlock dictionaryBlock = (DictionaryBlock) decodedObject;
            mapPositionsForDictionary(dictionaryBlock);

            decodedObjectNode = decodedObjectNode.getChild(0);

            isPositionsMapped = true;
        }
        else if (decodedObject instanceof RunLengthEncodedBlock) {
            //RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) decodedObject;
            mapPositionsForRle();

            decodedObjectNode = decodedObjectNode.getChild(0);

            isPositionsMapped = true;
        }
        else {
            isPositionsMapped = false;
        }

        columnarMap = (ColumnarMap) decodedObjectNode.getDecodedBlock();
        decodedBlock = columnarMap.getNullCheckBlock();
    }

    private void populateNestedPositions()
    {
        if (positionCount == 0) {
            return;
        }

        verify(positionsOffset == 0);
        verify(columnarMap != null);

        ensureOffsetsSize();

        // Nested level positions always start from 0.
        rawKeyBlockBuffer.positionsOffset = 0;
        rawKeyBlockBuffer.positionCount = 0;
        rawKeyBlockBuffer.isPositionsMapped = false;

        rawValueBlockBuffer.positionsOffset = 0;
        rawValueBlockBuffer.positionCount = 0;
        rawValueBlockBuffer.isPositionsMapped = false;

        int[] positions = isPositionsMapped ? mappedPositions : this.positions;

        int lastOffset = 0;
        int columnarArrayBaseOffset = columnarMap.getOffset(0);
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            int beginOffsetInBlock = columnarMap.getOffset(position);
            int endOffsetInBlock = columnarMap.getOffset(position + 1);
            int currentRowSize = endOffsetInBlock - beginOffsetInBlock;
            int currentOffset = lastOffset + currentRowSize;

            offsets[i] = currentOffset;

            beginOffsetInBlock -= columnarArrayBaseOffset;
            if (currentRowSize > 0) {
                rawKeyBlockBuffer.appendPositionRange(beginOffsetInBlock, currentRowSize);
                rawValueBlockBuffer.appendPositionRange(beginOffsetInBlock, currentRowSize);
            }

            lastOffset = currentOffset;
        }
    }

    private void appendOffsets()
    {
        ensureOffsetsBufferSize();

        int baseOffset = getLastOffsetInBuffer();

        if (positionsOffset > 0) {
            baseOffset -= offsets[positionsOffset - 1];
        }

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int currentOffset = offsets[i];
            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset + baseOffset);
            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;
        }

        verify(offsetsBufferIndex == (bufferedPositionCount + batchSize) * ARRAY_INT_INDEX_SCALE);
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

        int[] positions = isPositionsMapped ? mappedPositions : this.positions;

        int[] hashTable = hashTables.get();

        int beginHashTableBufferIndex = hashTableBufferIndex;

        int beginOffset = positionsOffset == 0 ? 0 : offsets[positionsOffset - 1];
        int endOffset = offsets[positionsOffset + batchSize - 1];
        int hashTablesSize = (endOffset - beginOffset) * HASH_MULTIPLIER;

        ensureHashTablesBufferSize(hashTableBufferIndex + hashTablesSize * ARRAY_INT_INDEX_SCALE);

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];

            int beginOffsetInBlock = columnarMap.getOffset(position);
            int endOffsetInBlock = columnarMap.getOffset(position + 1);

            hashTableBufferIndex = ByteArrayUtils.writeInts(
                    hashTablesBuffer,
                    hashTableBufferIndex,
                    hashTable,
                    beginOffsetInBlock * HASH_MULTIPLIER,
                    (endOffsetInBlock - beginOffsetInBlock) * HASH_MULTIPLIER);
        }

        verify(hashTablesSize * ARRAY_INT_INDEX_SCALE == hashTableBufferIndex - beginHashTableBufferIndex);
        verify(getLastOffsetInBuffer() * ARRAY_INT_INDEX_SCALE * HASH_MULTIPLIER == hashTableBufferIndex);
    }

    private int getLastOffsetInBuffer()
    {
        int offset = 0;
        if (offsetsBufferIndex > 0) {
            // There're already some values in the buffer
            offset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
        }
        return offset;
    }

    private void ensureOffsetsSize()
    {
        if (offsets == null || offsets.length < positionCount) {
            offsets = new int[positionCount * 2];
        }
    }

    private void ensurePositionOffsetsSize()
    {
        if (positionOffsets == null || positionOffsets.length < positionCount) {
            positionOffsets = new int[positionCount * 2];
        }
    }

    private void ensureOffsetsBufferSize()
    {
        int requiredSize = offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE;
        if (requiredSize > offsetsBuffer.length) {
            offsetsBuffer = Arrays.copyOf(offsetsBuffer, max(requiredSize, offsetsBuffer.length * 2));
        }
    }

    private void ensureHashTablesBufferSize(int requiredSize)
    {
        if (requiredSize > hashTablesBuffer.length) {
            hashTablesBuffer = Arrays.copyOf(hashTablesBuffer, max(requiredSize, hashTablesBuffer.length * 2));
        }
    }
}
