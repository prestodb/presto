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
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.SliceOutput;

import java.util.ArrayList;
import java.util.Arrays;

import static com.facebook.presto.operator.ByteArrayUtils.writeLengthPrefixedString;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class RowBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    public static final String NAME = "ROW";

    private byte[] offsetsBuffer;
    private int offsetsBufferIndex;
    private int[] offsets;          // This array holds the condensed offsets for each position
    private int[][] positionOffsets;  // This array holds the offsets into nested block for each top level row

    private ColumnarRow columnarRow;
    private BlockEncodingBuffers[] rawFieldBlockBuffers;

    RowBlockEncodingBuffers(PartitionedOutputOperator.DecodedObjectNode columnarRowBlockNode, int initialBufferSize)
    {
        this.initialBufferSize = initialBufferSize;

        ArrayList<PartitionedOutputOperator.DecodedObjectNode> childrenNodes = columnarRowBlockNode.getChildren();
        rawFieldBlockBuffers = new BlockEncodingBuffers[childrenNodes.size()];
        for (int i = 0; i < childrenNodes.size(); i++) {
            rawFieldBlockBuffers[i] = createBlockEncodingBuffers(columnarRowBlockNode.getChild(i), initialBufferSize);
        }

        prepareBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        // TODO: These local buffers will be requested from the buffer pools in the future.
        if (offsetsBuffer == null) {
            offsetsBuffer = new byte[initialBufferSize * ARRAY_INT_INDEX_SCALE];
        }
        offsetsBufferIndex = 0;

        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            rawFieldBlockBuffers[i].prepareBuffers();
        }
    }

    @Override
    protected void resetBuffers()
    {
        bufferedPositionCount = 0;
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        nullsBufferContainsNull = false;
        offsetsBufferIndex = 0;

        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            rawFieldBlockBuffers[i].resetBuffers();
        }
    }

    @Override
    protected void setupDecodedBlockAndMapPositions(PartitionedOutputOperator.DecodedObjectNode decodedObjectNode)
    {
        mapPositions(decodedObjectNode);

        populateNestedPositions();

        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            rawFieldBlockBuffers[i].setupDecodedBlockAndMapPositions(decodedObjectNode.getChild(i));
        }
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

        ensurePositionOffsetsSize(positionCount);

        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            // Nested level BlockEncodingBuffers will modify the positionOffsets array in place,
            // so we need to make a copy for each rawFieldBlockBuffers.
            int[] currentPositionOffsets = positionOffsets[i];
            System.arraycopy(offsets, 0, currentPositionOffsets, 0, positionCount);
            rawFieldBlockBuffers[i].accumulateRowSizes(currentPositionOffsets, positionCount, rowSizes);
        }
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

        ensurePositionOffsetsSize(positionCount);
        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            // Nested level BlockEncodingBuffers will modify the positionOffsets array in place,
            // so we need to make a copy for each rawFieldBlockBuffers.
            int[] destinationPositionOffsets = this.positionOffsets[i];
            System.arraycopy(positionOffsets, 0, destinationPositionOffsets, 0, positionCount);
            rawFieldBlockBuffers[i].accumulateRowSizes(destinationPositionOffsets, positionCount, rowSizes);
        }
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

        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            rawFieldBlockBuffers[i].setNextBatch(nestedLevelPositionBegin, nestedLevelPositionEnd - nestedLevelPositionBegin);
        }
    }

    @Override
    protected void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        int[] positions = isPositionsMapped ? mappedPositions : this.positions;
        verify(positionsOffset + batchSize - 1 < positions.length && positions[positionsOffset] >= 0 &&
                positions[positionsOffset + batchSize - 1] < columnarRow.getPositionCount());

        appendNulls();
        appendOffsets();

        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            rawFieldBlockBuffers[i].copyValues();
        }

        bufferedPositionCount += batchSize;
    }

    @Override
    protected void writeTo(SliceOutput sliceOutput)
    {
        // TODO: getSizeInBytes() was calculated in flush and doesnt need to recalculated
        verify(getSizeInBytes() <= sliceOutput.writableBytes());

        writeLengthPrefixedString(sliceOutput, NAME);
        sliceOutput.writeInt(rawFieldBlockBuffers.length);
        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            rawFieldBlockBuffers[i].writeTo(sliceOutput);
        }

        sliceOutput.writeInt(bufferedPositionCount); //positionCount
        sliceOutput.writeInt(0);  // the base position
        sliceOutput.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);

        writeNullsTo(sliceOutput);
    }

    @Override
    protected int getSizeInBytes()
    {
        int rawFieldBlockBuffersSize = 0;
        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            rawFieldBlockBuffersSize += rawFieldBlockBuffers[i].getSizeInBytes();
        }

        return NAME.length() + SIZE_OF_INT +        // encoding name
                SIZE_OF_INT +                       // field count
                rawFieldBlockBuffersSize +          // field blocks
                SIZE_OF_INT +                       // positionCount
                SIZE_OF_INT +                       // offset 0. The offsetsBuffer doesn't contain the offset 0 so we need to add it here.
                offsetsBufferIndex +                // offsets buffer.
                SIZE_OF_BYTE +                      // nulls uses 1 byte for mayHaveNull
                nullsBufferIndex +                  // nulls buffer
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

        columnarRow = (ColumnarRow) decodedObjectNode.getDecodedBlock();
        decodedBlock = columnarRow.getNullCheckBlock();
    }

    private void populateNestedPositions()
    {
        if (positionCount == 0) {
            return;
        }

        verify(positionsOffset == 0);
        verify(columnarRow != null);

        ensureOffsetsSize();

        // Nested level positions always start from 0.
        for (int i = 0; i < rawFieldBlockBuffers.length; i++) {
            rawFieldBlockBuffers[i].positionsOffset = 0;
            rawFieldBlockBuffers[i].positionCount = 0;
            rawFieldBlockBuffers[i].isPositionsMapped = false;
        }

        int[] positions = isPositionsMapped ? mappedPositions : this.positions;

        int lastOffset = 0;
        int columnarArrayBaseOffset = columnarRow.getOffset(0);
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            int beginOffsetInBlock = columnarRow.getOffset(position);
            int endOffsetInBlock = columnarRow.getOffset(position + 1);  // if the row is null, endOffsetInBlock == beginOffsetInBlock
            int currentRowSize = endOffsetInBlock - beginOffsetInBlock;
            int currentOffset = lastOffset + currentRowSize;

            offsets[i] = currentOffset;

            if (currentRowSize > 0) {
                for (int j = 0; j < rawFieldBlockBuffers.length; j++) {
                    rawFieldBlockBuffers[j].appendPositionRange(beginOffsetInBlock - columnarArrayBaseOffset, currentRowSize);
                }
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

    private void ensurePositionOffsetsSize(int positionCount)
    {
        if (positionOffsets == null || positionOffsets[0].length < positionCount) {
            positionOffsets = new int[rawFieldBlockBuffers.length][positionCount * 2];
        }
    }
    private void ensureOffsetsBufferSize()
    {
        int requiredSize = offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE;
        if (requiredSize > offsetsBuffer.length) {
            offsetsBuffer = Arrays.copyOf(offsetsBuffer, max(requiredSize, offsetsBuffer.length * 2));
        }
    }
}
