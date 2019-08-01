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

import com.facebook.presto.spi.block.ColumnarRow;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.array.Arrays.ExpansionFactor.LARGE;
import static com.facebook.presto.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.UncheckedByteArrays.setIntUnchecked;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
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

    // This array holds the condensed offsets for each position for the incoming block.
    private int[] offsets;

    // The last offset in the offsets buffer
    private int lastOffset;

    // This array holds the offsets into its nested field blocks for each row in the RowBlock.
    private int[] offsetsCopy;

    // The AbstractBlockEncodingBuffer for the nested field blocks of the RowBlock
    private final BlockEncodingBuffer[] fieldBuffers;

    public RowBlockEncodingBuffer(DecodedBlockNode decodedBlockNode)
    {
        List<DecodedBlockNode> childrenNodes = decodedBlockNode.getChildren();
        fieldBuffers = new AbstractBlockEncodingBuffer[childrenNodes.size()];
        for (int i = 0; i < childrenNodes.size(); i++) {
            fieldBuffers[i] = createBlockEncodingBuffers(decodedBlockNode.getChildren().get(i));
        }
    }

    @Override
    public void accumulateSerializedRowSizes(int[] serializedRowSizes)
    {
        for (int i = 0; i < positionCount; i++) {
            serializedRowSizes[i] += POSITION_SIZE;
        }

        offsetsCopy = ensureCapacity(offsetsCopy, positionCount + 1);

        for (int i = 0; i < fieldBuffers.length; i++) {
            System.arraycopy(offsets, 0, offsetsCopy, 0, positionCount + 1);
            ((AbstractBlockEncodingBuffer) fieldBuffers[i]).accumulateSerializedRowSizes(offsetsCopy, positionCount, serializedRowSizes);
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
        resetNullsBuffer();

        for (int i = 0; i < fieldBuffers.length; i++) {
            fieldBuffers[i].resetBuffers();
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        int size = 0;
        for (int i = 0; i < fieldBuffers.length; i++) {
            size += fieldBuffers[i].getRetainedSizeInBytes();
        }

        return INSTANCE_SIZE +
                getPositionsRetainedSizeInBytes() +
                sizeOf(offsetsBuffer) +
                sizeOf(offsets) +
                sizeOf(offsetsCopy) +
                getNullsBufferRetainedSizeInBytes() +
                size;
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
    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        decodedBlockNode = mapPositionsToNestedBlock(decodedBlockNode);
        ColumnarRow columnarRow = (ColumnarRow) decodedBlockNode.getDecodedBlock();
        decodedBlock = columnarRow.getNullCheckBlock();

        populateNestedPositions(columnarRow);

        for (int i = 0; i < fieldBuffers.length; i++) {
            ((AbstractBlockEncodingBuffer) fieldBuffers[i]).setupDecodedBlockAndMapPositions(decodedBlockNode.getChildren().get(i));
        }
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

        offsetsCopy = ensureCapacity(offsetsCopy, positionCount + 1);

        for (int i = 0; i < fieldBuffers.length; i++) {
            System.arraycopy(positionOffsets, 0, offsetsCopy, 0, positionCount + 1);
            ((AbstractBlockEncodingBuffer) fieldBuffers[i]).accumulateSerializedRowSizes(offsetsCopy, positionCount, serializedRowSizes);
        }
    }

    private void populateNestedPositions(ColumnarRow columnarRow)
    {
        // Reset nested level positions before checking positionCount. Failing to do so may result in elementsBuffers having stale values when positionCount is 0.
        for (int i = 0; i < fieldBuffers.length; i++) {
            ((AbstractBlockEncodingBuffer) fieldBuffers[i]).resetPositions();
        }

        if (positionCount == 0) {
            return;
        }

        offsets = ensureCapacity(offsets, positionCount + 1);

        int[] positions = getPositions();

        int columnarRowBaseOffset = columnarRow.getOffset(0);
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            int beginOffset = columnarRow.getOffset(position);
            int endOffset = columnarRow.getOffset(position + 1);  // if the row is null, endOffsetInBlock == beginOffsetInBlock
            int currentRowSize = endOffset - beginOffset;
            offsets[i + 1] = offsets[i] + currentRowSize;

            if (currentRowSize > 0) {
                for (int j = 0; j < fieldBuffers.length; j++) {
                    ((AbstractBlockEncodingBuffer) fieldBuffers[j]).appendPositionRange(beginOffset - columnarRowBaseOffset, currentRowSize);
                }
            }
        }
    }

    private void appendOffsets()
    {
        offsetsBuffer = ensureCapacity(offsetsBuffer, offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE, LARGE, PRESERVE);

        int baseOffset = lastOffset - offsets[positionsOffset];
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            offsetsBufferIndex = setIntUnchecked(offsetsBuffer, offsetsBufferIndex, offsets[i + 1] + baseOffset);
        }
        lastOffset = offsets[positionsOffset + batchSize] + baseOffset;
    }
}
