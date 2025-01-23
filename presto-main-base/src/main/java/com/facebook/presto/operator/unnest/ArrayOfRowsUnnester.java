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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarRow;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * Unnester for a nested column with array type, only when array elements are of {@code RowType} type.
 * It maintains {@link ColumnarArray} and {@link ColumnarRow} objects to get underlying elements. The two
 * different columnar structures are required because there are two layers of translation involved. One
 * from {@code ArrayBlock} to {@code RowBlock}, and then from {@code RowBlock} to individual element blocks.
 * <p>
 * All protected methods implemented here assume that they are invoked when {@code columnarArray} and
 * {@code columnarRow} are non-null.
 */
class ArrayOfRowsUnnester
        implements Unnester
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayOfRowsUnnester.class).instanceSize();

    private final int fieldCount;
    private final UnnestBlockBuilder[] unnestBlockBuilders;

    private int[] lengths = new int[0];
    private ColumnarArray columnarArray;
    private ColumnarRow columnarRow;

    public ArrayOfRowsUnnester(int fieldCount)
    {
        unnestBlockBuilders = createUnnestBlockBuilder(fieldCount);
        this.fieldCount = fieldCount;
    }

    @Override
    public int getChannelCount()
    {
        return fieldCount;
    }

    @Override
    public void resetInput(Block block)
    {
        requireNonNull(block, "block is null");

        columnarArray = toColumnarArray(block);
        columnarRow = toColumnarRow(columnarArray.getElementsBlock());

        for (int i = 0; i < fieldCount; i++) {
            unnestBlockBuilders[i].resetInputBlock(columnarRow.getField(i), columnarRow.getNullCheckBlock());
        }

        int positionCount = block.getPositionCount();
        lengths = ensureCapacity(lengths, positionCount);
        for (int j = 0; j < positionCount; j++) {
            lengths[j] = columnarArray.getLength(j);
        }
    }

    @Override
    public int[] getLengths()
    {
        return lengths;
    }

    @Override
    public Block[] buildOutputBlocks(int[] maxLengths, int startPosition, int batchSize, int currentBatchTotalLength)
    {
        boolean nullRequired = needToInsertNulls(startPosition, batchSize, currentBatchTotalLength);

        Block[] outputBlocks = new Block[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            if (nullRequired) {
                outputBlocks[i] = unnestBlockBuilders[i].buildOutputBlockWithNulls(maxLengths, startPosition, batchSize, currentBatchTotalLength, this.lengths);
            }
            else {
                outputBlocks[i] = unnestBlockBuilders[i].buildOutputBlockWithoutNulls(currentBatchTotalLength);
            }
        }
        return outputBlocks;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // The lengths array in unnestBlockBuilders is the same object as in the unnester and doesn't need to be counted again.
        return INSTANCE_SIZE + sizeOf(lengths);
    }

    private static UnnestBlockBuilder[] createUnnestBlockBuilder(int fieldCount)
    {
        UnnestBlockBuilder[] builders = new UnnestBlockBuilder[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            builders[i] = new UnnestBlockBuilder();
        }

        return builders;
    }

    private boolean needToInsertNulls(int offset, int length, int currentBatchTotalLength)
    {
        int start = columnarArray.getOffset(offset);
        int end = columnarArray.getOffset(offset + length);
        int totalLength = end - start;

        if (totalLength < currentBatchTotalLength) {
            return true;
        }

        if (columnarRow.getNullCheckBlock().mayHaveNull()) {
            for (int i = start; i < end; i++) {
                if (columnarRow.isNull(i)) {
                    return true;
                }
            }
        }

        return false;
    }
}
