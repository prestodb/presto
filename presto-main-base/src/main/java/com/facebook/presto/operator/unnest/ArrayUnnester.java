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
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * Unnester for a nested column with array type, only when array elements are NOT of type {@code RowType}.
 * Maintains a {@link ColumnarArray} object to get underlying elements block from the array block.
 * <p>
 * All protected methods implemented here assume that they are being invoked when {@code columnarArray} is non-null.
 */
class ArrayUnnester
        implements Unnester
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayUnnester.class).instanceSize();

    private final UnnestBlockBuilder unnestBlockBuilder = new UnnestBlockBuilder();

    private int[] lengths = new int[0];
    private ColumnarArray columnarArray;

    @Override
    public int getChannelCount()
    {
        return 1;
    }

    @Override
    public void resetInput(Block block)
    {
        requireNonNull(block, "block is null");

        columnarArray = toColumnarArray(block);
        unnestBlockBuilder.resetInputBlock(columnarArray.getElementsBlock());

        int positionCount = block.getPositionCount();
        lengths = ensureCapacity(lengths, positionCount);
        for (int i = 0; i < positionCount; i++) {
            lengths[i] = columnarArray.getLength(i);
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
        boolean nullRequired = (columnarArray.getOffset(startPosition + batchSize) - columnarArray.getOffset(startPosition)) < currentBatchTotalLength;

        Block[] outputBlocks = new Block[1];
        if (nullRequired) {
            outputBlocks[0] = unnestBlockBuilder.buildOutputBlockWithNulls(maxLengths, startPosition, batchSize, currentBatchTotalLength, this.lengths);
        }
        else {
            outputBlocks[0] = unnestBlockBuilder.buildOutputBlockWithoutNulls(currentBatchTotalLength);
        }

        return outputBlocks;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // The lengths array in unnestBlockBuilders is the same object as in the unnester and doesn't need to be counted again.
        return INSTANCE_SIZE + sizeOf(lengths);
    }
}
