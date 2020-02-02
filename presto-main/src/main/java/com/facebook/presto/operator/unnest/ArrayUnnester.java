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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.block.ColumnarArray.toColumnarArray;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Unnester for a nested column with array type, only when array elements are NOT of type {@code RowType}.
 * Maintains a {@link ColumnarArray} object to get underlying elements block from the array block.
 *
 * All protected methods implemented here assume that they are being invoked when {@code columnarArray} is non-null.
 */
class ArrayUnnester
        implements Unnester
{
    private final UnnestBlockBuilder unnestBlockBuilder;

    private ColumnarArray columnarArray;
    private int currentPosition;

    public ArrayUnnester(Type elementType)
    {
        unnestBlockBuilder = new UnnestBlockBuilder(elementType);
        currentPosition = 0;
    }

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
        currentPosition = 0;
        unnestBlockBuilder.resetInputBlock(columnarArray.getElementsBlock());
    }

    @Override
    public void startNewOutput(PageBuilderStatus status, int expectedEntries)
    {
        unnestBlockBuilder.startNewOutput(status, expectedEntries);
    }

    @Override
    public int getCurrentUnnestedLength()
    {
        return columnarArray.getLength(currentPosition);
    }

    @Override
    public void processCurrentAndAdvance(int requiredOutputCount)
    {
        checkState(currentPosition >= 0 && columnarArray != null && currentPosition < columnarArray.getPositionCount(), "position out of bounds");

        // Translate indices
        int startElementIndex = columnarArray.getOffset(currentPosition);
        int length = columnarArray.getLength(currentPosition);

        // Append elements and nulls
        unnestBlockBuilder.appendRange(startElementIndex, length);
        for (int i = 0; i < requiredOutputCount - length; i++) {
            unnestBlockBuilder.appendNull();
        }

        currentPosition++;
    }

    @Override
    public Block[] buildOutputBlocksAndFlush()
    {
        Block[] outputBlocks = new Block[1];
        outputBlocks[0] = unnestBlockBuilder.buildOutputAndFlush();
        return outputBlocks;
    }
}
