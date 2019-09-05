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
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Generic Unnester implementation for nested columns.
 *
 * This is a layer of abstraction between {@link UnnestOperator} and {@link UnnestBlockBuilder} to enable
 * translation of indices from input nested blocks to underlying element blocks.
 */
abstract class Unnester
{
    private final UnnestBlockBuilder[] unnestBlockBuilders;
    private int currentPosition;

    protected Unnester(Type... types)
    {
        requireNonNull(types, "types is null");
        this.unnestBlockBuilders = new UnnestBlockBuilder[types.length];
        this.currentPosition = 0;
        for (int i = 0; i < types.length; i++) {
            requireNonNull(types[i], "type is null");
            unnestBlockBuilders[i] = new UnnestBlockBuilder(types[i]);
        }
    }

    /**
     * Update {@code unnestBlockBuilders} with a new input.
     */
    public final void resetInput(Block block)
    {
        requireNonNull(block, "block is null");
        resetColumnarStructure(block);
        for (int i = 0; i < getChannelCount(); i++) {
            unnestBlockBuilders[i].resetInputBlock(getElementsBlock(i));
        }
        currentPosition = 0;
    }

    public final int getCurrentUnnestedLength()
    {
        return getElementsLength(currentPosition);
    }

    /**
     * Prepare for a new output page
     */
    public final void startNewOutput(PageBuilderStatus status, int expectedEntries)
    {
        for (int i = 0; i < getChannelCount(); i++) {
            unnestBlockBuilders[i].startNewOutput(status, expectedEntries);
        }
    }

    public final void processCurrentAndAdvance(int requiredOutputCount)
    {
        checkState(currentPosition >= 0 && currentPosition < getInputEntryCount(), "position out of bounds");
        processCurrentPosition(requiredOutputCount);
        currentPosition++;
    }

    /**
     * Build output and flush output state for the @{code unnestBlockBuilders}.
     */
    public final Block[] buildOutputBlocksAndFlush()
    {
        Block[] outputBlocks = new Block[getChannelCount()];
        for (int i = 0; i < getChannelCount(); i++) {
            outputBlocks[i] = unnestBlockBuilders[i].buildOutputAndFlush();
        }
        return outputBlocks;
    }

    protected final int getCurrentPosition()
    {
        return currentPosition;
    }

    protected final UnnestBlockBuilder getBlockBuilder(int index)
    {
        return unnestBlockBuilders[index];
    }

    public abstract int getChannelCount();

    abstract int getInputEntryCount();

    /**
     * Process current position for all {@code unnestBlockBuilders}. This method has to
     * (1) translate {@code currentPosition} to indexes for the block builders, using columnar structures.
     * (2) invoke {@link UnnestBlockBuilder} methods to produce {@code requiredOutputCount} number of rows
     * for every output block.
     */
    protected abstract void processCurrentPosition(int requiredOutputCount);

    protected abstract void resetColumnarStructure(Block block);

    /**
     * Get underlying elements block corresponding to {@code channel}
     */
    protected abstract Block getElementsBlock(int channel);

    /**
     * Get the length of the nested element at position {@code index}
     */
    protected abstract int getElementsLength(int index);
}
