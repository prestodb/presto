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
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.block.ColumnarMap.toColumnarMap;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Unnester for a nested column with map type.
 * Maintains a {@link ColumnarMap} object to get underlying keys and values block from the map block.
 *
 * All protected methods implemented here assume that they are being invoked when {@code columnarMap} is non-null.
 */
class MapUnnester
        implements Unnester
{
    private final UnnestBlockBuilder keyUnnestBlockBuilder;
    private final UnnestBlockBuilder valueUnnestBlockBuilder;

    private ColumnarMap columnarMap;
    private int currentPosition;

    public MapUnnester(Type keyType, Type valueType)
    {
        keyUnnestBlockBuilder = new UnnestBlockBuilder(keyType);
        valueUnnestBlockBuilder = new UnnestBlockBuilder(valueType);
        currentPosition = 0;
    }

    @Override
    public int getChannelCount()
    {
        return 2;
    }

    @Override
    public void resetInput(Block block)
    {
        requireNonNull(block, "block is null");

        columnarMap = toColumnarMap(block);
        currentPosition = 0;
        keyUnnestBlockBuilder.resetInputBlock(columnarMap.getKeysBlock());
        valueUnnestBlockBuilder.resetInputBlock(columnarMap.getValuesBlock());
    }

    @Override
    public void startNewOutput(PageBuilderStatus status, int expectedEntries)
    {
        keyUnnestBlockBuilder.startNewOutput(status, expectedEntries);
        valueUnnestBlockBuilder.startNewOutput(status, expectedEntries);
    }

    @Override
    public int getCurrentUnnestedLength()
    {
        return columnarMap.getEntryCount(currentPosition);
    }

    @Override
    public void processCurrentAndAdvance(int requiredOutputCount)
    {
        checkState(currentPosition >= 0 && columnarMap != null && currentPosition < columnarMap.getPositionCount(), "position out of bounds");

        // Translate indices
        int mapLength = columnarMap.getEntryCount(currentPosition);
        int startingOffset = columnarMap.getOffset(currentPosition);

        // Append elements and nulls for keys Block
        keyUnnestBlockBuilder.appendRange(startingOffset, mapLength);
        for (int i = 0; i < requiredOutputCount - mapLength; i++) {
            keyUnnestBlockBuilder.appendNull();
        }

        // Append elements and nulls for values Block
        valueUnnestBlockBuilder.appendRange(startingOffset, mapLength);
        for (int i = 0; i < requiredOutputCount - mapLength; i++) {
            valueUnnestBlockBuilder.appendNull();
        }

        currentPosition++;
    }

    @Override
    public Block[] buildOutputBlocksAndFlush()
    {
        Block[] outputBlocks = new Block[2];
        outputBlocks[0] = keyUnnestBlockBuilder.buildOutputAndFlush();
        outputBlocks[1] = valueUnnestBlockBuilder.buildOutputAndFlush();
        return outputBlocks;
    }
}
