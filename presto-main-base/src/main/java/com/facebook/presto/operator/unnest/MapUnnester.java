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
import com.facebook.presto.common.block.ColumnarMap;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * Unnester for a nested column with map type.
 * Maintains a {@link ColumnarMap} object to get underlying keys and values block from the map block.
 * <p>
 * All protected methods implemented here assume that they are being invoked when {@code columnarMap} is non-null.
 */
class MapUnnester
        implements Unnester
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapUnnester.class).instanceSize();

    private final UnnestBlockBuilder keyUnnestBlockBuilder;
    private final UnnestBlockBuilder valueUnnestBlockBuilder;

    private int[] lengths = new int[0];
    private ColumnarMap columnarMap;

    public MapUnnester()
    {
        keyUnnestBlockBuilder = new UnnestBlockBuilder();
        valueUnnestBlockBuilder = new UnnestBlockBuilder();
    }

    @Override
    public int getChannelCount()
    {
        return 2;
    }

    public void resetInput(Block block)
    {
        requireNonNull(block, "block is null");

        columnarMap = toColumnarMap(block);
        keyUnnestBlockBuilder.resetInputBlock(columnarMap.getKeysBlock());
        valueUnnestBlockBuilder.resetInputBlock(columnarMap.getValuesBlock());

        int positionCount = block.getPositionCount();
        lengths = ensureCapacity(lengths, positionCount);

        for (int i = 0; i < positionCount; i++) {
            lengths[i] = columnarMap.getEntryCount(i);
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

        Block[] outputBlocks = new Block[2];
        if (nullRequired) {
            outputBlocks[0] = keyUnnestBlockBuilder.buildOutputBlockWithNulls(maxLengths, startPosition, batchSize, currentBatchTotalLength, this.lengths);
            outputBlocks[1] = valueUnnestBlockBuilder.buildOutputBlockWithNulls(maxLengths, startPosition, batchSize, currentBatchTotalLength, this.lengths);
        }
        else {
            outputBlocks[0] = keyUnnestBlockBuilder.buildOutputBlockWithoutNulls(currentBatchTotalLength);
            outputBlocks[1] = valueUnnestBlockBuilder.buildOutputBlockWithoutNulls(currentBatchTotalLength);
        }

        return outputBlocks;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // The lengths array in unnestBlockBuilders is the same object as in the unnester and doesn't need to be counted again.
        return INSTANCE_SIZE + sizeOf(lengths);
    }

    private boolean needToInsertNulls(int offset, int length, int maxTotalEntriesForBatch)
    {
        int totalEntriesForBatch = columnarMap.getOffset(offset + length) - columnarMap.getOffset(offset);
        return totalEntriesForBatch < maxTotalEntriesForBatch;
    }
}
