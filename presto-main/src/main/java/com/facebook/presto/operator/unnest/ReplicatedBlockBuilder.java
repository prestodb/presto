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
import com.facebook.presto.spi.block.DictionaryBlock;

import java.util.Arrays;

import static com.facebook.presto.operator.unnest.UnnestOperatorBlockUtil.calculateNewArraySize;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * This class manages the details for building replicate channel blocks without copying input data
 */
class ReplicatedBlockBuilder
{
    private Block source;
    private int[] ids;
    private int positionCount;

    public void resetInputBlock(Block block)
    {
        this.source = requireNonNull(block, "block is null");
    }

    public void startNewOutput(int expectedEntries)
    {
        checkState(source != null, "source is null");
        this.ids = new int[expectedEntries];
        this.positionCount = 0;
    }

    /**
     * Repeat the source element at position {@code index} for {@code count} times in the output
     */
    public void appendRepeated(int index, int count)
    {
        checkState(source != null, "source is null");
        checkElementIndex(index, source.getPositionCount());
        checkArgument(count >= 0, "count should be >= 0");

        if (positionCount + count > ids.length) {
            // Grow capacity
            int newSize = Math.max(calculateNewArraySize(ids.length), positionCount + count);
            ids = Arrays.copyOf(ids, newSize);
        }

        Arrays.fill(ids, positionCount, positionCount + count, index);
        positionCount += count;
    }

    public Block buildOutputAndFlush()
    {
        Block outputBlock = new DictionaryBlock(positionCount, source, ids);

        // Flush stored state, so that ids can not be modified after the dictionary has been constructed
        ids = new int[0];
        positionCount = 0;

        return outputBlock;
    }
}
