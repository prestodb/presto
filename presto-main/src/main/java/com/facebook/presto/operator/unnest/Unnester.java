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

/**
 * Unnester is a layer of abstraction between {@link UnnestOperator} and {@link UnnestBlockBuilder} to enable
 * translation of indices from input nested blocks to underlying element blocks.
 */
public interface Unnester
{
    int getChannelCount();

    void resetInput(Block block);

    /**
     * Return a vector with the number of entries for each position of the block to be unnested.
     */
    int[] getLengths();

    /**
     * Build the output blocks for the current batch for this unnester.
     *
     * @param maxLengths A vector that holds the max unnested row count for each position of all blocks to be unnested.
     * @param startPosition The start position of this batch.
     * @param batchSize The number of rows to be processed in this batch.
     * @param currentBatchTotalLength The total row count for this batch after the unnest is done.
     * @return
     */
    Block[] buildOutputBlocks(int[] maxLengths, int startPosition, int batchSize, int currentBatchTotalLength);

    long getRetainedSizeInBytes();
}
