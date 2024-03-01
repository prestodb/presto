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
import com.facebook.presto.common.block.DictionaryBlock;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * This class manages the details for building replicate channel blocks without copying input data
 */
class ReplicatedBlockBuilder
{
    private Block source;
    private int sourcePosition;

    public void resetInputBlock(Block block)
    {
        source = requireNonNull(block, "block is null");
        sourcePosition = 0;
    }

    public Block buildOutputBlock(int[] maxEntries, int offset, int length, int totalEntries)
    {
        int[] ids = new int[totalEntries];

        int fromPosition = 0;
        for (int i = 0; i < length; i++) {
            int toPosition = fromPosition + maxEntries[offset + i];
            Arrays.fill(ids, fromPosition, toPosition, sourcePosition++);
            fromPosition = toPosition;
        }

        return new DictionaryBlock(totalEntries, source, ids);
    }
}
