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

package com.facebook.presto.operator;

import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class StandardJoinFilterFunctionVerifier
        implements JoinFilterFunctionVerifier
{
    private static final Block[] EMPTY_BLOCK_ARRAY = new Block[0];

    private final JoinFilterFunction filterFunction;
    private final List<Block[]> pages;

    public StandardJoinFilterFunctionVerifier(JoinFilterFunction filterFunction, List<List<Block>> channels)
    {
        this.filterFunction = requireNonNull(filterFunction, "filterFunction can not be null");

        requireNonNull(channels, "channels can not be null");
        ImmutableList.Builder<Block[]> pagesBuilder = ImmutableList.builder();
        if (!channels.isEmpty()) {
            int pagesCount = channels.get(0).size();
            for (int pageIndex = 0; pageIndex < pagesCount; ++pageIndex) {
                Block[] blocks = new Block[channels.size()];
                for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
                    blocks[channelIndex] = channels.get(channelIndex).get(pageIndex);
                }
                pagesBuilder.add(blocks);
            }
        }
        this.pages = pagesBuilder.build();
    }

    @Override
    public boolean applyFilterFunction(int leftBlockIndex, int leftPosition, int rightPosition, Block[] allRightBlocks)
    {
        return filterFunction.filter(leftPosition, getLeftBlocks(leftBlockIndex), rightPosition, allRightBlocks);
    }

    private Block[] getLeftBlocks(int leftBlockIndex)
    {
        if (pages.isEmpty()) {
            return EMPTY_BLOCK_ARRAY;
        }
        else {
            return pages.get(leftBlockIndex);
        }
    }
}
