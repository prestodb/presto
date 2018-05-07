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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static java.util.Objects.requireNonNull;

public class StandardJoinFilterFunction
        implements JoinFilterFunction
{
    private static final Page EMPTY_PAGE = new Page(0);

    private final InternalJoinFilterFunction filterFunction;
    private final LongArrayList addresses;
    private final List<Page> pages;

    public StandardJoinFilterFunction(InternalJoinFilterFunction filterFunction, LongArrayList addresses, List<List<Block>> channels)
    {
        this.filterFunction = requireNonNull(filterFunction, "filterFunction can not be null");
        this.addresses = requireNonNull(addresses, "addresses is null");

        requireNonNull(channels, "channels can not be null");
        ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builder();
        if (!channels.isEmpty()) {
            int pagesCount = channels.get(0).size();
            for (int pageIndex = 0; pageIndex < pagesCount; ++pageIndex) {
                Block[] blocks = new Block[channels.size()];
                for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
                    blocks[channelIndex] = channels.get(channelIndex).get(pageIndex);
                }
                pagesBuilder.add(new Page(blocks));
            }
        }
        this.pages = pagesBuilder.build();
    }

    @Override
    public boolean filter(int leftPosition, int rightPosition, Page rightPage)
    {
        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return filterFunction.filter(blockPosition, pages.isEmpty() ? EMPTY_PAGE : pages.get(blockIndex), rightPosition, rightPage);
    }
}
