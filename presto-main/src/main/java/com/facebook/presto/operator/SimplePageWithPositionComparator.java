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
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SimplePageWithPositionComparator
        implements PageWithPositionComparator
{
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final List<Type> types;

    public SimplePageWithPositionComparator(List<Type> types, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
    }

    @Override
    public int compareTo(Page left, int leftPosition, Page right, int rightPosition)
    {
        for (int i = 0; i < sortChannels.size(); i++) {
            int sortChannel = sortChannels.get(i);
            Block leftBlock = left.getBlock(sortChannel);
            Block rightBlock = right.getBlock(sortChannel);

            SortOrder sortOrder = sortOrders.get(i);
            int compare = sortOrder.compareBlockValue(types.get(sortChannel), leftBlock, leftPosition, rightBlock, rightPosition);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }
}
