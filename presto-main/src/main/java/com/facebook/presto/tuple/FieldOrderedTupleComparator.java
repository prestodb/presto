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
package com.facebook.presto.tuple;

import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.operator.SortOrder;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Compares two rows element by element.
 */
public class FieldOrderedTupleComparator
        implements Comparator<RandomAccessBlock[]>
{
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;

    public FieldOrderedTupleComparator(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        checkNotNull(sortChannels, "sortChannels is null");
        checkNotNull(sortOrders, "sortOrders is null");
        checkArgument(sortChannels.size() == sortOrders.size(), "sortFields size (%s) doesn't match sortOrders size (%s)", sortChannels.size(), sortOrders.size());

        this.sortChannels = ImmutableList.copyOf(sortChannels);
        this.sortOrders = ImmutableList.copyOf(sortOrders);
    }

    @Override
    public int compare(RandomAccessBlock[] leftRow, RandomAccessBlock[] rightRow)
    {
        for (int index = 0; index < sortChannels.size(); index++) {
            int channel = sortChannels.get(index);
            SortOrder sortOrder = sortOrders.get(index);

            RandomAccessBlock left = leftRow[channel];
            RandomAccessBlock right = rightRow[channel];

            int comparison = left.compareTo(sortOrder, 0, right, 0);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }
}
