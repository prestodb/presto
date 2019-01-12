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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;

import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RowComparator
        implements Comparator<Page>
{
    private final List<Type> sortTypes;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;

    public RowComparator(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        this.sortTypes = ImmutableList.copyOf(requireNonNull(sortTypes, "sortTypes is null"));
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        checkArgument(sortTypes.size() == sortChannels.size(), "sortTypes size (%s) doesn't match sortChannels size (%s)", sortTypes.size(), sortChannels.size());
        checkArgument(sortChannels.size() == sortOrders.size(), "sortFields size (%s) doesn't match sortOrders size (%s)", sortChannels.size(), sortOrders.size());
    }

    @Override
    public int compare(Page leftRow, Page rightRow)
    {
        for (int index = 0; index < sortChannels.size(); index++) {
            Type type = sortTypes.get(index);
            int channel = sortChannels.get(index);
            SortOrder sortOrder = sortOrders.get(index);

            Block left = leftRow.getBlock(channel);
            Block right = rightRow.getBlock(channel);

            int comparison = sortOrder.compareBlockValue(type, left, 0, right, 0);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }
}
