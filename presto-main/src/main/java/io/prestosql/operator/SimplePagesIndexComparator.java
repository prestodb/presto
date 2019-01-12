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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;

import java.util.List;

import static io.prestosql.operator.SyntheticAddress.decodePosition;
import static io.prestosql.operator.SyntheticAddress.decodeSliceIndex;
import static java.util.Objects.requireNonNull;

public class SimplePagesIndexComparator
        implements PagesIndexComparator
{
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final List<Type> sortTypes;

    public SimplePagesIndexComparator(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        this.sortTypes = ImmutableList.copyOf(requireNonNull(sortTypes, "sortTypes is null"));
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
    }

    @Override
    public int compareTo(PagesIndex pagesIndex, int leftPosition, int rightPosition)
    {
        long leftPageAddress = pagesIndex.getValueAddresses().getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = pagesIndex.getValueAddresses().getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        for (int i = 0; i < sortChannels.size(); i++) {
            int sortChannel = sortChannels.get(i);
            Block leftBlock = pagesIndex.getChannel(sortChannel).get(leftBlockIndex);
            Block rightBlock = pagesIndex.getChannel(sortChannel).get(rightBlockIndex);

            SortOrder sortOrder = sortOrders.get(i);
            int compare = sortOrder.compareBlockValue(sortTypes.get(i), leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }
}
