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

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.array.AdaptiveLongBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.google.common.primitives.Ints;

import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class SimplePagesIndexComparator
        implements PagesIndexComparator
{
    private final int[] sortChannels;
    private final SortOrder[] sortOrders;
    private final Type[] sortTypes;

    public SimplePagesIndexComparator(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        this.sortChannels = Ints.toArray(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrders = requireNonNull(sortOrders, "sortOrders is null").toArray(new SortOrder[0]);
        this.sortTypes = requireNonNull(sortTypes, "sortTypes is null").toArray(new Type[0]);
    }

    @Override
    public int compareTo(PagesIndex pagesIndex, int leftPosition, int rightPosition)
    {
        AdaptiveLongBigArray valueAddresses = pagesIndex.getValueAddresses();
        long leftPageAddress = valueAddresses.get(leftPosition);
        long rightPageAddress = valueAddresses.get(rightPosition);

        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        for (int i = 0; i < sortChannels.length; i++) {
            int sortChannel = sortChannels[i];
            SortOrder sortOrder = sortOrders[i];
            Type sortType = sortTypes[i];
            List<Block> indexChannel = pagesIndex.getChannel(sortChannel);
            Block leftBlock = indexChannel.get(leftBlockIndex);
            Block rightBlock = indexChannel.get(rightBlockIndex);

            try {
                int compare = sortOrder.compareBlockValue(sortType, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                if (compare != 0) {
                    return compare;
                }
            }
            catch (NotSupportedException e) {
                throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
            }
        }
        return 0;
    }
}
