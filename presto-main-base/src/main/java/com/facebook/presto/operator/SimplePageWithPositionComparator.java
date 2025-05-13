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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.google.common.primitives.Ints;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SimplePageWithPositionComparator
        implements PageWithPositionComparator
{
    private final int[] sortChannels;
    private final SortOrder[] sortOrders;
    private final Type[] types;

    public SimplePageWithPositionComparator(List<Type> types, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        requireNonNull(types, "types is null");
        this.sortChannels = Ints.toArray(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrders = requireNonNull(sortOrders, "sortOrders is null").toArray(new SortOrder[0]);
        checkArgument(this.sortOrders.length == this.sortChannels.length, "sortChannels and sortOrders length mismatch");
        this.types = new Type[this.sortChannels.length];
        for (int i = 0; i < this.sortChannels.length; i++) {
            int channel = this.sortChannels[i];
            this.types[i] = types.get(channel);
            checkArgument(this.types[i] != null, "type %s in types is null", channel);
        }
    }

    @Override
    public int compareTo(Page left, int leftPosition, Page right, int rightPosition)
    {
        for (int i = 0; i < sortChannels.length; i++) {
            int sortChannel = sortChannels[i];
            SortOrder sortOrder = sortOrders[i];
            Type type = types[i];
            Block leftBlock = left.getBlock(sortChannel);
            Block rightBlock = right.getBlock(sortChannel);

            try {
                int compare = sortOrder.compareBlockValue(type, leftBlock, leftPosition, rightBlock, rightPosition);
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
