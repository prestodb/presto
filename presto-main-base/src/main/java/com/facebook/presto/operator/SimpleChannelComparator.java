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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SimpleChannelComparator
        implements PagesIndexComparator
{
    private final int leftChannel;
    private final int rightChannel;
    private final SortOrder sortOrder;
    private final Type sortType;

    public SimpleChannelComparator(int leftChannel, int rightChannel, Type sortType, SortOrder sortOrder)
    {
        this.leftChannel = leftChannel;
        this.rightChannel = rightChannel;
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
        this.sortType = requireNonNull(sortType, "sortType is null.");
    }

    @Override
    public int compareTo(PagesIndex pagesIndex, int leftPosition, int rightPosition)
    {
        long leftPageAddress = pagesIndex.getValueAddresses().get(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = pagesIndex.getValueAddresses().get(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        try {
            Block leftBlock = pagesIndex.getChannel(leftChannel).get(leftBlockIndex);
            Block rightBlock = pagesIndex.getChannel(rightChannel).get(rightBlockIndex);
            int result = sortOrder.compareBlockValue(sortType, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);

            // sortOrder compares block values and adjusts the result by ASC and DESC. SimpleChannelComparator should
            // return the simple comparison, so reverse it if it is DESC.
            return sortOrder.isAscending() ? result : -result;
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, throwable);
        }
    }
}
