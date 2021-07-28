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

import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.type.BlockTypeOperators;

import static com.facebook.presto.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SimpleChannelComparator
        implements PagesIndexComparator
{
    private final int leftChannel;
    private final int rightChannel;
    private final BlockTypeOperators.BlockPositionComparison comparator;

    public SimpleChannelComparator(int leftChannel, int rightChannel, BlockTypeOperators.BlockPositionComparison comparator)
    {
        this.leftChannel = leftChannel;
        this.rightChannel = rightChannel;
        this.comparator = requireNonNull(comparator, "comparator is null");
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
            return (int) comparator.compare(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, throwable);
        }
    }
}
