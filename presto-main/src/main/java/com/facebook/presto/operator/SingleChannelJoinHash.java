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

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;

public class SingleChannelJoinHash
        extends JoinHash
{
    private final LongArrayList addresses;
    private final ObjectArrayList<RandomAccessBlock>[] channels;
    private final ObjectArrayList<RandomAccessBlock> hashChannel;

    public SingleChannelJoinHash(LongArrayList addresses,
            ObjectArrayList<RandomAccessBlock>[] channels,
            int hashChannel,
            OperatorContext operatorContext)
    {
        super(channels.length, addresses.size(), operatorContext);
        this.addresses = addresses;
        this.channels = channels;

        this.hashChannel = this.channels[hashChannel];

        buildHash(addresses.size());
    }

    public void appendTo(int position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        for (ObjectArrayList<RandomAccessBlock> channel : channels) {
            RandomAccessBlock block = channel.get(blockIndex);
            block.appendTo(blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    protected int hashCursor(BlockCursor... cursors)
    {
        return cursors[0].calculateHashCode();
    }

    protected int hashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        RandomAccessBlock block = hashChannel.get(blockIndex);
        return block.hashCode(blockPosition);
    }

    protected boolean positionEqualsCurrentRow(int position, BlockCursor... cursors)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        RandomAccessBlock block = hashChannel.get(blockIndex);
        return block.equals(blockPosition, cursors[0]);
    }

    protected boolean positionEqualsPosition(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);
        RandomAccessBlock leftBlock = hashChannel.get(leftBlockIndex);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);
        RandomAccessBlock rightBlock = hashChannel.get(rightBlockIndex);

        return leftBlock.equals(leftBlockPosition, rightBlock, rightBlockPosition);
    }
}
