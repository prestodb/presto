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

public class MultiChannelJoinHash
        extends JoinHash
{
    private final LongArrayList addresses;
    private final ObjectArrayList<RandomAccessBlock>[] channels;
    private final ObjectArrayList<RandomAccessBlock>[] hashChannels;

    public MultiChannelJoinHash(LongArrayList addresses,
            ObjectArrayList<RandomAccessBlock>[] channels,
            int[] hashChannels,
            OperatorContext operatorContext)
    {
        super(channels.length, addresses.size(), operatorContext);
        this.addresses = addresses;
        this.channels = channels;

        this.hashChannels = (ObjectArrayList<RandomAccessBlock>[]) new ObjectArrayList[hashChannels.length];
        for (int i = 0; i < hashChannels.length; i++) {
            this.hashChannels[i] = this.channels[hashChannels[i]];
        }

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
        int result = 0;
        for (BlockCursor cursor : cursors) {
            result = result * 31 + cursor.calculateHashCode();
        }
        return result;
    }

    protected int hashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        int result = 0;
        for (ObjectArrayList<RandomAccessBlock> channel : hashChannels) {
            RandomAccessBlock block = channel.get(blockIndex);
            result = result * 31 + block.hashCode(blockPosition);
        }
        return result;
    }

    protected boolean positionEqualsCurrentRow(int position, BlockCursor... cursors)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        for (int i = 0; i < hashChannels.length; i++) {
            ObjectArrayList<RandomAccessBlock> channel = hashChannels[i];
            RandomAccessBlock block = channel.get(blockIndex);
            if (!block.equals(blockPosition, cursors[i])) {
                return false;
            }
        }
        return true;
    }

    protected boolean positionEqualsPosition(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        for (ObjectArrayList<RandomAccessBlock> channel : hashChannels) {
            RandomAccessBlock leftBlock = channel.get(leftBlockIndex);
            RandomAccessBlock rightBlock = channel.get(rightBlockIndex);
            if (!leftBlock.equals(leftBlockPosition, rightBlock, rightBlockPosition)) {
                return false;
            }
        }

        return true;
    }
}
