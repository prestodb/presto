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

import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class SimplePagesHashStrategy
        implements PagesHashStrategy
{
    private final List<List<RandomAccessBlock>> channels;
    private final List<List<RandomAccessBlock>> hashChannels;

    public SimplePagesHashStrategy(List<List<RandomAccessBlock>> channels, List<Integer> hashChannels)
    {
        this.channels = ImmutableList.copyOf(channels);

        ImmutableList.Builder<List<RandomAccessBlock>> hashChannelsBuilder = ImmutableList.builder();
        for (int hashChannel : hashChannels) {
            hashChannelsBuilder.add(channels.get(hashChannel));
        }
        this.hashChannels = hashChannelsBuilder.build();
    }

    @Override
    public int getChannelCount()
    {
        return channels.size();
    }

    @Override
    public void appendTo(int blockIndex, int blockPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (List<RandomAccessBlock> channel : channels) {
            RandomAccessBlock block = channel.get(blockIndex);
            block.appendTo(blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    @Override
    public int hashPosition(int blockIndex, int blockPosition)
    {
        int result = 0;
        for (List<RandomAccessBlock> channel : hashChannels) {
            RandomAccessBlock block = channel.get(blockIndex);
            result = result * 31 + block.hashCode(blockPosition);
        }
        return result;
    }

    @Override
    public boolean positionEqualsCursors(int blockIndex, int blockPosition, BlockCursor[] cursors)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            List<RandomAccessBlock> channel = hashChannels.get(i);
            RandomAccessBlock block = channel.get(blockIndex);
            if (!block.equals(blockPosition, cursors[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        for (List<RandomAccessBlock> channel : hashChannels) {
            RandomAccessBlock leftBlock = channel.get(leftBlockIndex);
            RandomAccessBlock rightBlock = channel.get(rightBlockIndex);
            if (!leftBlock.equals(leftBlockPosition, rightBlock, rightBlockPosition)) {
                return false;
            }
        }

        return true;
    }
}
