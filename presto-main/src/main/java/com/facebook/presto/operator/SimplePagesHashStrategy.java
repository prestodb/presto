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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockCursor;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class SimplePagesHashStrategy
        implements PagesHashStrategy
{
    private final List<List<Block>> channels;
    private final List<List<Block>> hashChannels;

    public SimplePagesHashStrategy(List<List<Block>> channels, List<Integer> hashChannels)
    {
        this.channels = ImmutableList.copyOf(channels);

        ImmutableList.Builder<List<Block>> hashChannelsBuilder = ImmutableList.builder();
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
        for (List<Block> channel : channels) {
            Block block = channel.get(blockIndex);
            block.appendTo(blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    @Override
    public int hashPosition(int blockIndex, int blockPosition)
    {
        int result = 0;
        for (List<Block> channel : hashChannels) {
            Block block = channel.get(blockIndex);
            result = result * 31 + block.hash(blockPosition);
        }
        return result;
    }

    @Override
    public boolean positionEqualsCursors(int blockIndex, int blockPosition, BlockCursor[] cursors)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            List<Block> channel = hashChannels.get(i);
            Block block = channel.get(blockIndex);
            if (!block.equalTo(blockPosition, cursors[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        for (List<Block> channel : hashChannels) {
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!leftBlock.equalTo(leftBlockPosition, rightBlock, rightBlockPosition)) {
                return false;
            }
        }

        return true;
    }
}
