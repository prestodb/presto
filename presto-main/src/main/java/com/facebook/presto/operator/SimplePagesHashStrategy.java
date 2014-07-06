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
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SimplePagesHashStrategy
        implements PagesHashStrategy
{
    private final List<Type> types;
    private final List<List<Block>> channels;
    private final List<Integer> hashChannels;

    public SimplePagesHashStrategy(List<Type> types, List<List<Block>> channels, List<Integer> hashChannels)
    {
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.channels = ImmutableList.copyOf(checkNotNull(channels, "channels is null"));
        checkArgument(types.size() == channels.size(), "Expected types and channels to be the same length");
        this.hashChannels = ImmutableList.copyOf(checkNotNull(hashChannels, "hashChannels is null"));
    }

    @Override
    public int getChannelCount()
    {
        return channels.size();
    }

    @Override
    public void appendTo(int blockIndex, int blockPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        for (int i = 0; i < channels.size(); i++) {
            Type type = types.get(i);
            List<Block> channel = channels.get(i);
            Block block = channel.get(blockIndex);
            type.appendTo(block, blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    @Override
    public int hashPosition(int blockIndex, int blockPosition)
    {
        int result = 0;
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            Block block = channels.get(hashChannel).get(blockIndex);
            result = result * 31 + type.hash(block, blockPosition);
        }
        return result;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftBlockPosition, int rightPosition, Block... rightBlocks)
    {
        for (int i = 0; i < hashChannels.size(); i++) {
            int hashChannel = hashChannels.get(i);
            Type type = types.get(hashChannel);
            Block leftBlock = channels.get(hashChannel).get(leftBlockIndex);
            if (!type.equalTo(leftBlock, leftBlockPosition, rightBlocks[i], rightPosition)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        for (int hashChannel : hashChannels) {
            Type type = types.get(hashChannel);
            List<Block> channel = channels.get(hashChannel);
            Block leftBlock = channel.get(leftBlockIndex);
            Block rightBlock = channel.get(rightBlockIndex);
            if (!type.equalTo(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition)) {
                return false;
            }
        }

        return true;
    }
}
