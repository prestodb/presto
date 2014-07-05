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

import java.util.List;

// This class exists as template for code generation and for testing
public class TwoChannelPagesHashStrategy
        implements PagesHashStrategy
{
    private final Type typeA;
    private final Type typeB;
    private final List<Block> channelA;
    private final List<Block> channelB;
    private final List<Block> hashChannelA;
    private final List<Block> hashChannelB;

    public TwoChannelPagesHashStrategy(List<Type> types, List<List<Block>> channels)
    {
        this.typeA = types.get(0);
        this.typeB = types.get(0);
        this.channelA = channels.get(0);
        this.channelB = channels.get(1);
        this.hashChannelA = channels.get(2);
        this.hashChannelB = channels.get(3);
    }

    @Override
    public int getChannelCount()
    {
        return 2;
    }

    @Override
    public void appendTo(int blockIndex, int blockPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        typeA.appendTo(channelA.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + 0));
        typeB.appendTo(channelB.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + 1));
    }

    @Override
    public int hashPosition(int blockIndex, int blockPosition)
    {
        int result = 0;
        result = result * 31 + typeA.hash(hashChannelA.get(blockIndex), blockPosition);
        result = result * 31 + typeB.hash(hashChannelB.get(blockIndex), blockPosition);
        return result;
    }

    @Override
    public boolean positionEqualsRow(int leftBlockIndex, int leftBlockPosition, int rightPosition, Block[] rightBlocks)
    {
        if (!typeA.equalTo(hashChannelA.get(leftBlockIndex), leftBlockPosition, rightBlocks[0], rightPosition)) {
            return false;
        }
        if (!typeB.equalTo(hashChannelB.get(leftBlockIndex), leftBlockPosition, rightBlocks[1], rightPosition)) {
            return false;
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        if (!typeA.equalTo(hashChannelA.get(leftBlockIndex), leftBlockPosition, hashChannelA.get(rightBlockIndex), rightBlockPosition)) {
            return false;
        }
        if (!typeB.equalTo(hashChannelB.get(leftBlockIndex), leftBlockPosition, hashChannelB.get(rightBlockIndex), rightBlockPosition)) {
            return false;
        }

        return true;
    }
}
