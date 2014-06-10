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

import java.util.List;

// This class exists as template for code generation and for testing
public class TwoChannelPagesHashStrategy
        implements PagesHashStrategy
{
    private final List<Block> channelA;
    private final List<Block> channelB;
    private final List<Block> hashChannelA;
    private final List<Block> hashChannelB;

    public TwoChannelPagesHashStrategy(List<List<Block>> channels)
    {
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
        channelA.get(blockIndex).appendTo(blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + 0));
        channelB.get(blockIndex).appendTo(blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + 1));
    }

    @Override
    public int hashPosition(int blockIndex, int blockPosition)
    {
        int result = 0;
        result = result * 31 + hashChannelA.get(blockIndex).hash(blockPosition);
        result = result * 31 + hashChannelB.get(blockIndex).hash(blockPosition);
        return result;
    }

    @Override
    public boolean positionEqualsCursors(int blockIndex, int blockPosition, BlockCursor[] cursors)
    {
        if (!hashChannelA.get(blockIndex).equalTo(blockPosition, cursors[0])) {
            return false;
        }
        if (!hashChannelB.get(blockIndex).equalTo(blockPosition, cursors[1])) {
            return false;
        }
        return true;
    }

    @Override
    public boolean positionEqualsPosition(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition)
    {
        if (!hashChannelA.get(leftBlockIndex).equalTo(leftBlockPosition, hashChannelA.get(rightBlockIndex), rightBlockPosition)) {
            return false;
        }
        if (!hashChannelB.get(leftBlockIndex).equalTo(leftBlockPosition, hashChannelB.get(rightBlockIndex), rightBlockPosition)) {
            return false;
        }

        return true;
    }
}
