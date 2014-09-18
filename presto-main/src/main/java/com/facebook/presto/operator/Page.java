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
import com.google.common.base.Objects;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Page
{
    private final Block[] blocks;
    private final int positionCount;
    private final long sizeInBytes;

    public Page(Block... blocks)
    {
        this(determinePositionCount(blocks), blocks);
    }

    public Page(int positionCount, Block... blocks)
    {
        checkNotNull(blocks, "blocks is null");
        this.blocks = Arrays.copyOf(blocks, blocks.length);
        this.positionCount = positionCount;

        long sizeInBytes = 0;
        for (Block block : blocks) {
            sizeInBytes += block.getSizeInBytes();
        }
        this.sizeInBytes = sizeInBytes;
    }

    public int getChannelCount()
    {
        return blocks.length;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    public Block[] getBlocks()
    {
        return blocks.clone();
    }

    public Block getBlock(int channel)
    {
        return blocks[channel];
    }

    public Block[] getSingleValueBlocks(int position)
    {
        Block[] row = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            row[i] = blocks[i].getSingleValueBlock(position);
        }
        return row;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("channelCount", getChannelCount())
                .addValue("@" + Integer.toHexString(System.identityHashCode(this)))
                .toString();
    }

    private static int determinePositionCount(Block... blocks)
    {
        checkNotNull(blocks, "blocks is null");
        checkArgument(blocks.length != 0, "blocks is empty");

        return blocks[0].getPositionCount();
    }
}
