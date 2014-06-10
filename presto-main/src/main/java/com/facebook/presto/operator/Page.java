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
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.Arrays;

public class Page
{
    private final Block[] blocks;
    private final int positionCount;

    public Page(Block... blocks)
    {
        this(blocks[0].getPositionCount(), blocks);
    }

    public Page(int positionCount, Block... blocks)
    {
        Preconditions.checkNotNull(blocks, "blocks is null");
        this.blocks = Arrays.copyOf(blocks, blocks.length);
        this.positionCount = positionCount;
    }

    public int getChannelCount()
    {
        return blocks.length;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public DataSize getDataSize()
    {
        long dataSize = 0;
        for (Block block : blocks) {
            dataSize += block.getSizeInBytes();
        }
        return new DataSize(dataSize, Unit.BYTE);
    }

    public Block[] getBlocks()
    {
        return blocks.clone();
    }

    public Block getBlock(int channel)
    {
        return blocks[channel];
    }

    public boolean getBoolean(int channel, int position)
    {
        return getBlock(channel).getBoolean(position);
    }

    public long getLong(int channel, int position)
    {
        return getBlock(channel).getLong(position);
    }

    public double getDouble(int channel, int position)
    {
        return getBlock(channel).getDouble(position);
    }

    public Slice getSlice(int channel, int position)
    {
        return getBlock(channel).getSlice(position);
    }

    public boolean isNull(int channel, int position)
    {
        return getBlock(channel).isNull(position);
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

    public Function<Integer, Block> blockGetter()
    {
        return new Function<Integer, Block>()
        {
            @Override
            public Block apply(Integer input)
            {
                return getBlock(input);
            }
        };
    }
}
