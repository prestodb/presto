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

import java.util.List;

public class SimpleJoinProbe
        implements JoinProbe
{
    public static class SimpleJoinProbeFactory
            implements JoinProbeFactory
    {
        private List<Integer> probeJoinChannels;

        public SimpleJoinProbeFactory(List<Integer> probeJoinChannels)
        {
            this.probeJoinChannels = probeJoinChannels;
        }

        @Override
        public JoinProbe createJoinProbe(LookupSource lookupSource, Page page)
        {
            return new SimpleJoinProbe(lookupSource, page, probeJoinChannels);
        }
    }

    private final LookupSource lookupSource;
    private final int positionCount;
    private final Block[] blocks;
    private final Block[] probeBlocks;
    private int position = -1;

    private SimpleJoinProbe(LookupSource lookupSource, Page page, List<Integer> probeJoinChannels)
    {
        this.lookupSource = lookupSource;
        this.positionCount = page.getPositionCount();
        this.blocks = new Block[page.getChannelCount()];
        this.probeBlocks = new Block[probeJoinChannels.size()];

        for (int i = 0; i < page.getChannelCount(); i++) {
            blocks[i] = page.getBlock(i);
        }

        for (int i = 0; i < probeJoinChannels.size(); i++) {
            probeBlocks[i] = blocks[probeJoinChannels.get(i)];
        }
    }

    @Override
    public int getChannelCount()
    {
        return blocks.length;
    }

    @Override
    public boolean advanceNextPosition()
    {
        position++;
        return position < positionCount;
    }

    @Override
    public void appendTo(PageBuilder pageBuilder)
    {
        for (int outputIndex = 0; outputIndex < blocks.length; outputIndex++) {
            Block block = blocks[outputIndex];
            block.appendTo(position, pageBuilder.getBlockBuilder(outputIndex));
        }
    }

    @Override
    public long getCurrentJoinPosition()
    {
        if (currentRowContainsNull()) {
            return -1;
        }
        return lookupSource.getJoinPosition(position, probeBlocks);
    }

    private boolean currentRowContainsNull()
    {
        for (Block probeBlock : probeBlocks) {
            if (probeBlock.isNull(position)) {
                return true;
            }
        }
        return false;
    }
}
