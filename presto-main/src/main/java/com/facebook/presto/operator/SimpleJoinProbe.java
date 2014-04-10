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

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

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
    private final BlockCursor[] cursors;
    private final BlockCursor[] probeCursors;

    private SimpleJoinProbe(LookupSource lookupSource, Page page, List<Integer> probeJoinChannels)
    {
        this.lookupSource = lookupSource;
        this.cursors = new BlockCursor[page.getChannelCount()];
        this.probeCursors = new BlockCursor[probeJoinChannels.size()];

        for (int i = 0; i < page.getChannelCount(); i++) {
            cursors[i] = page.getBlock(i).cursor();
        }

        for (int i = 0; i < probeJoinChannels.size(); i++) {
            probeCursors[i] = cursors[probeJoinChannels.get(i)];
        }
    }

    @Override
    public int getChannelCount()
    {
        return cursors.length;
    }

    @Override
    public boolean advanceNextPosition()
    {
        // advance all cursors
        boolean advanced = cursors[0].advanceNextPosition();
        for (int i = 1; i < cursors.length; i++) {
            checkState(advanced == cursors[i].advanceNextPosition());
        }
        return advanced;
    }

    @Override
    public void appendTo(PageBuilder pageBuilder)
    {
        for (int outputIndex = 0; outputIndex < cursors.length; outputIndex++) {
            BlockCursor cursor = cursors[outputIndex];
            cursor.appendTo(pageBuilder.getBlockBuilder(outputIndex));
        }
    }

    @Override
    public long getCurrentJoinPosition()
    {
        if (currentRowContainsNull()) {
            return -1;
        }
        return lookupSource.getJoinPosition(probeCursors);
    }

    private boolean currentRowContainsNull()
    {
        for (BlockCursor probeCursor : probeCursors) {
            if (probeCursor.isNull()) {
                return true;
            }
        }
        return false;
    }
}
