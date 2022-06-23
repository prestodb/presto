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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.util.List;
import java.util.OptionalInt;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public class JoinProbe
{
    public static class JoinProbeFactory
    {
        private final int[] probeOutputChannels;
        private final int[] probeJoinChannels;
        private final int probeHashChannel; // only valid when >= 0

        public JoinProbeFactory(int[] probeOutputChannels, List<Integer> probeJoinChannels, OptionalInt probeHashChannel)
        {
            this.probeOutputChannels = probeOutputChannels;
            this.probeJoinChannels = Ints.toArray(probeJoinChannels);
            this.probeHashChannel = probeHashChannel.orElse(-1);
        }

        public JoinProbe createJoinProbe(Page page)
        {
            Page probePage = page.getLoadedPage(probeJoinChannels);
            return new JoinProbe(probeOutputChannels, page, probePage, probeHashChannel >= 0 ? page.getBlock(probeHashChannel).getLoadedBlock() : null);
        }
    }

    private final int[] probeOutputChannels;
    private final int positionCount;
    private final Page page;
    private final Page probePage;
    @Nullable
    private final Block probeHashBlock;
    private final boolean probeMayHaveNull;

    private int position = -1;

    private JoinProbe(int[] probeOutputChannels, Page page, Page probePage, @Nullable Block probeHashBlock)
    {
        this.probeOutputChannels = probeOutputChannels;
        this.positionCount = page.getPositionCount();
        this.page = page;
        this.probePage = probePage;
        this.probeHashBlock = probeHashBlock;
        this.probeMayHaveNull = probeMayHaveNull(probePage);
    }

    public int[] getOutputChannels()
    {
        return probeOutputChannels;
    }

    public boolean advanceNextPosition()
    {
        return ++position < positionCount;
    }

    public long getCurrentJoinPosition(LookupSource lookupSource)
    {
        if (probeMayHaveNull && currentRowContainsNull()) {
            return -1;
        }
        if (probeHashBlock != null) {
            long rawHash = BIGINT.getLong(probeHashBlock, position);
            return lookupSource.getJoinPosition(position, probePage, page, rawHash);
        }
        return lookupSource.getJoinPosition(position, probePage, page);
    }

    public int getPosition()
    {
        return position;
    }

    public Page getPage()
    {
        return page;
    }

    private boolean currentRowContainsNull()
    {
        for (int i = 0; i < probePage.getChannelCount(); i++) {
            if (probePage.getBlock(i).isNull(position)) {
                return true;
            }
        }
        return false;
    }

    private static boolean probeMayHaveNull(Page probePage)
    {
        for (int i = 0; i < probePage.getChannelCount(); i++) {
            if (probePage.getBlock(i).mayHaveNull()) {
                return true;
            }
        }
        return false;
    }
}
