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
package com.facebook.presto.spi.function;

import com.facebook.presto.spi.block.BlockBuilder;

public abstract class RankingWindowFunction
        implements WindowFunction
{
    protected WindowIndex windowIndex;

    private int currentPeerGroupStart;
    private int currentPosition;

    @Override
    public final void reset(WindowIndex windowIndex)
    {
        this.windowIndex = windowIndex;
        this.currentPeerGroupStart = -1;
        this.currentPosition = 0;

        reset();
    }

    @Override
    public final void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd)
    {
        boolean newPeerGroup = false;
        if (peerGroupStart != currentPeerGroupStart) {
            currentPeerGroupStart = peerGroupStart;
            newPeerGroup = true;
        }

        int peerGroupCount = (peerGroupEnd - peerGroupStart) + 1;

        processRow(output, newPeerGroup, peerGroupCount, currentPosition);

        currentPosition++;
    }

    /**
     * Reset state for a new partition (including the first one).
     */
    public void reset()
    {
        // subclasses can override
    }

    /**
     * Process a row by outputting the result of the window function.
     * <p/>
     * This method provides information about the ordering peer group. A peer group is all
     * of the rows that are peers within the specified ordering. Rows are peers if they
     * compare equal to each other using the specified ordering expression. The ordering
     * of rows within a peer group is undefined (otherwise they would not be peers).
     *
     * @param output the {@link BlockBuilder} to use for writing the output row
     * @param newPeerGroup if this row starts a new peer group
     * @param peerGroupCount the total number of rows in this peer group
     * @param currentPosition the current position for this row
     */
    public abstract void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount, int currentPosition);
}
