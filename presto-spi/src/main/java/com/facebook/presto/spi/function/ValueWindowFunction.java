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

public abstract class ValueWindowFunction
        implements WindowFunction
{
    protected WindowIndex windowIndex;

    private int currentPosition;

    @Override
    public final void reset(WindowIndex windowIndex)
    {
        this.windowIndex = windowIndex;
        this.currentPosition = 0;

        reset();
    }

    @Override
    public final void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd)
    {
        processRow(output, frameStart, frameEnd, currentPosition);

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
     *
     * @param output the {@link BlockBuilder} to use for writing the output row
     * @param frameStart the position of the first row in the window frame
     * @param frameEnd the position of the last row in the window frame
     * @param currentPosition the current position for this row
     */
    public abstract void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition);
}
