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
package com.facebook.presto.operator.window;

import com.facebook.presto.operator.PagesHashStrategy;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.sql.tree.FrameBound;
import com.google.common.primitives.Ints;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.tree.FrameBound.Type.FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.PRECEDING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.tree.WindowFrame.Type.RANGE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;

public final class WindowPartition
{
    private final PagesIndex pagesIndex;
    private final int partitionStart;
    private final int partitionEnd;

    private final int[] outputChannels;
    private final List<WindowFunction> windowFunctions;
    private final FrameInfo frameInfo;
    private final PagesHashStrategy peerGroupHashStrategy;

    private int peerGroupStart;
    private int peerGroupEnd;
    private int frameStart;
    private int frameEnd;

    private int currentPosition;

    public WindowPartition(PagesIndex pagesIndex,
            int partitionStart,
            int partitionEnd,
            int[] outputChannels,
            List<WindowFunction> windowFunctions,
            FrameInfo frameInfo,
            PagesHashStrategy peerGroupHashStrategy)
    {
        this.pagesIndex = pagesIndex;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.outputChannels = outputChannels;
        this.windowFunctions = windowFunctions;
        this.frameInfo = frameInfo;
        this.peerGroupHashStrategy = peerGroupHashStrategy;

        // reset functions for new partition
        WindowIndex windowIndex = new WindowIndex(pagesIndex, partitionStart, partitionEnd);
        for (WindowFunction windowFunction : windowFunctions) {
            windowFunction.reset(windowIndex);
        }

        currentPosition = partitionStart;
        updatePeerGroup();
    }

    public int getPartitionEnd()
    {
        return partitionEnd;
    }

    public boolean hasNext()
    {
        return currentPosition < partitionEnd;
    }

    public void processNextRow(PageBuilder pageBuilder)
    {
        checkState(hasNext(), "No more rows in partition");

        // copy output channels
        pageBuilder.declarePosition();
        int channel = 0;
        while (channel < outputChannels.length) {
            pagesIndex.appendTo(outputChannels[channel], currentPosition, pageBuilder.getBlockBuilder(channel));
            channel++;
        }

        // check for new peer group
        if (currentPosition == peerGroupEnd) {
            updatePeerGroup();
        }

        // compute window frame
        updateFrame();

        // process window functions
        for (WindowFunction function : windowFunctions) {
            function.processRow(
                    pageBuilder.getBlockBuilder(channel),
                    peerGroupStart - partitionStart,
                    peerGroupEnd - partitionStart - 1,
                    frameStart,
                    frameEnd);
            channel++;
        }

        currentPosition++;
    }

    private void updatePeerGroup()
    {
        peerGroupStart = currentPosition;
        // find end of peer group
        peerGroupEnd = peerGroupStart + 1;
        while ((peerGroupEnd < partitionEnd) && pagesIndex.positionEqualsPosition(peerGroupHashStrategy, peerGroupStart, peerGroupEnd)) {
            peerGroupEnd++;
        }
    }

    private void updateFrame()
    {
        int rowPosition = currentPosition - partitionStart;
        int endPosition = partitionEnd - partitionStart - 1;

        // frame start
        if (frameInfo.getStartType() == UNBOUNDED_PRECEDING) {
            frameStart = 0;
        }
        else if (frameInfo.getStartType() == PRECEDING) {
            frameStart = preceding(rowPosition, getStartValue());
        }
        else if (frameInfo.getStartType() == FOLLOWING) {
            frameStart = following(rowPosition, endPosition, getStartValue());
        }
        else if (frameInfo.getType() == RANGE) {
            frameStart = peerGroupStart - partitionStart;
        }
        else {
            frameStart = rowPosition;
        }

        // frame end
        if (frameInfo.getEndType() == UNBOUNDED_FOLLOWING) {
            frameEnd = endPosition;
        }
        else if (frameInfo.getEndType() == PRECEDING) {
            frameEnd = preceding(rowPosition, getEndValue());
        }
        else if (frameInfo.getEndType() == FOLLOWING) {
            frameEnd = following(rowPosition, endPosition, getEndValue());
        }
        else if (frameInfo.getType() == RANGE) {
            frameEnd = peerGroupEnd - partitionStart - 1;
        }
        else {
            frameEnd = rowPosition;
        }

        // handle empty frame
        if (emptyFrame(rowPosition, endPosition)) {
            frameStart = -1;
            frameEnd = -1;
        }
    }

    private boolean emptyFrame(int rowPosition, int endPosition)
    {
        FrameBound.Type startType = frameInfo.getStartType();
        FrameBound.Type endType = frameInfo.getEndType();

        int positions = endPosition - rowPosition;

        if ((startType == UNBOUNDED_PRECEDING) && (endType == PRECEDING)) {
            return getEndValue() > rowPosition;
        }

        if ((startType == FOLLOWING) && (endType == UNBOUNDED_FOLLOWING)) {
            return getStartValue() > positions;
        }

        if (startType != endType) {
            return false;
        }

        FrameBound.Type type = frameInfo.getStartType();
        if ((type != PRECEDING) && (type != FOLLOWING)) {
            return false;
        }

        long start = getStartValue();
        long end = getEndValue();

        if (type == PRECEDING) {
            return (start < end) || ((start > rowPosition) && (end > rowPosition));
        }

        return (start > end) || ((start > positions) && (end > positions));
    }

    private static int preceding(int rowPosition, long value)
    {
        if (value > rowPosition) {
            return 0;
        }
        return Ints.checkedCast(rowPosition - value);
    }

    private static int following(int rowPosition, int endPosition, long value)
    {
        if (value > (endPosition - rowPosition)) {
            return endPosition;
        }
        return Ints.checkedCast(rowPosition + value);
    }

    private long getStartValue()
    {
        return getFrameValue(frameInfo.getStartChannel(), "starting");
    }

    private long getEndValue()
    {
        return getFrameValue(frameInfo.getEndChannel(), "ending");
    }

    private long getFrameValue(int channel, String type)
    {
        checkCondition(!pagesIndex.isNull(channel, currentPosition), INVALID_WINDOW_FRAME, "Window frame %s offset must not be null", type);
        long value = pagesIndex.getLong(channel, currentPosition);
        checkCondition(value >= 0, INVALID_WINDOW_FRAME, "Window frame %s offset must not be negative", value);
        return value;
    }
}
