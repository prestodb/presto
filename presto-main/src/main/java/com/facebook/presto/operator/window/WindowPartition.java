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
import com.facebook.presto.spi.function.WindowIndex;
import com.facebook.presto.sql.tree.FrameBound;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.tree.FrameBound.Type.FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.PRECEDING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.tree.WindowFrame.Type.RANGE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;

public final class WindowPartition
{
    private final PagesIndex pagesIndex;
    private final int partitionStart;
    private final int partitionEnd;

    private final int[] outputChannels;
    private final List<FramedWindowFunction> windowFunctions;
    private final PagesHashStrategy peerGroupHashStrategy;

    private int peerGroupStart;
    private int peerGroupEnd;

    private int currentPosition;

    public WindowPartition(PagesIndex pagesIndex,
            int partitionStart,
            int partitionEnd,
            int[] outputChannels,
            List<FramedWindowFunction> windowFunctions,
            PagesHashStrategy peerGroupHashStrategy)
    {
        this.pagesIndex = pagesIndex;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.outputChannels = outputChannels;
        this.windowFunctions = ImmutableList.copyOf(windowFunctions);
        this.peerGroupHashStrategy = peerGroupHashStrategy;

        // reset functions for new partition
        WindowIndex windowIndex = new PagesWindowIndex(pagesIndex, partitionStart, partitionEnd);
        for (FramedWindowFunction framedWindowFunction : windowFunctions) {
            framedWindowFunction.getFunction().reset(windowIndex);
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

        for (FramedWindowFunction framedFunction : windowFunctions) {
            Range range = getFrameRange(framedFunction.getFrame());
            framedFunction.getFunction().processRow(
                    pageBuilder.getBlockBuilder(channel),
                    peerGroupStart - partitionStart,
                    peerGroupEnd - partitionStart - 1,
                    range.start,
                    range.end);
            channel++;
        }

        currentPosition++;
    }

    private static class Range
    {
        private final int start;
        private final int end;

        Range(int start, int end)
        {
            this.start = start;
            this.end = end;
        }

        public int getStart()
        {
            return start;
        }

        public int getEnd()
        {
            return end;
        }
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

    private Range getFrameRange(FrameInfo frameInfo)
    {
        int rowPosition = currentPosition - partitionStart;
        int endPosition = partitionEnd - partitionStart - 1;

        int frameStart;
        int frameEnd;

        // frame start
        if (frameInfo.getStartType() == UNBOUNDED_PRECEDING) {
            frameStart = 0;
        }
        else if (frameInfo.getStartType() == PRECEDING) {
            frameStart = preceding(rowPosition, getStartValue(frameInfo));
        }
        else if (frameInfo.getStartType() == FOLLOWING) {
            frameStart = following(rowPosition, endPosition, getStartValue(frameInfo));
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
            frameEnd = preceding(rowPosition, getEndValue(frameInfo));
        }
        else if (frameInfo.getEndType() == FOLLOWING) {
            frameEnd = following(rowPosition, endPosition, getEndValue(frameInfo));
        }
        else if (frameInfo.getType() == RANGE) {
            frameEnd = peerGroupEnd - partitionStart - 1;
        }
        else {
            frameEnd = rowPosition;
        }

        // handle empty frame
        if (emptyFrame(frameInfo, rowPosition, endPosition)) {
            frameStart = -1;
            frameEnd = -1;
        }

        return new Range(frameStart, frameEnd);
    }

    private boolean emptyFrame(FrameInfo frameInfo, int rowPosition, int endPosition)
    {
        FrameBound.Type startType = frameInfo.getStartType();
        FrameBound.Type endType = frameInfo.getEndType();

        int positions = endPosition - rowPosition;

        if ((startType == UNBOUNDED_PRECEDING) && (endType == PRECEDING)) {
            return getEndValue(frameInfo) > rowPosition;
        }

        if ((startType == FOLLOWING) && (endType == UNBOUNDED_FOLLOWING)) {
            return getStartValue(frameInfo) > positions;
        }

        if (startType != endType) {
            return false;
        }

        FrameBound.Type type = frameInfo.getStartType();
        if ((type != PRECEDING) && (type != FOLLOWING)) {
            return false;
        }

        long start = getStartValue(frameInfo);
        long end = getEndValue(frameInfo);

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
        return toIntExact(rowPosition - value);
    }

    private static int following(int rowPosition, int endPosition, long value)
    {
        if (value > (endPosition - rowPosition)) {
            return endPosition;
        }
        return toIntExact(rowPosition + value);
    }

    private long getStartValue(FrameInfo frameInfo)
    {
        return getFrameValue(frameInfo.getStartChannel(), "starting");
    }

    private long getEndValue(FrameInfo frameInfo)
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
