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

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.operator.PagesHashStrategy;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PagesIndexComparator;
import com.facebook.presto.operator.WindowOperator.FrameBoundKey;
import com.facebook.presto.spi.function.WindowIndex;
import com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType;
import com.facebook.presto.sql.tree.SortItem.Ordering;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.facebook.presto.operator.WindowOperator.FrameBoundKey.Type.END;
import static com.facebook.presto.operator.WindowOperator.FrameBoundKey.Type.START;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.CURRENT_ROW;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.FOLLOWING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;
import static com.facebook.presto.sql.tree.SortItem.Ordering.DESCENDING;
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

    // Recently computed frame bounds for functions with frame type RANGE.
    // When computing frame start and frame end for a row, frame bounds for the previous row
    // are used as the starting point. Then they are moved backward or forward based on the sort order
    // until the matching position for a current row is found.
    // This approach is efficient in case when frame offset values are constant. It was chosen
    // based on the assumption that in most use cases frame offset is constant rather than
    // row-dependent.
    private final Map<Integer, Range> recentRanges;
    private final PagesHashStrategy peerGroupHashStrategy;
    private final Map<FrameBoundKey, PagesIndexComparator> frameBoundComparators;

    private int peerGroupStart;
    private int peerGroupEnd;

    private int currentPosition;

    public WindowPartition(
            PagesIndex pagesIndex,
            int partitionStart,
            int partitionEnd,
            int[] outputChannels,
            List<FramedWindowFunction> windowFunctions,
            PagesHashStrategy peerGroupHashStrategy,
            Map<FrameBoundKey, PagesIndexComparator> frameBoundComparators)
    {
        this.pagesIndex = pagesIndex;
        this.partitionStart = partitionStart;
        this.partitionEnd = partitionEnd;
        this.outputChannels = outputChannels;
        this.windowFunctions = ImmutableList.copyOf(windowFunctions);
        this.peerGroupHashStrategy = peerGroupHashStrategy;
        this.frameBoundComparators = frameBoundComparators;

        // reset functions for new partition
        WindowIndex windowIndex = new PagesWindowIndex(pagesIndex, partitionStart, partitionEnd);
        for (FramedWindowFunction framedWindowFunction : windowFunctions) {
            framedWindowFunction.getFunction().reset(windowIndex);
        }

        currentPosition = partitionStart;
        updatePeerGroup();

        recentRanges = initializeRangeCache(partitionStart, partitionEnd, peerGroupEnd, windowFunctions);
    }

    private static Map<Integer, Range> initializeRangeCache(int partitionStart, int partitionEnd, int peerGroupEnd, List<FramedWindowFunction> windowFunctions)
    {
        Map<Integer, Range> ranges = new HashMap<>();
        Range initialPeerRange = new Range(0, peerGroupEnd - partitionStart - 1);
        Range initialUnboundedRange = new Range(0, partitionEnd - partitionStart - 1);
        for (int i = 0; i < windowFunctions.size(); i++) {
            FrameInfo frame = windowFunctions.get(i).getFrame();
            if (frame.getType() == RANGE) {
                if (frame.getEndType() == UNBOUNDED_FOLLOWING) {
                    ranges.put(i, initialUnboundedRange);
                }
                else {
                    ranges.put(i, initialPeerRange);
                }
            }
        }

        return ranges;
    }

    public int getPartitionStart()
    {
        return partitionStart;
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

        for (int i = 0; i < windowFunctions.size(); i++) {
            FramedWindowFunction framedFunction = windowFunctions.get(i);
            Range range = getFrameRange(framedFunction.getFrame(), i);
            framedFunction.getFunction().processRow(
                    pageBuilder.getBlockBuilder(channel),
                    peerGroupStart - partitionStart,
                    peerGroupEnd - partitionStart - 1,
                    range.getStart(),
                    range.getEnd());
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

    private Range getFrameRange(FrameInfo frameInfo, int functionIndex)
    {
        switch (frameInfo.getType()) {
            case RANGE:
                Range range = getFrameRange(
                        frameInfo,
                        recentRanges.get(functionIndex),
                        frameBoundComparators.get(new FrameBoundKey(functionIndex, START)),
                        frameBoundComparators.get(new FrameBoundKey(functionIndex, END)));
                // handle empty frame. If the frame is out of partition bounds, record the nearest valid frame as the 'recentRange' for the next row.
                if (emptyFrame(range)) {
                    recentRanges.put(functionIndex, nearestValidFrame(range));
                    return new Range(-1, -1);
                }
                recentRanges.put(functionIndex, range);
                return range;
            case ROWS:
                return getFrameRange(frameInfo);
            default:
                throw new IllegalArgumentException("Unsupported frame type: " + frameInfo.getType());
        }
    }

    private Range getFrameRange(FrameInfo frameInfo)
    {
        int rowPosition = currentPosition - partitionStart;
        int endPosition = partitionEnd - partitionStart - 1;

        // handle empty frame
        if (emptyFrame(frameInfo, rowPosition, endPosition)) {
            return new Range(-1, -1);
        }

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
        else {
            frameEnd = rowPosition;
        }

        return new Range(frameStart, frameEnd);
    }

    private Range getFrameRange(FrameInfo frameInfo, Range recentRange, PagesIndexComparator startComparator, PagesIndexComparator endComparator)
    {
        // full partition
        if ((frameInfo.getStartType() == UNBOUNDED_PRECEDING && frameInfo.getEndType() == UNBOUNDED_FOLLOWING)) {
            return new Range(0, partitionEnd - partitionStart - 1);
        }

        // frame defined by peer group
        if ((frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == CURRENT_ROW) ||
                (frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == UNBOUNDED_FOLLOWING) ||
                (frameInfo.getStartType() == UNBOUNDED_PRECEDING && frameInfo.getEndType() == CURRENT_ROW)) {
            // same peer group as recent row
            if (currentPosition == partitionStart || pagesIndex.positionEqualsPosition(peerGroupHashStrategy, currentPosition - 1, currentPosition)) {
                return recentRange;
            }
            // next peer group
            return new Range(
                    frameInfo.getStartType() == UNBOUNDED_PRECEDING ? 0 : peerGroupStart - partitionStart,
                    frameInfo.getEndType() == UNBOUNDED_FOLLOWING ? partitionEnd - partitionStart - 1 : peerGroupEnd - partitionStart - 1);
        }

        // at this point, frame definition has at least one of: X PRECEDING, Y FOLLOWING
        // 1. leading or trailing nulls: frame consists of nulls peer group, possibly extended to partition start / end.
        // according to Spec, behavior of "X PRECEDING", "X FOLLOWING" frame boundaries is similar to "CURRENT ROW" for null values.
        if (pagesIndex.isNull(frameInfo.getSortKeyChannel(), currentPosition)) {
            return new Range(
                    frameInfo.getStartType() == UNBOUNDED_PRECEDING ? 0 : peerGroupStart - partitionStart,
                    frameInfo.getEndType() == UNBOUNDED_FOLLOWING ? partitionEnd - partitionStart - 1 : peerGroupEnd - partitionStart - 1);
        }

        // 2. non-null value in current row. Find frame boundaries starting from recentRange
        int frameStart;
        switch (frameInfo.getStartType()) {
            case UNBOUNDED_PRECEDING:
                frameStart = 0;
                break;
            case CURRENT_ROW:
                frameStart = peerGroupStart - partitionStart;
                break;
            case PRECEDING:
                frameStart = getFrameStartPreceding(recentRange.getStart(), frameInfo, startComparator);
                break;
            case FOLLOWING:
                // note: this is the only case where frameStart might get out of partition bound
                frameStart = getFrameStartFollowing(recentRange.getStart(), frameInfo, startComparator);
                break;
            default:
                // start type cannot be UNBOUNDED_FOLLOWING
                throw new IllegalArgumentException("Unsupported frame start type: " + frameInfo.getStartType());
        }

        int frameEnd;
        switch (frameInfo.getEndType()) {
            case UNBOUNDED_FOLLOWING:
                frameEnd = partitionEnd - partitionStart - 1;
                break;
            case CURRENT_ROW:
                frameEnd = peerGroupEnd - partitionStart - 1;
                break;
            case PRECEDING:
                // note: this is the only case where frameEnd might get out of partition bound
                frameEnd = getFrameEndPreceding(recentRange.getEnd(), frameInfo, endComparator);
                break;
            case FOLLOWING:
                frameEnd = getFrameEndFollowing(recentRange.getEnd(), frameInfo, endComparator);
                break;
            default:
                // end type cannot be UNBOUNDED_PRECEDING
                throw new IllegalArgumentException("Unsupported frame end type: " + frameInfo.getStartType());
        }

        return new Range(frameStart, frameEnd);
    }

    private int getFrameStartPreceding(int recent, FrameInfo frameInfo, PagesIndexComparator comparator)
    {
        int sortKeyChannel = frameInfo.getSortKeyChannelForStartComparison();
        Ordering ordering = frameInfo.getOrdering().get();

        // If the recent frame start points at a null, it means that we are now processing first non-null position.
        // For frame start "X PRECEDING", the frame starts at the first null for all null values, and it never includes nulls for non-null values.
        if (pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart + recent)) {
            return currentPosition - partitionStart;
        }

        return seek(
                comparator,
                sortKeyChannel,
                recent,
                -1,
                ordering == DESCENDING,
                0,
                p -> false);
    }

    private int getFrameStartFollowing(int recent, FrameInfo frameInfo, PagesIndexComparator comparator)
    {
        int sortKeyChannel = frameInfo.getSortKeyChannelForStartComparison();
        Ordering ordering = frameInfo.getOrdering().get();

        int position = recent;

        // If the recent frame start points at the beginning of partition and it is null, it means that we are now processing first non-null position.
        // frame start for first non-null position - leave section of leading nulls
        if (recent == 0 && pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart)) {
            position = currentPosition - partitionStart;
        }
        // leave section of trailing nulls
        while (pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart + position)) {
            position--;
        }

        return seek(
                comparator,
                sortKeyChannel,
                position,
                -1,
                ordering == DESCENDING,
                0,
                p -> p >= partitionEnd - partitionStart || pagesIndex.isNull(sortKeyChannel, partitionStart + p));
    }

    private int getFrameEndPreceding(int recent, FrameInfo frameInfo, PagesIndexComparator comparator)
    {
        int sortKeyChannel = frameInfo.getSortKeyChannelForEndComparison();
        Ordering ordering = frameInfo.getOrdering().get();

        int position = recent;

        // leave section of leading nulls
        while (pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart + position)) {
            position++;
        }

        return seek(
                comparator,
                sortKeyChannel,
                position,
                1,
                ordering == ASCENDING,
                partitionEnd - 1 - partitionStart,
                p -> p < 0 || pagesIndex.isNull(sortKeyChannel, partitionStart + p));
    }

    private int getFrameEndFollowing(int recent, FrameInfo frameInfo, PagesIndexComparator comparator)
    {
        Ordering ordering = frameInfo.getOrdering().get();
        int sortKeyChannel = frameInfo.getSortKeyChannelForEndComparison();

        int position = recent;

        // frame end for first non-null position - leave section of leading nulls
        if (pagesIndex.isNull(frameInfo.getSortKeyChannel(), partitionStart + recent)) {
            position = currentPosition - partitionStart;
        }

        return seek(
                comparator,
                sortKeyChannel,
                position,
                1,
                ordering == ASCENDING,
                partitionEnd - 1 - partitionStart,
                p -> false);
    }

    private int compare(PagesIndexComparator comparator, int left, int right, boolean reverse)
    {
        int result = comparator.compareTo(pagesIndex, left, right);

        if (reverse) {
            return -result;
        }

        return result;
    }

    // This method assumes that `sortKeyChannel` is not null at `position`
    private int seek(PagesIndexComparator comparator, int sortKeyChannel, int position, int step, boolean reverse, int limit, Predicate<Integer> bound)
    {
        int comparison = compare(comparator, partitionStart + position, currentPosition, reverse);
        while (comparison < 0) {
            position -= step;

            if (bound.test(position)) {
                return position;
            }

            comparison = compare(comparator, partitionStart + position, currentPosition, reverse);
        }
        while (true) {
            if (position == limit || pagesIndex.isNull(sortKeyChannel, partitionStart + position + step)) {
                break;
            }
            int newComparison = compare(comparator, partitionStart + position + step, currentPosition, reverse);
            if (newComparison >= 0) {
                position += step;
            }
            else {
                break;
            }
        }

        return position;
    }

    private boolean emptyFrame(Range range)
    {
        return range.getStart() > range.getEnd() ||
                range.getStart() >= partitionEnd - partitionStart ||
                range.getEnd() < 0;
    }

    /**
     * Return the nearest valid frame. A frame is valid if its start and end are within partition.
     * Note: A valid frame might be empty i.e. its end might be before its start.
     */
    private Range nearestValidFrame(Range range)
    {
        return new Range(
                Math.min(partitionEnd - partitionStart - 1, range.getStart()),
                Math.max(0, range.getEnd()));
    }

    private boolean emptyFrame(FrameInfo frameInfo, int rowPosition, int endPosition)
    {
        BoundType startType = frameInfo.getStartType();
        BoundType endType = frameInfo.getEndType();

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

        BoundType type = frameInfo.getStartType();
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
