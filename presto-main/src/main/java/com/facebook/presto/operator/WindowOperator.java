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

import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.operator.window.WindowIndex;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.sql.tree.FrameBound.Type.FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.PRECEDING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;

public class WindowOperator
        implements Operator
{
    public static class WindowOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<WindowFunctionDefinition> windowFunctionDefinitions;
        private final List<Type> partitionTypes;
        private final List<Integer> partitionChannels;
        private final List<Type> sortTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final WindowFrame.Type frameType;
        private final FrameBound.Type frameStartType;
        private final Optional<Integer> frameStartChannel;
        private final FrameBound.Type frameEndType;
        private final Optional<Integer> frameEndChannel;
        private final int expectedPositions;
        private final List<Type> types;
        private boolean closed;

        public WindowOperatorFactory(
                int operatorId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<WindowFunctionDefinition> windowFunctionDefinitions,
                List<Integer> partitionChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                WindowFrame.Type frameType,
                FrameBound.Type frameStartType,
                Optional<Integer> frameStartChannel,
                FrameBound.Type frameEndType,
                Optional<Integer> frameEndChannel,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(checkNotNull(outputChannels, "outputChannels is null"));
            this.windowFunctionDefinitions = windowFunctionDefinitions;
            ImmutableList.Builder<Type> partitionTypes = ImmutableList.builder();
            for (int channel : partitionChannels) {
                partitionTypes.add(sourceTypes.get(channel));
            }
            this.partitionTypes = partitionTypes.build();
            this.partitionChannels = ImmutableList.copyOf(checkNotNull(partitionChannels, "partitionChannels is null"));
            ImmutableList.Builder<Type> sortTypes = ImmutableList.builder();
            for (int channel : sortChannels) {
                sortTypes.add(sourceTypes.get(channel));
            }
            this.sortTypes = sortTypes.build();
            this.sortChannels = ImmutableList.copyOf(checkNotNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(checkNotNull(sortOrder, "sortOrder is null"));

            this.frameType = checkNotNull(frameType, "frameType is null");
            this.frameStartType = checkNotNull(frameStartType, "frameStartType is null");
            this.frameStartChannel = checkNotNull(frameStartChannel, "frameStartChannel is null");
            this.frameEndType = checkNotNull(frameEndType, "frameEndType is null");
            this.frameEndChannel = checkNotNull(frameEndChannel, "frameEndChannel is null");

            this.expectedPositions = expectedPositions;

            this.types = toTypes(sourceTypes, outputChannels, toWindowFunctions(windowFunctionDefinitions));
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, WindowOperator.class.getSimpleName());
            return new WindowOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    windowFunctionDefinitions,
                    partitionTypes,
                    partitionChannels,
                    sortTypes,
                    sortChannels,
                    sortOrder,
                    frameType,
                    frameStartType,
                    frameStartChannel,
                    frameEndType,
                    frameEndChannel,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final int[] outputChannels;
    private final List<WindowFunction> windowFunctions;
    private final List<Type> partitionTypes;
    private final List<Integer> partitionChannels;
    private final List<Type> sortTypes;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final List<Type> types;

    private final boolean frameRange;
    private final FrameBound.Type frameStartType;
    private final int frameStartChannel;
    private final FrameBound.Type frameEndType;
    private final int frameEndChannel;

    private final PagesIndex pagesIndex;

    private final PageBuilder pageBuilder;

    private State state = State.NEEDS_INPUT;

    private int currentPosition;

    private IntComparator partitionComparator;
    private IntComparator orderComparator;

    private int partitionStart;
    private int partitionEnd;
    private int peerGroupStart;
    private int peerGroupEnd;
    private int frameStart;
    private int frameEnd;

    public WindowOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> windowFunctionDefinitions,
            List<Type> partitionTypes, List<Integer> partitionChannels,
            List<Type> sortTypes, List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            WindowFrame.Type frameType,
            FrameBound.Type frameStartType,
            Optional<Integer> frameStartChannel,
            FrameBound.Type frameEndType,
            Optional<Integer> frameEndChannel,
            int expectedPositions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(checkNotNull(outputChannels, "outputChannels is null"));
        this.windowFunctions = toWindowFunctions(checkNotNull(windowFunctionDefinitions, "windowFunctionDefinitions is null"));
        this.partitionTypes = ImmutableList.copyOf(checkNotNull(partitionTypes, "partitionTypes is null"));
        this.partitionChannels = ImmutableList.copyOf(checkNotNull(partitionChannels, "partitionChannels is null"));
        this.sortTypes = ImmutableList.copyOf(checkNotNull(sortTypes, "sortTypes is null"));
        this.sortChannels = ImmutableList.copyOf(checkNotNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(checkNotNull(sortOrder, "sortOrder is null"));

        this.frameRange = (checkNotNull(frameType, "frameType is null") == WindowFrame.Type.RANGE);
        this.frameStartType = checkNotNull(frameStartType, "frameStartType is null");
        this.frameStartChannel = checkNotNull(frameStartChannel, "frameStartChannel is null").orElse(-1);
        this.frameEndType = checkNotNull(frameEndType, "frameEndType is null");
        this.frameEndChannel = checkNotNull(frameEndChannel, "frameEndChannel is null").orElse(-1);

        this.types = toTypes(sourceTypes, outputChannels, windowFunctions);

        this.pagesIndex = new PagesIndex(sourceTypes, expectedPositions, operatorContext);
        this.pageBuilder = new PageBuilder(this.types);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            // we partition by ordering the values so partitions are sequential values
            List<SortOrder> partitionOrder = Collections.nCopies(partitionChannels.size(), ASC_NULLS_LAST);

            // sort everything by partition channels, then sort channels
            List<Integer> orderChannels = ImmutableList.copyOf(concat(partitionChannels, sortChannels));
            List<SortOrder> ordering = ImmutableList.copyOf(concat(partitionOrder, sortOrder));
            List<Type> orderingTypes = ImmutableList.copyOf(concat(partitionTypes, sortTypes));

            // sort the index
            pagesIndex.sort(orderingTypes, orderChannels, ordering);

            // create partition comparator
            partitionComparator = pagesIndex.createComparator(orderingTypes, partitionChannels, partitionOrder);

            // create order comparator
            orderComparator = pagesIndex.createComparator(orderingTypes, sortChannels, sortOrder);
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        checkNotNull(page, "page is null");

        pagesIndex.addPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        if (currentPosition >= pagesIndex.getPositionCount()) {
            state = State.FINISHED;
            return null;
        }

        // iterate through the positions sequentially until we have one full page
        pageBuilder.reset();
        while (!pageBuilder.isFull() && currentPosition < pagesIndex.getPositionCount()) {
            // check for new partition
            boolean newPartition = (currentPosition == 0) || (currentPosition == partitionEnd);
            if (newPartition) {
                partitionStart = currentPosition;
                // find end of partition
                partitionEnd++;
                while ((partitionEnd < pagesIndex.getPositionCount()) &&
                        (partitionComparator.compare(partitionEnd - 1, partitionEnd) == 0)) {
                    partitionEnd++;
                }

                // reset functions for new partition
                WindowIndex windowIndex = new WindowIndex(pagesIndex, partitionStart, partitionEnd);
                for (WindowFunction function : windowFunctions) {
                    function.reset(windowIndex);
                }
            }

            // copy output channels
            pageBuilder.declarePosition();
            int channel = 0;
            while (channel < outputChannels.length) {
                pagesIndex.appendTo(outputChannels[channel], currentPosition, pageBuilder.getBlockBuilder(channel));
                channel++;
            }

            // check for new peer group
            boolean newPeerGroup = newPartition || (currentPosition == peerGroupEnd);
            if (newPeerGroup) {
                peerGroupStart = currentPosition;
                // find end of peer group
                peerGroupEnd++;
                while ((peerGroupEnd < partitionEnd) &&
                        (orderComparator.compare(peerGroupEnd - 1, peerGroupEnd) == 0)) {
                    peerGroupEnd++;
                }
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

        // output the page if we have any data
        if (pageBuilder.isEmpty()) {
            state = State.FINISHED;
            return null;
        }

        Page page = pageBuilder.build();
        return page;
    }

    private void updateFrame()
    {
        int rowPosition = currentPosition - partitionStart;
        int endPosition = partitionEnd - partitionStart - 1;

        // frame start
        if (frameStartType == UNBOUNDED_PRECEDING) {
            frameStart = 0;
        }
        else if (frameStartType == PRECEDING) {
            frameStart = preceding(rowPosition, getStartValue());
        }
        else if (frameStartType == FOLLOWING) {
            frameStart = following(rowPosition, endPosition, getStartValue());
        }
        else if (frameRange) {
            frameStart = peerGroupStart - partitionStart;
        }
        else {
            frameStart = rowPosition;
        }

        // frame end
        if (frameEndType == UNBOUNDED_FOLLOWING) {
            frameEnd = endPosition;
        }
        else if (frameEndType == PRECEDING) {
            frameEnd = preceding(rowPosition, getEndValue());
        }
        else if (frameEndType == FOLLOWING) {
            frameEnd = following(rowPosition, endPosition, getEndValue());
        }
        else if (frameRange) {
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
        if (frameStartType != frameEndType) {
            return false;
        }

        FrameBound.Type type = frameStartType;
        if ((type != PRECEDING) && (type != FOLLOWING)) {
            return false;
        }

        long start = getStartValue();
        long end = getEndValue();

        if (type == PRECEDING) {
            return (start < end) || ((start > rowPosition) && (end > rowPosition));
        }

        int positions = endPosition - rowPosition;
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
        return getFrameValue(frameStartChannel, "starting");
    }

    private long getEndValue()
    {
        return getFrameValue(frameEndChannel, "ending");
    }

    private long getFrameValue(int channel, String type)
    {
        checkCondition(!pagesIndex.isNull(channel, currentPosition), INVALID_WINDOW_FRAME, "Window frame %s offset must not be null", type);
        long value = pagesIndex.getLong(channel, currentPosition);
        checkCondition(value >= 0, INVALID_WINDOW_FRAME, "Window frame %s offset must not be negative");
        return value;
    }

    private static List<Type> toTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels, List<WindowFunction> windowFunctions)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int channel : outputChannels) {
            types.add(sourceTypes.get(channel));
        }
        for (WindowFunction function : windowFunctions) {
            types.add(function.getType());
        }
        return types.build();
    }

    private static List<WindowFunction> toWindowFunctions(List<WindowFunctionDefinition> windowFunctionDefinitions)
    {
        ImmutableList.Builder<WindowFunction> builder = ImmutableList.builder();
        for (WindowFunctionDefinition windowFunctionDefinition : windowFunctionDefinitions) {
            builder.add(windowFunctionDefinition.createWindowFunction());
        }
        return builder.build();
    }
}
