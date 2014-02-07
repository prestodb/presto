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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MarkDistinctOperator
        implements Operator
{
    public static class MarkDistinctOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final int[] markDistinctChannels;
        private final List<TupleInfo> tupleInfos;
        private final Optional<Integer> sampleWeightChannel;
        private boolean closed;

        public MarkDistinctOperatorFactory(int operatorId, List<TupleInfo> sourceTupleInfos, Collection<Integer> markDistinctChannels, Optional<Integer> sampleWeightChannel)
        {
            this.operatorId = operatorId;
            checkNotNull(markDistinctChannels, "markDistinctChannels is null");
            checkArgument(!markDistinctChannels.isEmpty(), "markDistinctChannels is empty");
            checkNotNull(sampleWeightChannel, "sampleWeightChannel is null");
            this.markDistinctChannels = Ints.toArray(markDistinctChannels);
            this.sampleWeightChannel = sampleWeightChannel;

            this.tupleInfos = ImmutableList.<TupleInfo>builder()
                    .addAll(sourceTupleInfos)
                    .add(TupleInfo.SINGLE_BOOLEAN)
                    .build();
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MarkDistinctOperator.class.getSimpleName());
            if (sampleWeightChannel.isPresent()) {
                return new MarkDistinctSampledOperator(operatorContext, tupleInfos, markDistinctChannels, sampleWeightChannel.get());
            }
            else {
                return new MarkDistinctOperator(operatorContext, tupleInfos, markDistinctChannels);
            }
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<TupleInfo> tupleInfos;
    private final MarkDistinctHash markDistinctHash;

    private Page outputPage;
    private boolean finishing;

    public MarkDistinctOperator(OperatorContext operatorContext, List<TupleInfo> tupleInfos, int[] markDistinctChannels)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        checkNotNull(tupleInfos, "tupleInfos is null");
        checkArgument(markDistinctChannels.length >= 0, "markDistinctChannels is empty");

        ImmutableList.Builder<TupleInfo.Type> types = ImmutableList.builder();

        for (int channel : markDistinctChannels) {
            types.add(tupleInfos.get(channel).getType());
        }

        this.markDistinctHash = new MarkDistinctHash(types.build(), markDistinctChannels);

        this.tupleInfos = tupleInfos;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && outputPage == null;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());
        if (finishing || outputPage != null) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(outputPage == null, "Operator still has pending output");
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());

        Block markerBlock = markDistinctHash.markDistinctRows(page);

        // add the new boolean column to the page
        Block[] sourceBlocks = page.getBlocks();
        Block[] outputBlocks = new Block[sourceBlocks.length + 1]; // +1 for the single boolean output channel

        System.arraycopy(sourceBlocks, 0, outputBlocks, 0, sourceBlocks.length);
        outputBlocks[sourceBlocks.length] = markerBlock;

        outputPage = new Page(outputBlocks);
    }

    @Override
    public Page getOutput()
    {
        Page result = outputPage;
        outputPage = null;
        return result;
    }
}

class MarkDistinctSampledOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<TupleInfo> tupleInfos;
    private final MarkDistinctHash markDistinctHash;
    private final int sampleWeightChannel;
    private final int markerChannel;

    private BlockCursor[] cursors;
    private BlockCursor markerCursor;
    private boolean finishing;
    private PageBuilder pageBuilder;
    private long sampleWeight;
    private boolean distinct;

    public MarkDistinctSampledOperator(OperatorContext operatorContext, List<TupleInfo> tupleInfos, int[] markDistinctChannels, int sampleWeightChannel)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        checkNotNull(tupleInfos, "tupleInfos is null");
        checkArgument(markDistinctChannels.length >= 0, "markDistinctChannels is empty");
        this.sampleWeightChannel = sampleWeightChannel;
        // Add marker at end of columns
        this.markerChannel = tupleInfos.size() - 1;

        ImmutableList.Builder<TupleInfo.Type> types = ImmutableList.builder();

        for (int channel : markDistinctChannels) {
            types.add(tupleInfos.get(channel).getType());
        }

        this.markDistinctHash = new MarkDistinctHash(types.build(), markDistinctChannels);

        this.tupleInfos = tupleInfos;
        this.pageBuilder = new PageBuilder(tupleInfos);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && markerCursor == null && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());
        if (finishing || markerCursor != null) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(markerCursor == null, "Current page has not been completely processed yet");
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());

        markerCursor = markDistinctHash.markDistinctRows(page).cursor();

        this.cursors = new BlockCursor[page.getChannelCount()];
        for (int i = 0; i < page.getChannelCount(); i++) {
            this.cursors[i] = page.getBlock(i).cursor();
        }
    }

    private boolean advance()
    {
        if (markerCursor == null) {
            return false;
        }

        if (distinct && sampleWeight > 1) {
            distinct = false;
            sampleWeight--;
            return true;
        }

        boolean advanced = markerCursor.advanceNextPosition();
        for (BlockCursor cursor : cursors) {
            checkState(advanced == cursor.advanceNextPosition());
        }

        if (!advanced) {
            markerCursor = null;
            Arrays.fill(cursors, null);
        }
        else {
            sampleWeight = cursors[sampleWeightChannel].getLong();
            distinct = markerCursor.getBoolean();
        }

        return advanced;
    }

    @Override
    public Page getOutput()
    {
        // Build the weight block, giving all distinct rows a weight of one. advance() handles splitting rows with weight > 1, if they're distinct
        while (!pageBuilder.isFull() && advance()) {
            for (int i = 0; i < cursors.length; i++) {
                BlockBuilder builder = pageBuilder.getBlockBuilder(i);
                if (i == sampleWeightChannel) {
                    if (distinct) {
                        builder.append(1);
                    }
                    else {
                        builder.append(sampleWeight);
                    }
                }
                else {
                    builder.append(cursors[i]);
                }
            }
            pageBuilder.getBlockBuilder(markerChannel).append(distinct);
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty() && markerCursor == null)) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }
}
