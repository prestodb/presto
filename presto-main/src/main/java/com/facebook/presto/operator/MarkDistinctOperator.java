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
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MarkDistinctOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    public static class MarkDistinctOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final int[] markDistinctChannels;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public MarkDistinctOperatorFactory(int operatorId, List<TupleInfo> sourceTupleInfos, Collection<Integer> markDistinctChannels)
        {
            this.operatorId = operatorId;
            checkNotNull(markDistinctChannels, "markDistinctChannels is null");
            checkArgument(!markDistinctChannels.isEmpty(), "markDistinctChannels is empty");
            this.markDistinctChannels = Ints.toArray(markDistinctChannels);

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
            return new MarkDistinctOperator(operatorContext, tupleInfos, markDistinctChannels);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

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
