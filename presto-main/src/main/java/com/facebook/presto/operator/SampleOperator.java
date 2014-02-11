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

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.math3.distribution.PoissonDistribution;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class SampleOperator
        implements Operator
{
    public static class SampleOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final double sampleRatio;

        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public SampleOperatorFactory(int operatorId, double sampleRatio, List<TupleInfo> sourceTupleInfos)
        {
            this.operatorId = operatorId;
            this.sampleRatio = sampleRatio;
            this.tupleInfos = ImmutableList.<TupleInfo>builder()
                    .addAll(checkNotNull(sourceTupleInfos, "sourceTupleInfos is null"))
                    .add(TupleInfo.SINGLE_LONG)
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, SampleOperator.class.getSimpleName());
            return new SampleOperator(operatorContext, sampleRatio, tupleInfos);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<TupleInfo> tupleInfos;
    private final PageBuilder pageBuilder;
    private final BlockCursor[] cursors;
    private final PoissonDistribution poisson;
    private final int sampleWeightChannel;
    private boolean finishing;
    private int remainingPositions;

    public SampleOperator(OperatorContext operatorContext, double sampleRatio, List<TupleInfo> tupleInfos)
    {
        //Note: Poissonized Samples can be larger than the original dataset if desired
        checkArgument(sampleRatio > 0, "sample ratio must be strictly positive");

        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
        this.pageBuilder = new PageBuilder(tupleInfos);
        this.cursors = new BlockCursor[tupleInfos.size() - 1];
        this.poisson = new PoissonDistribution(sampleRatio);
        this.sampleWeightChannel = tupleInfos.size() - 1;
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
    public final void finish()
    {
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty() && remainingPositions == 0;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public final boolean needsInput()
    {
        return !finishing && !pageBuilder.isFull() && remainingPositions == 0;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        checkState(!pageBuilder.isFull(), "Page buffer is full");
        checkState(remainingPositions == 0, "previous page has not been completely processed");

        for (int i = 0; i < page.getChannelCount(); i++) {
            cursors[i] = page.getBlock(i).cursor();
        }
        remainingPositions = page.getPositionCount();
    }

    @Override
    public Page getOutput()
    {
        for (; remainingPositions > 0 && !pageBuilder.isFull(); remainingPositions--) {
            for (BlockCursor cursor : cursors) {
                checkState(cursor.advanceNextPosition());
            }

            int repeats = poisson.sample();
            if (repeats > 0) {
                for (int i = 0; i < cursors.length; i++) {
                    cursors[i].appendTupleTo(pageBuilder.getBlockBuilder(i));
                }
                pageBuilder.getBlockBuilder(sampleWeightChannel).append(repeats);
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty())) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }
}
