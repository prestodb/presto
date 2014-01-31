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
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.PoissonDistributionImpl;

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

        private final List<ProjectionFunction> projections;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public SampleOperatorFactory(int operatorId, double sampleRatio, List<ProjectionFunction> projections)
        {
            this.operatorId = operatorId;
            this.sampleRatio = sampleRatio;
            this.projections = ImmutableList.copyOf(checkNotNull(projections, "projections is null"));
            this.tupleInfos = toTupleInfos(projections);
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
            return new SampleOperator(operatorContext, sampleRatio, projections, tupleInfos);
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
    private boolean finishing;
    private double sampleRatio;
    private final List<ProjectionFunction> projections;

    public SampleOperator(OperatorContext operatorContext, double sampleRatio, List<ProjectionFunction> projections, List<TupleInfo> tupleInfos)
    {
        //Note: Poissonized Samples can be larger than the original dataset if desired
        checkArgument(sampleRatio >= 0.0, "sample ratio must be at least zero");

        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.sampleRatio = sampleRatio;
        this.projections = ImmutableList.copyOf(projections);
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
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
    public final void finish()
    {
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public final boolean needsInput()
    {
        return !finishing && !pageBuilder.isFull();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        checkState(!pageBuilder.isFull(), "Page buffer is full");

        Block[] blocks = page.getBlocks();

        int rows = page.getPositionCount();

        BlockCursor[] cursors = new BlockCursor[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            cursors[i] = blocks[i].cursor();
        }

        PoissonDistributionImpl pdi = (sampleRatio > 0) ? new PoissonDistributionImpl(sampleRatio) : null;

        for (int position = 0; position < rows; position++) {
            for (BlockCursor cursor : cursors) {
                checkState(cursor.advanceNextPosition());
            }

            try {
                int repeats = (sampleRatio > 0) ? pdi.sample() : 0;

                for (int j = 0; j < repeats; j++) {
                    for (int i = 0; i < projections.size(); i++) {
                        // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                        projections.get(i).project(cursors, pageBuilder.getBlockBuilder(i));
                    }
                }
            }
            catch (MathException e) {
                throw Throwables.propagate(e);
            }
        }

        for (BlockCursor cursor : cursors) {
            checkState(!cursor.advanceNextPosition());
        }
    }

    @Override
    public Page getOutput()
    {
        if (needsInput() || pageBuilder.isEmpty()) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private static List<TupleInfo> toTupleInfos(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        return tupleInfos.build();
    }
}
