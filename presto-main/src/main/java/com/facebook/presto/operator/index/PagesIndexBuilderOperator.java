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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class PagesIndexBuilderOperator
    implements Operator
{
    public static class PagesIndexSupplier
    {
        private final List<TupleInfo> tupleInfos;
        private final BlockingQueue<PagesIndex> pagesIndexes = new LinkedBlockingQueue<>();

        public PagesIndexSupplier(List<TupleInfo> tupleInfos)
        {
            this.tupleInfos = ImmutableList.copyOf(checkNotNull(tupleInfos, "tupleInfos is null"));
        }

        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        public ListenableFuture<PagesIndex> getPagesIndex()
        {
            try {
                return Futures.immediateFuture(pagesIndexes.take());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }

        public void addPagesIndex(PagesIndex pagesIndex)
        {
            pagesIndexes.add(pagesIndex);
        }
    }

    public static class PagesIndexBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PagesIndexSupplier pagesIndexSupplier;
        private final int expectedPositions;
        private boolean closed;

        public PagesIndexBuilderOperatorFactory(
                int operatorId,
                List<TupleInfo> tupleInfos,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.pagesIndexSupplier = new PagesIndexSupplier(checkNotNull(tupleInfos, "tupleInfos is null"));
            this.expectedPositions = checkNotNull(expectedPositions, "expectedPositions is null");
        }

        public PagesIndexSupplier getPagesIndexSupplier()
        {
            return pagesIndexSupplier;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return pagesIndexSupplier.getTupleInfos();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, PagesIndexBuilderOperator.class.getSimpleName());
            return new PagesIndexBuilderOperator(
                    operatorContext,
                    pagesIndexSupplier,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PagesIndexSupplier pagesIndexSupplier;

    private final PagesIndex pagesIndex;

    private boolean finished;

    public PagesIndexBuilderOperator(
            OperatorContext operatorContext,
            PagesIndexSupplier pagesIndexSupplier,
            int expectedPositions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.pagesIndexSupplier = checkNotNull(pagesIndexSupplier, "pagesIndexSupplier is null");
        this.pagesIndex = new PagesIndex(pagesIndexSupplier.getTupleInfos(), expectedPositions, operatorContext);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return pagesIndexSupplier.getTupleInfos();
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        pagesIndexSupplier.addPagesIndex(pagesIndex);
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        pagesIndex.addPage(page);
        operatorContext.recordGeneratedOutput(page.getDataSize(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
