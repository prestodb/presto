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
import com.facebook.presto.operator.FinishedOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.RecordProjectOperator;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.spi.Index;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.MappedRecordSet;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class IndexSourceOperator
        implements SourceOperator
{
    public static class IndexSourceOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final Index index;
        private final List<Type> types;
        private final List<Integer> probeKeyRemap;
        private boolean closed;

        public IndexSourceOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                Index index,
                List<Type> types,
                List<Integer> probeKeyRemap)
        {
            this.operatorId = operatorId;
            this.sourceId = checkNotNull(sourceId, "sourceId is null");
            this.index = checkNotNull(index, "index is null");
            this.types = checkNotNull(types, "types is null");
            this.probeKeyRemap = ImmutableList.copyOf(checkNotNull(probeKeyRemap, "probeKeyRemap is null"));
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, IndexSourceOperator.class.getSimpleName());
            return new IndexSourceOperator(
                    operatorContext,
                    sourceId,
                    index,
                    types,
                    probeKeyRemap);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final Index index;
    private final List<Type> types;
    private final List<Integer> probeKeyRemap;

    @GuardedBy("this")
    private Operator source;

    public IndexSourceOperator(
            OperatorContext operatorContext,
            PlanNodeId planNodeId,
            Index index,
            List<Type> types,
            List<Integer> probeKeyRemap)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.planNodeId = checkNotNull(planNodeId, "planNodeId is null");
        this.index = checkNotNull(index, "index is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.probeKeyRemap = ImmutableList.copyOf(checkNotNull(probeKeyRemap, "probeKeyRemap is null"));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public synchronized void addSplit(Split split)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof IndexSplit, "Split must be instance of IndexSplit");
        checkState(getSource() == null, "Index source split already set");

        IndexSplit indexSplit = (IndexSplit) split;

        // Remap the record set into the format the index is expecting
        RecordSet recordSet = new MappedRecordSet(indexSplit.getKeyRecordSet(), probeKeyRemap);
        RecordSet result = index.lookup(recordSet);
        source = new RecordProjectOperator(operatorContext, result);

        operatorContext.setInfoSupplier(Suppliers.ofInstance(split.getInfo()));
    }

    @Override
    public synchronized void noMoreSplits()
    {
        if (source == null) {
            source = new FinishedOperator(operatorContext, types);
        }
    }

    private synchronized Operator getSource()
    {
        return source;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        Operator delegate = getSource();
        if (delegate == null) {
            return;
        }
        delegate.finish();
    }

    @Override
    public boolean isFinished()
    {
        Operator delegate = getSource();
        return delegate != null && delegate.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        Operator delegate = getSource();
        if (delegate == null) {
            return null;
        }
        return delegate.getOutput();
    }
}
