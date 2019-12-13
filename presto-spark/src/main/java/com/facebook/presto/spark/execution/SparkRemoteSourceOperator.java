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
package com.facebook.presto.spark.execution;

import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.plan.PlanNodeId;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SparkRemoteSourceOperator
        implements SourceOperator
{
    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final Iterator<SerializedPage> iterator;
    private final PagesSerde serde;

    private boolean finished;

    public SparkRemoteSourceOperator(PlanNodeId sourceId, OperatorContext operatorContext, Iterator<SerializedPage> iterator, PagesSerde serde)
    {
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.iterator = requireNonNull(iterator, "iterator is null");
        this.serde = requireNonNull(serde, "serde is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }

        SerializedPage serializedPage = null;
        synchronized (iterator) {
            if (iterator.hasNext()) {
                serializedPage = iterator.next();
            }
        }
        if (serializedPage == null) {
            finished = true;
            return null;
        }

        return serde.deserialize(serializedPage);
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void noMoreSplits()
    {
        throw new UnsupportedOperationException();
    }

    public static class SparkRemoteSourceOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Iterator<SerializedPage> iterator;
        private final PagesSerde serde;

        private boolean closed;

        public SparkRemoteSourceOperatorFactory(int operatorId, PlanNodeId planNodeId, Iterator<SerializedPage> iterator, PagesSerde serde)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.iterator = requireNonNull(iterator, "iterator is null");
            this.serde = requireNonNull(serde, "serde is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "operator factory is closed");
            return new SparkRemoteSourceOperator(
                    planNodeId,
                    driverContext.addOperatorContext(operatorId, planNodeId, SparkRemoteSourceOperator.class.getSimpleName()),
                    iterator,
                    serde);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
