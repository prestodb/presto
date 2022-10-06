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

import com.facebook.presto.common.Page;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.plan.PlanNodeId;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRemoteSourceOperator
        implements SourceOperator
{
    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final PrestoSparkPageInput pageInput;
    private final boolean isFirstOperator;

    private boolean finished;

    public PrestoSparkRemoteSourceOperator(PlanNodeId sourceId, OperatorContext operatorContext, PrestoSparkPageInput pageInput, boolean isFirstOperator)
    {
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.localSystemMemoryContext();
        this.pageInput = requireNonNull(pageInput, "pageInput is null");
        this.isFirstOperator = isFirstOperator;
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

        Page page = pageInput.getNextPage(() -> {
            updateMemoryContext();
            return true;
        });
        updateMemoryContext();
        if (page == null) {
            finished = true;
            return null;
        }
        return page;
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
    public Supplier<Optional<UpdatablePageSource>> addSplit(ScheduledSplit scheduledSplit)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void noMoreSplits()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        systemMemoryContext.setBytes(0);
    }

    private void updateMemoryContext()
    {
        if (pageInput instanceof PrestoSparkDiskPageInput) {
            long memorySize = 0;
            PrestoSparkDiskPageInput diskPageInput = (PrestoSparkDiskPageInput) pageInput;
            memorySize += diskPageInput.getStagingBroadcastTableSizeInBytes();

            // Since the cache is shared, only the first PrestoSparkRemoteSourceOperator should report the cache memory
            if (isFirstOperator) {
                memorySize += diskPageInput.getRetainedSizeInBytes();
            }

            systemMemoryContext.setBytes(memorySize);
        }
    }

    public static class SparkRemoteSourceOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PrestoSparkPageInput pageInput;
        private boolean isFirstOperator = true;

        private boolean closed;

        public SparkRemoteSourceOperatorFactory(int operatorId, PlanNodeId planNodeId, PrestoSparkPageInput pageInput)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pageInput = requireNonNull(pageInput, "pageInput is null");
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PrestoSparkRemoteSourceOperator.class.getSimpleName());
            SourceOperator operator = new PrestoSparkRemoteSourceOperator(planNodeId, operatorContext, pageInput, isFirstOperator);
            isFirstOperator = false;
            return operator;
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }
}
