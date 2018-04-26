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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.sql.planner.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class NestedLoopBuildOperator
        implements Operator
{
    public static class NestedLoopBuildOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier;

        private boolean closed;

        public NestedLoopBuildOperatorFactory(int operatorId, PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            nestedLoopJoinPagesSupplier = new NestedLoopJoinPagesSupplier();
            nestedLoopJoinPagesSupplier.retain();
        }

        public NestedLoopJoinPagesSupplier getNestedLoopJoinPagesSupplier()
        {
            return nestedLoopJoinPagesSupplier;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NestedLoopBuildOperator.class.getSimpleName());
            return new NestedLoopBuildOperator(operatorContext, nestedLoopJoinPagesSupplier);
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }
            closed = true;
            nestedLoopJoinPagesSupplier.release();
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new NestedLoopBuildOperatorFactory(operatorId, planNodeId);
        }
    }

    private final OperatorContext operatorContext;
    private final NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier;
    private final NestedLoopJoinPagesBuilder nestedLoopJoinPagesBuilder;
    private final LocalMemoryContext localUserMemoryContext;
    private boolean finished;

    public NestedLoopBuildOperator(OperatorContext operatorContext, NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.nestedLoopJoinPagesSupplier = requireNonNull(nestedLoopJoinPagesSupplier, "nestedLoopJoinPagesSupplier is null");
        this.nestedLoopJoinPagesBuilder = new NestedLoopJoinPagesBuilder(operatorContext);
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        // The NestedLoopJoinPages will take over our memory reservation, so after this point ours will be zero.
        nestedLoopJoinPagesSupplier.setPages(nestedLoopJoinPagesBuilder.build());

        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        if (page.getPositionCount() == 0) {
            return;
        }

        nestedLoopJoinPagesBuilder.addPage(page);
        if (!localUserMemoryContext.trySetBytes(nestedLoopJoinPagesBuilder.getEstimatedSize().toBytes())) {
            nestedLoopJoinPagesBuilder.compact();
            localUserMemoryContext.setBytes(nestedLoopJoinPagesBuilder.getEstimatedSize().toBytes());
        }
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
