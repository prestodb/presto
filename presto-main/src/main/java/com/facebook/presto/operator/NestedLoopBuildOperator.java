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
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class NestedLoopBuildOperator
        implements Operator
{
    public static class NestedLoopBuildOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeDataManager<NestedLoopJoinPagesBridge> nestedLoopJoinPagesBridgeManager;

        private boolean closed;

        public NestedLoopBuildOperatorFactory(int operatorId, PlanNodeId planNodeId, JoinBridgeDataManager<NestedLoopJoinPagesBridge> nestedLoopJoinPagesBridgeManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.nestedLoopJoinPagesBridgeManager = requireNonNull(nestedLoopJoinPagesBridgeManager, "nestedLoopJoinPagesBridgeManager is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NestedLoopBuildOperator.class.getSimpleName());
            return new NestedLoopBuildOperator(operatorContext, nestedLoopJoinPagesBridgeManager.forLifespan(driverContext.getLifespan()));
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new NestedLoopBuildOperatorFactory(operatorId, planNodeId, nestedLoopJoinPagesBridgeManager);
        }
    }

    private final OperatorContext operatorContext;
    private final NestedLoopJoinPagesBridge nestedLoopJoinPagesBridge;
    private final NestedLoopJoinPagesBuilder nestedLoopJoinPagesBuilder;
    private final LocalMemoryContext localUserMemoryContext;

    // When the pages are no longer needed, the isFinished method on this operator will return true.
    private final ListenableFuture<?> joinPagesDestroyed;

    private boolean joinPagesBuilt;

    public NestedLoopBuildOperator(OperatorContext operatorContext, NestedLoopJoinPagesBridge nestedLoopJoinPagesBridge)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.nestedLoopJoinPagesBridge = requireNonNull(nestedLoopJoinPagesBridge, "nestedLoopJoinPagesBridge is null");
        this.nestedLoopJoinPagesBuilder = new NestedLoopJoinPagesBuilder(operatorContext);
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        joinPagesDestroyed = nestedLoopJoinPagesBridge.isDestroyed();
        joinPagesDestroyed.addListener(operatorContext::notifyAsync, directExecutor());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (joinPagesBuilt) {
            return;
        }

        // nestedLoopJoinPagesBuilder and the built NestedLoopJoinPages will mostly share the same objects.
        // Extra allocation is minimal during build call. As a result, memory accounting is not updated here.
        nestedLoopJoinPagesBridge.setPages(nestedLoopJoinPagesBuilder.build());
        joinPagesBuilt = true;
    }

    @Override
    public boolean isFinished()
    {
        return joinPagesDestroyed.isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return joinPagesBuilt ? joinPagesDestroyed : NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !joinPagesBuilt && !isFinished();
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
