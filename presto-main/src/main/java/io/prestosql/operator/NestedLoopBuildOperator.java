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
package io.prestosql.operator;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.Optional;
import java.util.concurrent.Future;

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
        private final JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager;

        private boolean closed;

        public NestedLoopBuildOperatorFactory(int operatorId, PlanNodeId planNodeId, JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.nestedLoopJoinBridgeManager = requireNonNull(nestedLoopJoinBridgeManager, "nestedLoopJoinBridgeManager is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NestedLoopBuildOperator.class.getSimpleName());
            return new NestedLoopBuildOperator(operatorContext, nestedLoopJoinBridgeManager.getJoinBridge(driverContext.getLifespan()));
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
            return new NestedLoopBuildOperatorFactory(operatorId, planNodeId, nestedLoopJoinBridgeManager);
        }
    }

    private final OperatorContext operatorContext;
    private final NestedLoopJoinBridge nestedLoopJoinBridge;
    private final NestedLoopJoinPagesBuilder nestedLoopJoinPagesBuilder;
    private final LocalMemoryContext localUserMemoryContext;

    // Initially, probeDoneWithPages is not present.
    // Once finish is called, probeDoneWithPages will be set to a future that completes when the pages are no longer needed by the probe side.
    // When the pages are no longer needed, the isFinished method on this operator will return true.
    private Optional<ListenableFuture<?>> probeDoneWithPages = Optional.empty();

    public NestedLoopBuildOperator(OperatorContext operatorContext, NestedLoopJoinBridge nestedLoopJoinBridge)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.nestedLoopJoinBridge = requireNonNull(nestedLoopJoinBridge, "nestedLoopJoinBridge is null");
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
        if (probeDoneWithPages.isPresent()) {
            return;
        }

        // nestedLoopJoinPagesBuilder and the built NestedLoopJoinPages will mostly share the same objects.
        // Extra allocation is minimal during build call. As a result, memory accounting is not updated here.
        probeDoneWithPages = Optional.of(nestedLoopJoinBridge.setPages(nestedLoopJoinPagesBuilder.build()));
    }

    @Override
    public boolean isFinished()
    {
        return probeDoneWithPages.map(Future::isDone).orElse(false);
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return probeDoneWithPages.orElse(NOT_BLOCKED);
    }

    @Override
    public boolean needsInput()
    {
        return !probeDoneWithPages.isPresent();
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
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
