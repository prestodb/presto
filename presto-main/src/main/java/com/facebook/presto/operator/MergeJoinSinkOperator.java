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

import com.facebook.presto.common.Page;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkState;

public class MergeJoinSinkOperator
        implements Operator
{
    public static class MergeJoinSinkOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final MergeJoinSourceManager mergeJoinSourceManager;
        private boolean closed;

        public MergeJoinSinkOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                MergeJoinSourceManager mergeJoinSourceManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.mergeJoinSourceManager = mergeJoinSourceManager;
        }

        @Override
        public MergeJoinSinkOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, MergeJoinSinkOperator.class.getSimpleName());
            LocalMemoryContext memoryContext = operatorContext.aggregateUserMemoryContext().newLocalMemoryContext(MergeJoinSource.class.getSimpleName());
            MergeJoinSource mergeJoinSource = mergeJoinSourceManager.getMergeJoinSource(driverContext.getLifespan(), memoryContext);
            return new MergeJoinSinkOperator(operatorContext, mergeJoinSource);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Callback sink can not be duplicated");
        }
    }

    @VisibleForTesting
    public enum State
    {
        CONSUMING_INPUT,
        WAITING_FOR_CONSUMER,
        CLOSED
    }

    private final OperatorContext operatorContext;
    final MergeJoinSource mergeJoinSource;
    private State state = State.CONSUMING_INPUT;

    public MergeJoinSinkOperator(
            OperatorContext operatorContext,
            MergeJoinSource mergeJoinSource)
    {
        this.operatorContext = operatorContext;
        this.mergeJoinSource = mergeJoinSource;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @VisibleForTesting
    public State getState()
    {
        return state;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (state == State.WAITING_FOR_CONSUMER) {
            boolean canProduce = mergeJoinSource.getProducerFuture().isDone();
            if (!canProduce) {
                return mergeJoinSource.getProducerFuture();
            }
            state = State.CONSUMING_INPUT;
        }
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.CONSUMING_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        if (page == null || isFinished()) {
            return;
        }
        checkState(state == State.CONSUMING_INPUT);

        mergeJoinSource.addPage(page);
        state = State.WAITING_FOR_CONSUMER;

        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        if (state == State.CONSUMING_INPUT) {
            mergeJoinSource.addPage(null);
            state = State.CLOSED;
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.CLOSED;
    }

    @Override
    public void close()
    {
    }
}
