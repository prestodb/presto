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

import com.facebook.presto.execution.SharedBuffer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TaskOutputOperator
        implements Operator
{
    public static class TaskOutputFactory
            implements OutputFactory
    {
        private final SharedBuffer sharedBuffer;

        public TaskOutputFactory(SharedBuffer sharedBuffer)
        {
            this.sharedBuffer = checkNotNull(sharedBuffer, "sharedBuffer is null");
        }

        @Override
        public OperatorFactory createOutputOperator(int operatorId, List<Type> sourceTypes)
        {
            return new TaskOutputOperatorFactory(operatorId, sharedBuffer);
        }
    }

    public static class TaskOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final SharedBuffer sharedBuffer;

        public TaskOutputOperatorFactory(int operatorId, SharedBuffer sharedBuffer)
        {
            this.operatorId = operatorId;
            this.sharedBuffer = checkNotNull(sharedBuffer, "sharedBuffer is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, TaskOutputOperator.class.getSimpleName());
            return new TaskOutputOperator(operatorContext, sharedBuffer);
        }

        @Override
        public void close()
        {
        }
    }

    private final OperatorContext operatorContext;
    private final SharedBuffer sharedBuffer;
    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private boolean finished;

    public TaskOutputOperator(OperatorContext operatorContext, SharedBuffer sharedBuffer)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.sharedBuffer = checkNotNull(sharedBuffer, "sharedBuffer is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }

        return finished && blocked == NOT_BLOCKED;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
        return !finished && blocked == NOT_BLOCKED;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(blocked == NOT_BLOCKED, "output is already blocked");
        ListenableFuture<?> future = sharedBuffer.enqueue(page);
        if (!future.isDone()) {
            this.blocked = future;
        }
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
