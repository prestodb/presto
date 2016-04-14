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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LocalExchangeSourceOperator
        implements Operator
{
    public static class LocalExchangeSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final LocalExchange inMemoryExchange;
        private int bufferIndex;
        private boolean closed;

        public LocalExchangeSourceOperatorFactory(int operatorId, PlanNodeId planNodeId, LocalExchange inMemoryExchange)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.inMemoryExchange = requireNonNull(inMemoryExchange, "inMemoryExchange is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return inMemoryExchange.getTypes();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            checkState(bufferIndex < inMemoryExchange.getBufferCount(), "All operators already created");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalExchangeSourceOperator.class.getSimpleName());
            Operator operator = new LocalExchangeSourceOperator(operatorContext, inMemoryExchange.getSource(bufferIndex));
            bufferIndex++;
            return operator;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories can not be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final LocalExchangeSource source;

    public LocalExchangeSourceOperator(OperatorContext operatorContext, LocalExchangeSource source)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.source = requireNonNull(source, "source is null");
        operatorContext.setInfoSupplier(source::getBufferInfo);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return source.getTypes();
    }

    @Override
    public void finish()
    {
        source.finish();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return source.waitForReading();
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
        Page page = source.removePage();
        if (page != null) {
            operatorContext.recordGeneratedInput(page.getSizeInBytes(), page.getPositionCount());
        }
        return page;
    }

    @Override
    public void close()
    {
        source.close();
    }
}
