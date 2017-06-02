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
import com.facebook.presto.operator.exchange.LocalMergeExchange.ExchangeBuffer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LocalMergeSinkOperator
        implements Operator
{
    public static class LocalMergeSinkOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> types;
        private final LocalMergeExchange exchange;
        private final int operatorsCount;
        private final AtomicInteger operatorIndex = new AtomicInteger();
        private boolean closed;

        public LocalMergeSinkOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> types, LocalMergeExchange exchange, int operatorsCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.types = requireNonNull(types, "types is null");
            this.exchange = requireNonNull(exchange, "exchange is null");
            checkArgument(operatorsCount > 0, "operatorsCount must be greater than zero");
            this.operatorsCount = operatorsCount;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "closed");
            checkState(operatorIndex.get() < operatorsCount);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalMergeSourceOperator.class.getSimpleName());
            Operator operator = new LocalMergeSinkOperator(operatorContext, types, exchange.getBuffer(operatorIndex.get()));
            operatorIndex.incrementAndGet();
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
            throw new UnsupportedOperationException();
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final ExchangeBuffer exchangeBuffer;

    public LocalMergeSinkOperator(OperatorContext operatorContext, List<Type> types, ExchangeBuffer exchangeBuffer)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = requireNonNull(types, "types is null");
        this.exchangeBuffer = requireNonNull(exchangeBuffer, "exchangeBuffer is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        exchangeBuffer.finishWrite();
    }

    @Override
    public boolean isFinished()
    {
        return exchangeBuffer.isWriteFinished() || exchangeBuffer.isReadFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return exchangeBuffer.isWriteBlocked();
    }

    @Override
    public boolean needsInput()
    {
        return !isFinished() && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        exchangeBuffer.enqueuePage(page);
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
            throws Exception
    {
        finish();
    }
}
