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

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class InMemoryExchangeSourceOperator
        implements Operator
{
    public static class InMemoryExchangeSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final InMemoryExchange inMemoryExchange;
        private boolean closed;

        public InMemoryExchangeSourceOperatorFactory(int operatorId, InMemoryExchange inMemoryExchange)
        {
            this.operatorId = operatorId;
            this.inMemoryExchange = inMemoryExchange;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return inMemoryExchange.getTupleInfos();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, InMemoryExchangeSourceOperator.class.getSimpleName());
            return new InMemoryExchangeSourceOperator(operatorContext, inMemoryExchange);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final InMemoryExchange exchange;

    public InMemoryExchangeSourceOperator(OperatorContext operatorContext, InMemoryExchange exchange)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.exchange = checkNotNull(exchange, "exchange is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return exchange.getTupleInfos();
    }

    @Override
    public void finish()
    {
        exchange.finish();
    }

    @Override
    public boolean isFinished()
    {
        return exchange.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = exchange.waitForNotEmpty();
        if (blocked.isDone()) {
            return NOT_BLOCKED;
        }
        return blocked;
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
        Page page = exchange.removePage();
        if (page != null) {
            operatorContext.recordGeneratedInput(page.getDataSize(), page.getPositionCount());
        }
        return page;
    }
}
