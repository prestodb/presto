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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class InMemoryExchangeSourceOperator
        implements Operator
{
    public static class InMemoryExchangeSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final InMemoryExchange inMemoryExchange;
        private final boolean broadcast;
        private int bufferIndex;
        private boolean closed;

        public static InMemoryExchangeSourceOperatorFactory createRandomDistribution(int operatorId, InMemoryExchange inMemoryExchange)
        {
            requireNonNull(inMemoryExchange, "inMemoryExchange is null");
            checkArgument(inMemoryExchange.getBufferCount() == 1, "exchange must have only one buffer");
            return new InMemoryExchangeSourceOperatorFactory(operatorId, inMemoryExchange, false);
        }

        public static InMemoryExchangeSourceOperatorFactory createBroadcastDistribution(int operatorId, InMemoryExchange inMemoryExchange)
        {
            requireNonNull(inMemoryExchange, "inMemoryExchange is null");
            checkArgument(inMemoryExchange.getBufferCount() > 1, "exchange must have more than one buffer");
            return new InMemoryExchangeSourceOperatorFactory(operatorId, inMemoryExchange, true);
        }

        private InMemoryExchangeSourceOperatorFactory(int operatorId, InMemoryExchange inMemoryExchange, boolean broadcast)
        {
            this.operatorId = operatorId;
            this.inMemoryExchange = requireNonNull(inMemoryExchange, "inMemoryExchange is null");
            checkArgument(bufferIndex < inMemoryExchange.getBufferCount());
            this.broadcast = broadcast;
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, InMemoryExchangeSourceOperator.class.getSimpleName());
            Operator operator = new InMemoryExchangeSourceOperator(operatorContext, inMemoryExchange, bufferIndex);
            if (broadcast) {
                bufferIndex++;
            }
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
    private final InMemoryExchange exchange;
    private final int bufferIndex;

    public InMemoryExchangeSourceOperator(OperatorContext operatorContext, InMemoryExchange exchange, int bufferIndex)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.exchange = requireNonNull(exchange, "exchange is null");
        checkArgument(bufferIndex < exchange.getBufferCount());
        this.bufferIndex = bufferIndex;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return exchange.getTypes();
    }

    @Override
    public void finish()
    {
        exchange.finish();
    }

    @Override
    public boolean isFinished()
    {
        return exchange.isFinished(bufferIndex);
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = exchange.waitForReading(bufferIndex);
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
        Page page = exchange.removePage(bufferIndex);
        if (page != null) {
            operatorContext.recordGeneratedInput(page.getSizeInBytes(), page.getPositionCount());
        }
        return page;
    }

    @Override
    public void close()
            throws Exception
    {
        finish();
    }
}
