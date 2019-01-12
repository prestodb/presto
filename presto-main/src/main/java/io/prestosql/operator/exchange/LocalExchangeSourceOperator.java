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
package io.prestosql.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;

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
        private final LocalExchangeFactory localExchangeFactory;
        private boolean closed;

        public LocalExchangeSourceOperatorFactory(int operatorId, PlanNodeId planNodeId, LocalExchangeFactory localExchangeFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "localExchangeFactory is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            LocalExchange inMemoryExchange = localExchangeFactory.getLocalExchange(driverContext.getLifespan());

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalExchangeSourceOperator.class.getSimpleName());
            return new LocalExchangeSourceOperator(operatorContext, inMemoryExchange.getNextSource());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories can not be duplicated");
        }

        public LocalExchangeFactory getLocalExchangeFactory()
        {
            return localExchangeFactory;
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
            operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        }
        return page;
    }

    @Override
    public void close()
    {
        source.close();
    }
}
