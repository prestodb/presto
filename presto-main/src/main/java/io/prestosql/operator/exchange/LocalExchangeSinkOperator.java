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
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.LocalPlannerAware;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeSinkFactoryId;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LocalExchangeSinkOperator
        implements Operator
{
    public static class LocalExchangeSinkOperatorFactory
            implements OperatorFactory, LocalPlannerAware
    {
        private final LocalExchangeFactory localExchangeFactory;

        private final int operatorId;
        // There will be a LocalExchangeSinkFactory per LocalExchangeSinkOperatorFactory per Driver Group.
        // A LocalExchangeSinkOperatorFactory needs to have access to LocalExchangeSinkFactories for each Driver Group.
        private final LocalExchangeSinkFactoryId sinkFactoryId;
        private final PlanNodeId planNodeId;
        private final Function<Page, Page> pagePreprocessor;
        private boolean closed;

        public LocalExchangeSinkOperatorFactory(LocalExchangeFactory localExchangeFactory, int operatorId, PlanNodeId planNodeId, LocalExchangeSinkFactoryId sinkFactoryId, Function<Page, Page> pagePreprocessor)
        {
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "localExchangeFactory is null");

            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sinkFactoryId = requireNonNull(sinkFactoryId, "sinkFactoryId is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalExchangeSinkOperator.class.getSimpleName());

            LocalExchangeSinkFactory localExchangeSinkFactory = localExchangeFactory.getLocalExchange(driverContext.getLifespan()).getSinkFactory(sinkFactoryId);

            return new LocalExchangeSinkOperator(operatorContext, localExchangeSinkFactory.createSink(), pagePreprocessor);
        }

        @Override
        public void noMoreOperators()
        {
            if (!closed) {
                closed = true;
                localExchangeFactory.closeSinks(sinkFactoryId);
            }
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            localExchangeFactory.getLocalExchange(lifespan).getSinkFactory(sinkFactoryId).close();
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LocalExchangeSinkOperatorFactory(localExchangeFactory, operatorId, planNodeId, localExchangeFactory.newSinkFactoryId(), pagePreprocessor);
        }

        @Override
        public void localPlannerComplete()
        {
            localExchangeFactory.noMoreSinkFactories();
        }
    }

    private final OperatorContext operatorContext;
    private final LocalExchangeSink sink;
    private final Function<Page, Page> pagePreprocessor;

    LocalExchangeSinkOperator(OperatorContext operatorContext, LocalExchangeSink sink, Function<Page, Page> pagePreprocessor)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sink = requireNonNull(sink, "sink is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        sink.finish();
    }

    @Override
    public boolean isFinished()
    {
        return sink.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return sink.waitForWriting();
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
        page = pagePreprocessor.apply(page);
        sink.addPage(page);
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
    {
        finish();
    }
}
