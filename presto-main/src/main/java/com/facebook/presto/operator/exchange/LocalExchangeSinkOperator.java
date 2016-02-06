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
import com.facebook.presto.operator.LocalPlannerAware;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LocalExchangeSinkOperator
        implements Operator
{
    public static class LocalExchangeSinkOperatorFactory
            implements OperatorFactory, LocalPlannerAware
    {
        private final int operatorId;
        private final LocalExchangeSinkFactory sinkFactory;
        private final PlanNodeId planNodeId;
        private final Function<Page, Page> pagePreprocessor;
        private boolean closed;

        public LocalExchangeSinkOperatorFactory(int operatorId, PlanNodeId planNodeId, LocalExchangeSinkFactory sinkFactory, Function<Page, Page> pagePreprocessor)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sinkFactory = requireNonNull(sinkFactory, "sinkFactory is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return sinkFactory.getTypes();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalExchangeSinkOperator.class.getSimpleName());
            return new LocalExchangeSinkOperator(operatorContext, sinkFactory.createSink(), pagePreprocessor);
        }

        @Override
        public void close()
        {
            if (!closed) {
                closed = true;
                sinkFactory.close();
            }
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LocalExchangeSinkOperatorFactory(operatorId, planNodeId, sinkFactory.duplicate(), pagePreprocessor);
        }

        @Override
        public void localPlannerComplete()
        {
            sinkFactory.noMoreSinkFactories();
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
    public List<Type> getTypes()
    {
        return sink.getTypes();
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
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
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
