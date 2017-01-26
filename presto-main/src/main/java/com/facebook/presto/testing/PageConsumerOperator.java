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
package com.facebook.presto.testing;

import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PageConsumerOperator
        implements Operator
{
    public static class PageConsumerOutputFactory
            implements OutputFactory
    {
        private final Function<List<Type>, Consumer<Page>> pageConsumerFactory;

        public PageConsumerOutputFactory(Function<List<Type>, Consumer<Page>> pageConsumerFactory)
        {
            this.pageConsumerFactory = requireNonNull(pageConsumerFactory, "pageConsumerFactory is null");
        }

        @Override
        public OperatorFactory createOutputOperator(int operatorId, PlanNodeId planNodeId, List<Type> types, Function<Page, Page> pagePreprocessor, PagesSerdeFactory serdeFactory)
        {
            return new PageConsumerOperatorFactory(operatorId, planNodeId, pageConsumerFactory.apply(types), pagePreprocessor);
        }
    }

    public static class PageConsumerOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Consumer<Page> pageConsumer;
        private final Function<Page, Page> pagePreprocessor;
        private boolean closed;

        public PageConsumerOperatorFactory(int operatorId, PlanNodeId planNodeId, Consumer<Page> pageConsumer, Function<Page, Page> pagePreprocessor)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pageConsumer = requireNonNull(pageConsumer, "pageConsumer is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PageConsumerOperator.class.getSimpleName());
            return new PageConsumerOperator(operatorContext, pageConsumer, pagePreprocessor);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PageConsumerOperatorFactory(operatorId, planNodeId, pageConsumer, pagePreprocessor);
        }
    }

    private final OperatorContext operatorContext;
    private final Consumer<Page> pageConsumer;
    private final Function<Page, Page> pagePreprocessor;
    private boolean finished;
    private boolean closed;

    public PageConsumerOperator(OperatorContext operatorContext, Consumer<Page> pageConsumer, Function<Page, Page> pagePreprocessor)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pageConsumer = requireNonNull(pageConsumer, "pageConsumer is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
    }

    public boolean isClosed()
    {
        return closed;
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
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finished, "operator finished");

        page = pagePreprocessor.apply(page);
        pageConsumer.accept(page);
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
        closed = true;
    }
}
