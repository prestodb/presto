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

import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProcessorOutput;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.function.Supplier;

import static com.facebook.presto.operator.project.PageProcessorOutput.EMPTY_PAGE_PROCESSOR_OUTPUT;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class FilterAndProjectOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final LocalMemoryContext outputMemoryContext;

    private final ListenableFuture<PageProcessor> processor;
    private PageProcessorOutput currentOutput = EMPTY_PAGE_PROCESSOR_OUTPUT;
    private boolean finishing;

    public FilterAndProjectOperator(OperatorContext operatorContext, Iterable<? extends Type> types, ListenableFuture<PageProcessor> processor)
    {
        this.processor = requireNonNull(processor, "processor is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputMemoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return processor;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public final List<Type> getTypes()
    {
        return types;
    }

    @Override
    public final void finish()
    {
        finishing = true;
    }

    @Override
    public final boolean isFinished()
    {
        boolean finished = finishing && !currentOutput.hasNext();
        if (finished) {
            currentOutput = EMPTY_PAGE_PROCESSOR_OUTPUT;
            outputMemoryContext.setBytes(0);
        }
        return finished;
    }

    @Override
    public final boolean needsInput()
    {
        return !finishing && !currentOutput.hasNext();
    }

    @Override
    public final void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(!currentOutput.hasNext(), "Page buffer is full");
        checkState(processor.isDone(), "Page processor is not ready");

        currentOutput = getFutureValue(processor).process(operatorContext.getSession().toConnectorSession(), page);
        outputMemoryContext.setBytes(currentOutput.getRetainedSizeInBytes());
    }

    @Override
    public final Page getOutput()
    {
        if (!currentOutput.hasNext()) {
            return null;
        }
        return currentOutput.next();
    }

    public static class FilterAndProjectOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Supplier<ListenableFuture<PageProcessor>> processor;
        private final List<Type> types;
        private boolean closed;

        public static FilterAndProjectOperatorFactory synchronousFilterAndProjectOperator(
                int operatorId,
                PlanNodeId planNodeId,
                Supplier<PageProcessor> processor,
                List<Type> types)
        {
            return new FilterAndProjectOperatorFactory(operatorId, planNodeId, () -> immediateFuture(processor.get()), types);
        }

        public static FilterAndProjectOperatorFactory asynchronousFilterAndProjectOperator(
                int operatorId,
                PlanNodeId planNodeId,
                Supplier<ListenableFuture<PageProcessor>> processor,
                List<Type> types)
        {
            return new FilterAndProjectOperatorFactory(operatorId, planNodeId, processor, types);
        }

        private FilterAndProjectOperatorFactory(int operatorId, PlanNodeId planNodeId, Supplier<ListenableFuture<PageProcessor>> processor, List<Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.processor = requireNonNull(processor, "processor is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, FilterAndProjectOperator.class.getSimpleName());
            return new FilterAndProjectOperator(operatorContext, types, processor.get());
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new FilterAndProjectOperatorFactory(operatorId, planNodeId, processor, types);
        }
    }
}
