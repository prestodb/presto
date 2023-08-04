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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNOperator
        implements Operator
{
    public static class TopNOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final int n;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private boolean closed;

        public TopNOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                int n,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.n = n;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNOperator.class.getSimpleName());
            return new TopNOperator(
                    operatorContext,
                    sourceTypes,
                    n,
                    sortChannels,
                    sortOrders);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNOperatorFactory(operatorId, planNodeId, sourceTypes, n, sortChannels, sortOrders);
        }
    }

    private final OperatorContext operatorContext;

    private GroupedTopNBuilder topNBuilder;
    private boolean finishing;

    private WorkProcessor<Page> outputPages;

    public TopNOperator(
            OperatorContext operatorContext,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        checkArgument(n >= 0, "n must be positive");
        if (n == 0) {
            finishing = true;
            // We create an empty WorkProcessor and finish it
            outputPages = WorkProcessor.of();
            outputPages.process();
        }
        else {
            topNBuilder = new InMemoryGroupedTopNBuilder(
                    types,
                    new SimplePageWithPositionComparator(types, sortChannels, sortOrders),
                    n,
                    false,
                    operatorContext.localUserMemoryContext(),
                    new NoChannelGroupByHash());
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && noMoreOutput();
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && !noMoreOutput();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        boolean done = topNBuilder.processPage(requireNonNull(page, "page is null")).process();
        // there is no grouping so work will always be done
        verify(done);
        topNBuilder.updateMemoryReservations();
    }

    @Override
    public Page getOutput()
    {
        if (!finishing || noMoreOutput()) {
            return null;
        }

        if (outputPages == null) {
            // start flushing
            outputPages = topNBuilder.buildResult();
        }

        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            topNBuilder.close();
            return null;
        }

        Page outputPage = outputPages.getResult();
        topNBuilder.updateMemoryReservations();

        return outputPage;
    }

    private boolean noMoreOutput()
    {
        return outputPages != null && outputPages.isFinished();
    }
}
