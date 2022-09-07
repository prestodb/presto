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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNOperator
        implements Operator
{
    public static Logger logger = Logger.get(TopNOperator.class);
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
        private final SpillerFactory spillerFactory;
        private final boolean spillEnabled;

        public TopNOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                int n,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                SpillerFactory spillerFactory,
                boolean spillEnabled)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.n = n;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
            this.spillerFactory = spillerFactory;
            this.spillEnabled = spillEnabled;
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
                    sortOrders,
                    spillerFactory,
                    spillEnabled);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNOperatorFactory(operatorId, planNodeId, sourceTypes, n, sortChannels, sortOrders, spillerFactory, spillEnabled);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;

    private GroupedTopNBuilder topNBuilder;
    private boolean finishing;

    private Iterator<Page> outputIterator;

    public TopNOperator(
            OperatorContext operatorContext,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            SpillerFactory spillerFactory,
            boolean spillEnabled)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        checkArgument(n >= 0, "n must be positive");
        if (n == 0) {
            finishing = true;
            outputIterator = emptyIterator();
        }
        else {
            if (spillEnabled) {
                logger.info("TopNOperator::: SpillEnabled id=%s", operatorContext.getOperatorId());
                topNBuilder = new SpillableGroupedTopNBuilder(
                        types,
                        emptyList(),
                        emptyList(),
                        null,
                        0,
                        false,
                        null,
                        new SimplePageWithPositionComparator(types, sortChannels, sortOrders),
                        n,
                        false,
                        operatorContext,
                        spillerFactory);
            }
            else {
                logger.info("TopNOperator::: NOT SpillEnabled");
                topNBuilder = new InMemoryGroupedTopNBuilder(
                        operatorContext,
                        types,
                        emptyList(),
                        emptyList(),
                        null,
                        0,
                        false,
                        null,
                        new SimplePageWithPositionComparator(types, sortChannels, sortOrders),
                        n,
                        false,
                        Optional.empty(),
                        true);
            }
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
        logger.info(" [%s] ===================== Consuming Page ==================== ", Thread.currentThread().getName());
        checkState(!finishing, "Operator is already finishing");
        boolean done = topNBuilder.processPage(requireNonNull(page, "page is null")).process();
        // there is no grouping so work will always be done
        verify(done);
        logger.info(" [%s] ===================== Consuming Page (Done) ==================== ", Thread.currentThread().getName());
    }

    @Override
    public Page getOutput()
    {
        logger.info(" [%s] <<<<<<<<<<<<<<<<<<<<< Trying for Output Page ==================== ",Thread.currentThread().getName());

        if (!finishing || noMoreOutput()) {
            return null;
        }

        if (outputIterator == null) {
            // start flushing
            outputIterator = topNBuilder.buildResult();
        }

        Page output = null;
        if (outputIterator.hasNext()) {
            output = outputIterator.next();
            logger.info(" [%s] <<<<<<<<<<<<<<<<<<<<< Got Output Page  %s (Done) ==================== ", Thread.currentThread().getName(), output);
        }
        else {
            outputIterator = emptyIterator();
        }
        return output;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        return topNBuilder.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke()
    {
        if (topNBuilder != null) {
            topNBuilder.finishMemoryRevoke();
        }
    }

    private boolean noMoreOutput()
    {
        return outputIterator != null && !outputIterator.hasNext();
    }
}
