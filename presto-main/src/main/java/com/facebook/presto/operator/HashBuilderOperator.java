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
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private enum State {
            NOT_CREATED, CREATED, CLOSED
        }

        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final SettableLookupSourceFactory lookupSourceFactory;
        private final List<Integer> hashChannels;
        private final Optional<Integer> hashChannel;
        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;

        private final int expectedPositions;
        private State state = State.NOT_CREATED;

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                Map<Symbol, Integer> layout,
                List<Integer> hashChannels,
                Optional<Integer> hashChannel,
                boolean outer,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.lookupSourceFactory = new SettableLookupSourceFactory(
                    requireNonNull(types, "types is null"),
                    requireNonNull(layout, "layout is null"),
                    outer);

            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");

            this.expectedPositions = expectedPositions;
        }

        public LookupSourceFactory getLookupSourceFactory()
        {
            return lookupSourceFactory;
        }

        @Override
        public List<Type> getTypes()
        {
            return lookupSourceFactory.getTypes();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(state == State.NOT_CREATED, "Only one hash build operator can be created");
            state = State.CREATED;

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());
            return new HashBuilderOperator(
                    operatorContext,
                    lookupSourceFactory,
                    hashChannels,
                    hashChannel,
                    filterFunctionFactory,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            state = State.CLOSED;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Hash build can not be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final SettableLookupSourceFactory lookupSourceFactory;
    private final List<Integer> hashChannels;
    private final Optional<Integer> hashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;

    private final PagesIndex pagesIndex;

    private boolean finished;

    public HashBuilderOperator(
            OperatorContext operatorContext,
            SettableLookupSourceFactory lookupSourceFactory,
            List<Integer> hashChannels,
            Optional<Integer> hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            int expectedPositions)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");

        this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");

        this.pagesIndex = new PagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return lookupSourceFactory.getTypes();
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        // After this point the LookupSource will take over our memory reservation, and ours will be zero
        Supplier<LookupSource> supplier = pagesIndex.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, hashChannel, filterFunctionFactory);
        lookupSourceFactory.setLookupSourceSupplier(supplier, operatorContext);
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
        checkState(!isFinished(), "Operator is already finished");

        pagesIndex.addPage(page);
        if (!operatorContext.trySetMemoryReservation(pagesIndex.getEstimatedSize().toBytes())) {
            pagesIndex.compact();
        }
        operatorContext.setMemoryReservation(pagesIndex.getEstimatedSize().toBytes());
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
