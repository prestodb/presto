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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ParallelHashBuildOperator
        implements Operator
{
    public static class ParallelHashBuildOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PartitionedLookupSourceSupplier lookupSourceSupplier;
        private final List<Integer> hashChannels;
        private final Optional<Integer> preComputedHashChannel;
        private final Optional<JoinFilterFunction> filterFunction;

        private final int expectedPositions;

        private int partitionIndex;
        private boolean closed;

        public ParallelHashBuildOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                Map<Symbol, Integer> layout,
                List<Integer> hashChannels,
                Optional<Integer> preComputedHashChannel,
                boolean outer,
                Optional<JoinFilterFunction> filterFunction,
                int expectedPositions,
                int partitionCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

            checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");
            lookupSourceSupplier = new PartitionedLookupSourceSupplier(
                    types,
                    hashChannels,
                    partitionCount,
                    requireNonNull(layout, "layout is null"),
                    outer);

            checkArgument(!hashChannels.isEmpty(), "hashChannels is empty");
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunction = requireNonNull(filterFunction, "filterFunction is null");

            this.expectedPositions = expectedPositions;
        }

        public LookupSourceSupplier getLookupSourceSupplier()
        {
            return lookupSourceSupplier;
        }

        @Override
        public List<Type> getTypes()
        {
            return lookupSourceSupplier.getTypes();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ParallelHashBuildOperator.class.getSimpleName());
            ParallelHashBuildOperator operator = new ParallelHashBuildOperator(
                    operatorContext,
                    lookupSourceSupplier,
                    partitionIndex,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunction,
                    expectedPositions);

            partitionIndex++;
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
            throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final PartitionedLookupSourceSupplier lookupSourceSupplier;
    private final int partitionIndex;

    private final List<Integer> hashChannels;
    private final Optional<Integer> preComputedHashChannel;
    private final Optional<JoinFilterFunction> filterFunction;

    private final PagesIndex index;

    private boolean finished;

    public ParallelHashBuildOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceSupplier lookupSourceSupplier,
            int partitionIndex,
            List<Integer> hashChannels,
            Optional<Integer> preComputedHashChannel,
            Optional<JoinFilterFunction> filterFunction,
            int expectedPositions)
    {
        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunction = filterFunction;

        this.index = new PagesIndex(lookupSourceSupplier.getTypes(), expectedPositions);
        this.lookupSourceSupplier = lookupSourceSupplier;

        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return lookupSourceSupplier.getTypes();
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }
        finished = true;

        // After this point the SharedLookupSource will take over our memory reservation, and ours will be zero
        LookupSource lookupSource = index.createLookupSource(hashChannels, preComputedHashChannel, filterFunction);
        lookupSourceSupplier.setLookupSource(partitionIndex, lookupSource, operatorContext);
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

        index.addPage(page);

        operatorContext.setMemoryReservation(page.getPositionCount());
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
