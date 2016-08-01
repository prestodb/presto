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

import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.builder.HashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.SpillableHashAggregationBuilder;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.spiller.SpillerFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder.toTypes;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class HashAggregationOperator
        implements Operator
{
    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final Optional<Integer> hashChannel;
        private final int expectedGroups;
        private final List<Type> types;
        private final long maxPartialMemory;
        private final long maxEntriesBeforeSpill;
        private final DataSize memoryLimitBeforeSpill;
        private final Optional<SpillerFactory> spillerFactory;

        private boolean closed;

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                int expectedGroups,
                DataSize maxPartialMemory)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    maxPartialMemory,
                    0,
                    new DataSize(0, MEGABYTE),
                    Optional.empty());
        }

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                int expectedGroups,
                DataSize maxPartialMemory,
                long maxEntriesBeforeSpill,
                DataSize memoryLimitBeforeSpill,
                Optional<SpillerFactory> spillerFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.expectedGroups = expectedGroups;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null").toBytes();
            this.maxEntriesBeforeSpill = maxEntriesBeforeSpill;
            this.memoryLimitBeforeSpill = requireNonNull(memoryLimitBeforeSpill, "memoryLimitBeforeSpill is null");
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");

            this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
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

            OperatorContext operatorContext;
            if (step.isOutputPartial()) {
                operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName(), maxPartialMemory);
            }
            else {
                operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName());
            }
            HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
                    operatorContext,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    maxEntriesBeforeSpill,
                    memoryLimitBeforeSpill,
                    spillerFactory);
            return hashAggregationOperator;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashAggregationOperatorFactory(
                    operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    new DataSize(maxPartialMemory, Unit.BYTE),
                    maxEntriesBeforeSpill,
                    memoryLimitBeforeSpill,
                    spillerFactory);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Step step;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;
    private final int expectedGroups;
    private final Optional<SpillerFactory> spillerFactory;
    private final long maxEntriesBeforeSpill;
    private final DataSize memoryLimitBeforeSpill;
    private final Closer closer = Closer.create();

    private final List<Type> types;

    private HashAggregationBuilder hashAggregationBuilder;
    private Iterator<Page> outputIterator;
    private boolean finishing;

    public HashAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Step step,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> hashChannel,
            int expectedGroups,
            long maxEntriesBeforeSpill,
            DataSize memoryLimitBeforeSpill,
            Optional<SpillerFactory> spillerFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(step, "step is null");
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");

        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.maxEntriesBeforeSpill = maxEntriesBeforeSpill;
        this.memoryLimitBeforeSpill = requireNonNull(memoryLimitBeforeSpill, "memoryLimitBeforeSpill is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && hashAggregationBuilder == null && (outputIterator == null || !outputIterator.hasNext());
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && outputIterator == null && (hashAggregationBuilder == null || !hashAggregationBuilder.checkFullAndUpdateMemory());
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        if (hashAggregationBuilder == null) {
            if (step.isOutputPartial() || (maxEntriesBeforeSpill == 0 && memoryLimitBeforeSpill.toBytes() == 0)) {
                hashAggregationBuilder = new InMemoryHashAggregationBuilder(
                        accumulatorFactories,
                        step,
                        expectedGroups,
                        groupByTypes,
                        groupByChannels,
                        hashChannel,
                        operatorContext);
            }
            else {
                hashAggregationBuilder = new SpillableHashAggregationBuilder(
                        accumulatorFactories,
                        step,
                        expectedGroups,
                        groupByTypes,
                        groupByChannels,
                        hashChannel,
                        operatorContext,
                        maxEntriesBeforeSpill,
                        memoryLimitBeforeSpill,
                        spillerFactory);
            }

            closer.register(hashAggregationBuilder);
            // assume initial aggregationBuilder is not full
        }
        else {
            checkState(!hashAggregationBuilder.checkFullAndUpdateMemory(), "Aggregation buffer is full");
        }
        hashAggregationBuilder.processPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (outputIterator == null || !outputIterator.hasNext()) {
            // current output iterator is done
            outputIterator = null;

            // no data
            if (hashAggregationBuilder == null) {
                return null;
            }

            if (!finishing && (hashAggregationBuilder.isBusy() || !hashAggregationBuilder.checkFullAndUpdateMemory())) {
                return null;
            }

            outputIterator = hashAggregationBuilder.buildResult();
            hashAggregationBuilder = null;

            if (!outputIterator.hasNext()) {
                // current output iterator is done
                outputIterator = null;
                return null;
            }
        }

        return outputIterator.next();
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
