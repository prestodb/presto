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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class AggregationOperator
        implements Operator
{
    private final boolean partial;

    public static class AggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private boolean closed;

        public AggregationOperatorFactory(int operatorId, PlanNodeId planNodeId, Step step, List<AccumulatorFactory> accumulatorFactories)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, AggregationOperator.class.getSimpleName());
            return new AggregationOperator(operatorContext, step, accumulatorFactories);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new AggregationOperatorFactory(operatorId, planNodeId, step, accumulatorFactories);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final LocalMemoryContext userMemoryContext;
    private final List<Aggregator> aggregates;

    private State state = State.NEEDS_INPUT;

    public AggregationOperator(OperatorContext operatorContext, Step step, List<AccumulatorFactory> accumulatorFactories)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext();
        this.userMemoryContext = operatorContext.localUserMemoryContext();

        requireNonNull(step, "step is null");
        this.partial = step.isOutputPartial();

        // wrapper each function with an aggregator
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        for (AccumulatorFactory accumulatorFactory : accumulatorFactories) {
            builder.add(new Aggregator(accumulatorFactory, step));
        }
        aggregates = builder.build();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");

        long memorySize = 0;
        for (Aggregator aggregate : aggregates) {
            aggregate.processPage(page);
            memorySize += aggregate.getEstimatedSize();
        }
        if (partial) {
            systemMemoryContext.setBytes(memorySize);
        }
        else {
            userMemoryContext.setBytes(memorySize);
        }
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        // project results into output blocks
        List<Type> types = aggregates.stream().map(Aggregator::getType).collect(toImmutableList());

        PageBuilder pageBuilder = new PageBuilder(types);

        pageBuilder.declarePosition();
        for (int i = 0; i < aggregates.size(); i++) {
            Aggregator aggregator = aggregates.get(i);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            aggregator.evaluate(blockBuilder);
        }

        state = State.FINISHED;
        return pageBuilder.build();
    }

    private static class Aggregator
    {
        private final Accumulator aggregation;
        private final Step step;
        private final int intermediateChannel;

        private Aggregator(AccumulatorFactory accumulatorFactory, Step step)
        {
            if (step.isInputRaw()) {
                intermediateChannel = -1;
                aggregation = accumulatorFactory.createAccumulator();
            }
            else {
                checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
                intermediateChannel = accumulatorFactory.getInputChannels().get(0);
                aggregation = accumulatorFactory.createIntermediateAccumulator();
            }
            this.step = step;
        }

        public Type getType()
        {
            if (step.isOutputPartial()) {
                return aggregation.getIntermediateType();
            }
            else {
                return aggregation.getFinalType();
            }
        }

        public void processPage(Page page)
        {
            if (step.isInputRaw()) {
                aggregation.addInput(page);
            }
            else {
                aggregation.addIntermediate(page.getBlock(intermediateChannel));
            }
        }

        public void evaluate(BlockBuilder blockBuilder)
        {
            if (step.isOutputPartial()) {
                aggregation.evaluateIntermediate(blockBuilder);
            }
            else {
                aggregation.evaluateFinal(blockBuilder);
            }
        }

        public long getEstimatedSize()
        {
            return aggregation.getEstimatedSize();
        }
    }
}
