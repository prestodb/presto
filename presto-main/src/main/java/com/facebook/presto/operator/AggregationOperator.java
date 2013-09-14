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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.FixedWidthAggregationFunction;
import com.facebook.presto.operator.aggregation.VariableWidthAggregationFunction;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class AggregationOperator
        implements Operator
{
    public static class AggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final Step step;
        private final List<AggregationFunctionDefinition> functionDefinitions;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public AggregationOperatorFactory(int operatorId, Step step, List<AggregationFunctionDefinition> functionDefinitions)
        {
            this.operatorId = operatorId;
            this.step = step;
            this.functionDefinitions = functionDefinitions;
            this.tupleInfos = toTupleInfos(step, functionDefinitions);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, AggregationOperator.class.getSimpleName());
            return new AggregationOperator(operatorContext, step, functionDefinitions);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final List<TupleInfo> tupleInfos;
    private final List<Aggregator> aggregates;

    private State state = State.NEEDS_INPUT;

    public AggregationOperator(OperatorContext operatorContext, Step step, List<AggregationFunctionDefinition> functionDefinitions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        checkNotNull(step, "step is null");
        checkNotNull(functionDefinitions, "functionDefinitions is null");

        this.tupleInfos = toTupleInfos(step, functionDefinitions);

        // wrapper each function with an aggregator
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            builder.add(createAggregator(functionDefinition, step));
        }
        aggregates = builder.build();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
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
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
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
        checkNotNull(page, "page is null");

        for (Aggregator aggregate : aggregates) {
            aggregate.addValue(page);
        }
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        // project results into output blocks
        Block[] blocks = new Block[aggregates.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = aggregates.get(i).getResult();
        }
        state = State.FINISHED;
        return new Page(blocks);
    }

    @VisibleForTesting
    public static Aggregator createAggregator(AggregationFunctionDefinition functionDefinition, Step step)
    {
        AggregationFunction function = functionDefinition.getFunction();
        if (function instanceof VariableWidthAggregationFunction) {
            return new VariableWidthAggregator<>((VariableWidthAggregationFunction<Object>) functionDefinition.getFunction(), functionDefinition.getInputs(), step);
        }
        else {
            Input input = null;
            if (!functionDefinition.getInputs().isEmpty()) {
                input = Iterables.getOnlyElement(functionDefinition.getInputs());
            }
            return new FixedWidthAggregator((FixedWidthAggregationFunction) functionDefinition.getFunction(), input, step);
        }
    }

    private static List<TupleInfo> toTupleInfos(Step step, List<AggregationFunctionDefinition> functionDefinitions)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            if (step != Step.PARTIAL) {
                tupleInfos.add(functionDefinition.getFunction().getFinalTupleInfo());
            }
            else {
                tupleInfos.add(functionDefinition.getFunction().getIntermediateTupleInfo());
            }
        }
        return tupleInfos.build();
    }

    @VisibleForTesting
    public interface Aggregator
    {
        void addValue(Page page);

        void addValue(BlockCursor... cursors);

        Block getResult();
    }

    private static class FixedWidthAggregator
            implements Aggregator
    {
        private final FixedWidthAggregationFunction function;
        private final Input input;
        private final Step step;
        private final Slice intermediateValue;

        private FixedWidthAggregator(FixedWidthAggregationFunction function, Input input, Step step)
        {
            Preconditions.checkNotNull(function, "function is null");
            Preconditions.checkNotNull(step, "step is null");
            this.function = function;
            this.input = input;
            this.step = step;
            this.intermediateValue = Slices.allocate(function.getFixedSize());
            function.initialize(intermediateValue, 0);
        }

        @Override
        public void addValue(BlockCursor... cursors)
        {
            BlockCursor cursor = cursors[input.getChannel()];

            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                function.addIntermediate(cursor, input.getField(), intermediateValue, 0);
            }
            else {
                function.addInput(cursor, input.getField(), intermediateValue, 0);
            }
        }

        @Override
        public void addValue(Page page)
        {
            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                BlockCursor cursor = page.getBlock(input.getChannel()).cursor();
                while (cursor.advanceNextPosition()) {
                    function.addIntermediate(cursor, input.getField(), intermediateValue, 0);
                }
            }
            else {
                Block block;
                int field = -1;
                if (input != null) {
                    block = page.getBlock(input.getChannel());
                    field = input.getField();
                }
                else {
                    block = null;
                }
                function.addInput(page.getPositionCount(), block, field, intermediateValue, 0);
            }
        }

        @Override
        public Block getResult()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                BlockBuilder output = new BlockBuilder(function.getIntermediateTupleInfo());
                function.evaluateIntermediate(intermediateValue, 0, output);
                return output.build();
            }
            else {
                BlockBuilder output = new BlockBuilder(function.getFinalTupleInfo());
                function.evaluateFinal(intermediateValue, 0, output);
                return output.build();
            }
        }
    }

    private static class VariableWidthAggregator<T>
            implements Aggregator
    {
        private final VariableWidthAggregationFunction<T> function;
        private final List<Input> inputs;
        private final Step step;
        private T intermediateValue;

        private final Block[] blocks;
        private final BlockCursor[] blockCursors;
        private final int[] fields;

        private VariableWidthAggregator(VariableWidthAggregationFunction<T> function, List<Input> inputs, Step step)
        {
            Preconditions.checkNotNull(function, "function is null");
            Preconditions.checkNotNull(step, "step is null");

            this.function = function;
            this.inputs = inputs;
            this.step = step;
            this.intermediateValue = function.initialize();

            this.blocks = new Block[inputs.size()];
            this.blockCursors = new BlockCursor[inputs.size()];
            this.fields = new int[inputs.size()];

            for (int i = 0; i < fields.length; i++) {
                fields[i] = inputs.get(i).getField();
            }
        }

        @Override
        public void addValue(Page page)
        {
            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                for (int i = 0; i < blockCursors.length; i++) {
                    blockCursors[i] = page.getBlock(inputs.get(i).getChannel()).cursor();
                }

                while (advanceAll(blockCursors)) {
                    intermediateValue = function.addIntermediate(blockCursors, fields, intermediateValue);
                }
            }
            else {
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = page.getBlock(inputs.get(i).getChannel());
                }
                intermediateValue = function.addInput(page.getPositionCount(), blocks, fields, intermediateValue);
            }
        }

        @Override
        public void addValue(BlockCursor... cursors)
        {
            for (int i = 0; i < blockCursors.length; i++) {
                blockCursors[i] = cursors[inputs.get(i).getChannel()];
            }

            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                intermediateValue = function.addIntermediate(blockCursors, fields, intermediateValue);
            }
            else {
                intermediateValue = function.addInput(blockCursors, fields, intermediateValue);
            }
        }

        @Override
        public Block getResult()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                BlockBuilder output = new BlockBuilder(function.getIntermediateTupleInfo());
                function.evaluateIntermediate(intermediateValue, output);
                return output.build();
            }
            else {
                BlockBuilder output = new BlockBuilder(function.getFinalTupleInfo());
                function.evaluateFinal(intermediateValue, output);
                return output.build();
            }
        }

        private static boolean advanceAll(BlockCursor... cursors)
        {
            boolean allAdvanced = true;
            for (BlockCursor cursor : cursors) {
                allAdvanced = cursor.advanceNextPosition() && allAdvanced;
            }
            return allAdvanced;
        }
    }
}
