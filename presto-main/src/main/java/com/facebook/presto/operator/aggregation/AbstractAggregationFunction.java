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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.GroupedAccumulatorState;
import com.facebook.presto.operator.aggregation.state.GroupedLongAndDoubleState;
import com.facebook.presto.operator.aggregation.state.GroupedNullableDoubleState;
import com.facebook.presto.operator.aggregation.state.GroupedNullableLongState;
import com.facebook.presto.operator.aggregation.state.LongAndDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.SingleLongAndDoubleState;
import com.facebook.presto.operator.aggregation.state.SingleNullableDoubleState;
import com.facebook.presto.operator.aggregation.state.SingleNullableLongState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import io.airlift.event.client.TypeParameterUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractAggregationFunction<T extends AccumulatorState>
        extends SimpleAggregationFunction
{
    private final Class<T> stateClass;

    protected AbstractAggregationFunction(Type finalType, Type intermediateType, Type parameterType)
    {
        super(finalType, intermediateType, parameterType);
        java.lang.reflect.Type[] types = TypeParameterUtils.getTypeParameters(AbstractAggregationFunction.class, getClass());
        checkState(types.length == 1 && types[0] instanceof Class);
        this.stateClass = (Class<T>) types[0];
    }

    protected void initializeState(T state)
    {
        // noop by default
    }

    protected abstract void processInput(T state, BlockCursor cursor);

    protected void evaluateIntermediate(T state, BlockBuilder out)
    {
        // Default to using the final evaluation
        evaluateFinal(state, out);
    }

    protected void processIntermediate(T state, BlockCursor cursor)
    {
        // Default to processing the intermediate as normal input
        processInput(state, cursor);
    }

    protected abstract void evaluateFinal(T state, BlockBuilder out);

    private T createSingleState()
    {
        // TODO: we should probably generate these classes
        if (stateClass.equals(NullableDoubleState.class)) {
            T state = stateClass.cast(new SingleNullableDoubleState());
            initializeState(state);
            return state;
        }
        if (stateClass.equals(NullableLongState.class)) {
            T state = stateClass.cast(new SingleNullableLongState());
            initializeState(state);
            return state;
        }
        if (stateClass.equals(LongAndDoubleState.class)) {
            T state = stateClass.cast(new SingleLongAndDoubleState());
            initializeState(state);
            return state;
        }
        throw new IllegalStateException(String.format("Unsupported state type %s", stateClass));
    }

    private T createGroupedState()
    {
        // TODO: we should probably generate these classes
        if (stateClass.equals(NullableDoubleState.class)) {
            NullableDoubleState defaultState = new SingleNullableDoubleState();
            initializeState(stateClass.cast(defaultState));
            return stateClass.cast(new GroupedNullableDoubleState(defaultState));
        }
        if (stateClass.equals(NullableLongState.class)) {
            NullableLongState defaultState = new SingleNullableLongState();
            initializeState(stateClass.cast(defaultState));
            return stateClass.cast(new GroupedNullableLongState(defaultState));
        }
        if (stateClass.equals(LongAndDoubleState.class)) {
            LongAndDoubleState defaultState = new SingleLongAndDoubleState();
            initializeState(stateClass.cast(defaultState));
            return stateClass.cast(new GroupedLongAndDoubleState(defaultState));
        }
        throw new IllegalStateException(String.format("Unsupported state type %s", stateClass));
    }

    @Override
    protected final GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "Approximate queries not supported");
        checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        return new GenericGroupedAccumulator(valueChannel, maskChannel);
    }

    public final class GenericGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final T state;
        // Reference to state cast as a GroupedAccumulatorState
        private final GroupedAccumulatorState groupedState;

        public GenericGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel)
        {
            super(valueChannel, AbstractAggregationFunction.this.getFinalType(), AbstractAggregationFunction.this.getIntermediateType(), maskChannel, Optional.<Integer>absent());
            this.state = AbstractAggregationFunction.this.createGroupedState();
            checkArgument(state instanceof GroupedAccumulatorState, "state is not a GroupedAccumulatorState");
            groupedState = (GroupedAccumulatorState) state;
        }

        @Override
        public long getEstimatedSize()
        {
            return state.getEstimatedSize();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            checkArgument(!sampleWeightBlock.isPresent(), "Sampled data not supported");
            state.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());

                if (masks != null && !masks.getBoolean()) {
                    continue;
                }
                if (!values.isNull()) {
                    groupedState.setGroupId(groupIdsBlock.getGroupId(position));
                    AbstractAggregationFunction.this.processInput(state, values);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block intermediatesBlock)
        {
            state.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor intermediates = intermediatesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());

                if (!intermediates.isNull()) {
                    groupedState.setGroupId(groupIdsBlock.getGroupId(position));
                    AbstractAggregationFunction.this.processIntermediate(state, intermediates);
                }
            }
            checkState(!intermediates.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            groupedState.setGroupId(groupId);
            AbstractAggregationFunction.this.evaluateIntermediate(state, output);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            groupedState.setGroupId(groupId);
            AbstractAggregationFunction.this.evaluateFinal(state, output);
        }
    }

    @Override
    protected final Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "Approximate queries not supported");
        checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        return new GenericAccumulator(valueChannel, maskChannel);
    }

    public final class GenericAccumulator
            extends SimpleAccumulator
    {
        private final T state;

        public GenericAccumulator(int valueChannel, Optional<Integer> maskChannel)
        {
            super(valueChannel, AbstractAggregationFunction.this.getFinalType(), AbstractAggregationFunction.this.getIntermediateType(), maskChannel, Optional.<Integer>absent());
            this.state = AbstractAggregationFunction.this.createSingleState();
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            checkArgument(!sampleWeightBlock.isPresent(), "Sampled data not supported");
            BlockCursor values = block.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
               masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                if (masks != null && !masks.getBoolean()) {
                    continue;
                }
                if (!values.isNull()) {
                    AbstractAggregationFunction.this.processInput(state, values);
                }
            }
        }

        @Override
        public long getEstimatedSize()
        {
            return state.getEstimatedSize();
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                if (!intermediates.isNull()) {
                    AbstractAggregationFunction.this.processIntermediate(state, intermediates);
                }
            }
        }

        @Override
        protected void evaluateIntermediate(BlockBuilder out)
        {
            AbstractAggregationFunction.this.evaluateIntermediate(state, out);
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            AbstractAggregationFunction.this.evaluateFinal(state, out);
        }
    }
}
