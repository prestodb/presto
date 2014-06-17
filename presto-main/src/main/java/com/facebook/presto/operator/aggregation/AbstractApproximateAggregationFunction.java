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
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.GroupedAccumulatorState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import io.airlift.event.client.TypeParameterUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractApproximateAggregationFunction<T extends AccumulatorState>
        extends SimpleAggregationFunction
{
    private final AccumulatorStateFactory<T> stateFactory;
    private final AccumulatorStateSerializer<T> stateSerializer;

    protected AbstractApproximateAggregationFunction(Type finalType, Type intermediateType, Type parameterType)
    {
        super(finalType, intermediateType, parameterType);
        java.lang.reflect.Type[] types = TypeParameterUtils.getTypeParameters(AbstractApproximateAggregationFunction.class, getClass());
        checkState(types.length == 1 && types[0] instanceof Class);
        stateFactory = new StateCompiler().generateStateFactory((Class<T>) types[0]);
        stateSerializer = new StateCompiler().generateStateSerializer((Class<T>) types[0]);
    }

    protected abstract void processInput(T state, BlockCursor cursor, long sampleWeight);

    protected void processIntermediate(T state, T scratchState, Block block, int index)
    {
        stateSerializer.deserialize(block, index, scratchState);
        combineState(state, scratchState);
    }

    /**
     * Combines two pieces of state. The result should be stored in state.
     */
    protected abstract void combineState(T state, T otherState);

    protected abstract void evaluateFinal(T state, double confidence, BlockBuilder out);

    private T createSingleState()
    {
        return stateFactory.createSingleState();
    }

    private T createGroupedState()
    {
        return stateFactory.createGroupedState();
    }

    protected AccumulatorStateSerializer<T> getStateSerializer()
    {
        return stateSerializer;
    }

    @Override
    protected final GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        return new GenericGroupedAccumulator(valueChannel, maskChannel, sampleWeightChannel, confidence);
    }

    public final class GenericGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final T state;
        // Reference to state cast as a GroupedAccumulatorState
        private final GroupedAccumulatorState groupedState;
        private final double confidence;

        public GenericGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, AbstractApproximateAggregationFunction.this.getFinalType(), AbstractApproximateAggregationFunction.this.getIntermediateType(), maskChannel, sampleWeightChannel);
            this.state = AbstractApproximateAggregationFunction.this.createGroupedState();
            checkArgument(state instanceof GroupedAccumulatorState, "state is not a GroupedAccumulatorState");
            groupedState = (GroupedAccumulatorState) state;
            this.confidence = confidence;
        }

        @Override
        public long getEstimatedSize()
        {
            return state.getEstimatedSize();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            groupedState.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }
            BlockCursor sampleWeights = null;
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                checkState(sampleWeights == null || sampleWeights.advanceNextPosition());
                long sampleWeight = computeSampleWeight(masks, sampleWeights);

                if (!values.isNull() && sampleWeight > 0) {
                    groupedState.setGroupId(groupIdsBlock.getGroupId(position));
                    AbstractApproximateAggregationFunction.this.processInput(state, values, sampleWeight);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block intermediates)
        {
            groupedState.ensureCapacity(groupIdsBlock.getGroupCount());
            T scratchState = AbstractApproximateAggregationFunction.this.createSingleState();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                if (!intermediates.isNull(position)) {
                    groupedState.setGroupId(groupIdsBlock.getGroupId(position));
                    AbstractApproximateAggregationFunction.this.processIntermediate(state, scratchState, intermediates, position);
                }
            }
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            groupedState.setGroupId(groupId);
            getStateSerializer().serialize(state, output);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            groupedState.setGroupId(groupId);
            AbstractApproximateAggregationFunction.this.evaluateFinal(state, confidence, output);
        }
    }

    @Override
    protected final Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        return new GenericAccumulator(valueChannel, maskChannel, sampleWeightChannel, confidence);
    }

    public final class GenericAccumulator
            extends SimpleAccumulator
    {
        private final T state;
        private final double confidence;

        public GenericAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, AbstractApproximateAggregationFunction.this.getFinalType(), AbstractApproximateAggregationFunction.this.getIntermediateType(), maskChannel, sampleWeightChannel);
            this.state = AbstractApproximateAggregationFunction.this.createSingleState();
            this.confidence = confidence;
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
               masks = maskBlock.get().cursor();
            }
            BlockCursor sampleWeights = null;
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get().cursor();
            }

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                checkState(sampleWeights == null || sampleWeights.advanceNextPosition());
                long sampleWeight = computeSampleWeight(masks, sampleWeights);
                if (!values.isNull() && sampleWeight > 0) {
                    AbstractApproximateAggregationFunction.this.processInput(state, values, sampleWeight);
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
            T scratchState = AbstractApproximateAggregationFunction.this.createSingleState();

            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    AbstractApproximateAggregationFunction.this.processIntermediate(state, scratchState, block, position);
                }
            }
        }

        @Override
        protected void evaluateIntermediate(BlockBuilder out)
        {
            getStateSerializer().serialize(state, out);
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            AbstractApproximateAggregationFunction.this.evaluateFinal(state, confidence, out);
        }
    }
}
