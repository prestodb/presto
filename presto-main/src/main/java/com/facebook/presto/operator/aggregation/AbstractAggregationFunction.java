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
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import io.airlift.event.client.TypeParameterUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractAggregationFunction<T extends AccumulatorState>
        extends SimpleAggregationFunction
{
    private final AccumulatorStateFactory<T> stateFactory;
    private final AccumulatorStateSerializer<T> stateSerializer;
    private final boolean approximationSupported;

    protected AbstractAggregationFunction(Type finalType, Type intermediateType, Type parameterType, boolean approximationSupported)
    {
        super(finalType, intermediateType, parameterType);
        java.lang.reflect.Type[] types = TypeParameterUtils.getTypeParameters(AbstractAggregationFunction.class, getClass());
        checkState(types.length == 1 && types[0] instanceof Class);
        stateFactory = new StateCompiler().generateStateFactory((Class<T>) types[0]);
        stateSerializer = new StateCompiler().generateStateSerializer((Class<T>) types[0]);
        this.approximationSupported = approximationSupported;
    }

    protected abstract void processInput(T state, Block block, int index, long sampleWeight);

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
        if (!approximationSupported) {
            checkArgument(confidence == 1.0, "Approximate queries not supported");
            checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        }
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
            super(valueChannel, AbstractAggregationFunction.this.getFinalType(), AbstractAggregationFunction.this.getIntermediateType(), maskChannel, sampleWeightChannel);
            this.state = AbstractAggregationFunction.this.createGroupedState();
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
        protected void processInput(GroupByIdBlock groupIdsBlock, Block values, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            checkArgument(approximationSupported || !sampleWeightBlock.isPresent(), "Sampled data not supported");
            groupedState.ensureCapacity(groupIdsBlock.getGroupCount());

            Block masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get();
            }
            Block sampleWeights = null;
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                long sampleWeight = ApproximateUtils.computeSampleWeight(masks, sampleWeights, position);

                if (!values.isNull(position) && sampleWeight > 0) {
                    groupedState.setGroupId(groupIdsBlock.getGroupId(position));
                    AbstractAggregationFunction.this.processInput(state, values, position, sampleWeight);
                }
            }
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block intermediates)
        {
            groupedState.ensureCapacity(groupIdsBlock.getGroupCount());
            T scratchState = AbstractAggregationFunction.this.createSingleState();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                if (!intermediates.isNull(position)) {
                    groupedState.setGroupId(groupIdsBlock.getGroupId(position));
                    AbstractAggregationFunction.this.processIntermediate(state, scratchState, intermediates, position);
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
            AbstractAggregationFunction.this.evaluateFinal(state, confidence, output);
        }
    }

    @Override
    protected final Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        if (!approximationSupported) {
            checkArgument(confidence == 1.0, "Approximate queries not supported");
            checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        }
        return new GenericAccumulator(valueChannel, maskChannel, sampleWeightChannel, confidence);
    }

    public final class GenericAccumulator
            extends SimpleAccumulator
    {
        private final T state;
        private final double confidence;

        public GenericAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, AbstractAggregationFunction.this.getFinalType(), AbstractAggregationFunction.this.getIntermediateType(), maskChannel, sampleWeightChannel);
            this.state = AbstractAggregationFunction.this.createSingleState();
            this.confidence = confidence;
        }

        @Override
        protected void processInput(Block values, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            checkArgument(approximationSupported || !sampleWeightBlock.isPresent(), "Sampled data not supported");
            Block masks = null;
            if (maskBlock.isPresent()) {
               masks = maskBlock.get();
            }
            Block sampleWeights = null;
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get();
            }

            for (int position = 0; position < values.getPositionCount(); position++) {
                long sampleWeight = ApproximateUtils.computeSampleWeight(masks, sampleWeights, position);
                if (!values.isNull(position) && sampleWeight > 0) {
                    AbstractAggregationFunction.this.processInput(state, values, position, sampleWeight);
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
            T scratchState = AbstractAggregationFunction.this.createSingleState();

            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    AbstractAggregationFunction.this.processIntermediate(state, scratchState, block, position);
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
            AbstractAggregationFunction.this.evaluateFinal(state, confidence, out);
        }
    }
}
