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
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.GroupedAccumulatorState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.airlift.event.client.TypeParameterUtils;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public abstract class AbstractAggregationFunction<T extends AccumulatorState>
        implements AggregationFunction
{
    private final Type finalType;
    private final Type intermediateType;
    private final ImmutableList<Type> parameterTypes;
    private final AccumulatorStateFactory<T> stateFactory;
    private final AccumulatorStateSerializer<T> stateSerializer;
    private final boolean approximationSupported;

    protected AbstractAggregationFunction(Type finalType, Type intermediateType, Type parameterType, boolean approximationSupported)
    {
        this.finalType = checkNotNull(finalType, "final type is null");
        this.intermediateType = checkNotNull(intermediateType, "intermediate type is null");
        this.parameterTypes = ImmutableList.of(checkNotNull(parameterType, "parameter type is null"));
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
    public final List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    @Override
    public final Type getFinalType()
    {
        return finalType;
    }

    @Override
    public final Type getIntermediateType()
    {
        return intermediateType;
    }

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public Accumulator createAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        if (!approximationSupported) {
            checkArgument(confidence == 1.0, "Approximate queries not supported");
            checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        }
        return new GenericAccumulator(argumentChannels[0], maskChannel, sampleWeightChannel, confidence);
    }

    @Override
    public Accumulator createIntermediateAggregation(double confidence)
    {
        return new GenericAccumulator(-1, Optional.<Integer>absent(), Optional.<Integer>absent(), confidence);
    }

    @Override
    public GroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        if (!approximationSupported) {
            checkArgument(confidence == 1.0, "Approximate queries not supported");
            checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        }
        return new GenericGroupedAccumulator(argumentChannels[0], maskChannel, sampleWeightChannel, confidence);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
    {
        return new GenericGroupedAccumulator(-1, Optional.<Integer>absent(), Optional.<Integer>absent(), confidence);
    }

    public final class GenericGroupedAccumulator
            implements GroupedAccumulator
    {
        private final T state;
        // Reference to state cast as a GroupedAccumulatorState
        private final GroupedAccumulatorState groupedState;
        private final double confidence;
        private final int valueChannel;
        private final Optional<Integer> maskChannel;
        private final Optional<Integer> sampleWeightChannel;

        public GenericGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            checkArgument(approximationSupported || !sampleWeightChannel.isPresent(), "Sampled data not supported");
            this.valueChannel = valueChannel;
            this.maskChannel = maskChannel;
            this.sampleWeightChannel = sampleWeightChannel;
            this.state = AbstractAggregationFunction.this.createGroupedState();
            checkArgument(state instanceof GroupedAccumulatorState, "state is not a GroupedAccumulatorState");
            groupedState = (GroupedAccumulatorState) state;
            this.confidence = confidence;
        }

        @Override
        public Type getIntermediateType()
        {
            return intermediateType;
        }

        @Override
        public Type getFinalType()
        {
            return finalType;
        }

        @Override
        public long getEstimatedSize()
        {
            return state.getEstimatedSize();
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            checkArgument(valueChannel != -1, "Raw input is not allowed for a final aggregation");

            groupedState.ensureCapacity(groupIdsBlock.getGroupCount());

            Block values = page.getBlock(valueChannel);
            Block masks = maskChannel.transform(page.blockGetter()).orNull();
            Block sampleWeights = sampleWeightChannel.transform(page.blockGetter()).orNull();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                long sampleWeight = ApproximateUtils.computeSampleWeight(masks, sampleWeights, position);

                if (!values.isNull(position) && sampleWeight > 0) {
                    groupedState.setGroupId(groupIdsBlock.getGroupId(position));
                    AbstractAggregationFunction.this.processInput(state, values, position, sampleWeight);
                }
            }
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            checkArgument(valueChannel == -1, "Intermediate input is only allowed for a final aggregation");

            groupedState.ensureCapacity(groupIdsBlock.getGroupCount());
            T scratchState = AbstractAggregationFunction.this.createSingleState();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    groupedState.setGroupId(groupIdsBlock.getGroupId(position));
                    AbstractAggregationFunction.this.processIntermediate(state, scratchState, block, position);
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

    public final class GenericAccumulator
            implements Accumulator
    {
        private final T state;
        private final double confidence;
        private final int valueChannel;
        private final Optional<Integer> maskChannel;
        private final Optional<Integer> sampleWeightChannel;

        public GenericAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            checkArgument(approximationSupported || !sampleWeightChannel.isPresent(), "Sampled data not supported");
            this.valueChannel = valueChannel;
            this.maskChannel = maskChannel;
            this.sampleWeightChannel = sampleWeightChannel;
            this.state = AbstractAggregationFunction.this.createSingleState();
            this.confidence = confidence;
        }

        @Override
        public Type getFinalType()
        {
            return finalType;
        }

        @Override
        public Type getIntermediateType()
        {
            return intermediateType;
        }

        @Override
        public void addInput(Page page)
        {
            Block values = page.getBlock(valueChannel);
            Block masks = maskChannel.transform(page.blockGetter()).orNull();
            Block sampleWeights = sampleWeightChannel.transform(page.blockGetter()).orNull();

            for (int position = 0; position < values.getPositionCount(); position++) {
                long sampleWeight = ApproximateUtils.computeSampleWeight(masks, sampleWeights, position);
                if (!values.isNull(position) && sampleWeight > 0) {
                    AbstractAggregationFunction.this.processInput(state, values, position, sampleWeight);
                }
            }
        }

        @Override
        public void addIntermediate(Block block)
        {
            T scratchState = AbstractAggregationFunction.this.createSingleState();

            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    AbstractAggregationFunction.this.processIntermediate(state, scratchState, block, position);
                }
            }
        }

        @Override
        public Block evaluateIntermediate()
        {
            BlockBuilder out = intermediateType.createBlockBuilder(new BlockBuilderStatus());
            getStateSerializer().serialize(state, out);
            return out.build();
        }

        @Override
        public Block evaluateFinal()
        {
            BlockBuilder out = finalType.createBlockBuilder(new BlockBuilderStatus());
            AbstractAggregationFunction.this.evaluateFinal(state, confidence, out);
            return out.build();
        }

        @Override
        public long getEstimatedSize()
        {
            return state.getEstimatedSize();
        }
    }
}
