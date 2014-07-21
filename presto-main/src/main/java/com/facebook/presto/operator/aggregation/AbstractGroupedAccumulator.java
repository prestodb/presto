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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractGroupedAccumulator<T extends AccumulatorState>
        implements GroupedAccumulator
{
    private final T state;
    // Reference to state cast as a GroupedAccumulatorState
    private final GroupedAccumulatorState groupedState;
    private final double confidence;
    private final List<Integer> inputChannels;
    private final Optional<Integer> maskChannel;
    private final Optional<Integer> sampleWeightChannel;
    private final Type finalType;
    private final Type intermediateType;
    protected final AccumulatorStateSerializer<T> stateSerializer;
    private final AccumulatorStateFactory<T> stateFactory;

    protected AbstractGroupedAccumulator(
            Type finalType,
            Type intermediateType,
            AccumulatorStateSerializer<T> stateSerializer,
            AccumulatorStateFactory<T> stateFactory,
            List<Integer> inputChannels,
            Optional<Integer> maskChannel,
            Optional<Integer> sampleWeightChannel,
            double confidence)
    {
        this.finalType = checkNotNull(finalType, "finalType is null");
        this.intermediateType = checkNotNull(intermediateType, "intermediateType is null");
        this.stateSerializer = checkNotNull(stateSerializer, "stateSerializer is null");
        this.stateFactory = checkNotNull(stateFactory, "stateFactory is null");
        this.inputChannels = ImmutableList.copyOf(checkNotNull(inputChannels, "inputChannels is null"));
        this.maskChannel = maskChannel;
        this.sampleWeightChannel = sampleWeightChannel;
        this.state = stateFactory.createGroupedState();
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

    protected abstract void processInput(T state, List<Block> parameterBlocks, int position, long sampleWeight);

    @Override
    public void addInput(GroupByIdBlock groupIdsBlock, Page page)
    {
        checkArgument(!inputChannels.isEmpty(), "Aggregation has no input channels");

        groupedState.ensureCapacity(groupIdsBlock.getGroupCount());

        List<Block> values = IterableTransformer.on(inputChannels).transform(page.blockGetter()).list();
        Block masks = maskChannel.transform(page.blockGetter()).orNull();
        Block sampleWeights = sampleWeightChannel.transform(page.blockGetter()).orNull();

        for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
            long sampleWeight = ApproximateUtils.computeSampleWeight(masks, sampleWeights, position);

            if (sampleWeight == 0) {
                continue;
            }
            if (anyIsNull(values, position)) {
                continue;
            }
            groupedState.setGroupId(groupIdsBlock.getGroupId(position));
            processInput(state, values, position, sampleWeight);
        }
    }

    private static boolean anyIsNull(List<Block> values, int position)
    {
        for (Block block : values) {
            if (block.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    protected abstract void processIntermediate(T state, T scratchState, Block block, int position);

    @Override
    public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
    {
        checkArgument(inputChannels.isEmpty(), "Intermediate input is only allowed for a final aggregation");

        groupedState.ensureCapacity(groupIdsBlock.getGroupCount());
        T scratchState = stateFactory.createSingleState();

        for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                groupedState.setGroupId(groupIdsBlock.getGroupId(position));
                processIntermediate(state, scratchState, block, position);
            }
        }
    }

    @Override
    public void evaluateIntermediate(int groupId, BlockBuilder output)
    {
        groupedState.setGroupId(groupId);
        stateSerializer.serialize(state, output);
    }

    protected abstract void evaluateFinal(T state, double confidence, BlockBuilder out);

    @Override
    public void evaluateFinal(int groupId, BlockBuilder output)
    {
        groupedState.setGroupId(groupId);
        evaluateFinal(state, confidence, output);
    }
}
