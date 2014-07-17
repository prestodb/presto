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

import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractAccumulator<T extends AccumulatorState>
        implements Accumulator
{
    private final T state;
    private final double confidence;
    private final int valueChannel;
    private final Optional<Integer> maskChannel;
    private final Optional<Integer> sampleWeightChannel;
    private final Type finalType;
    private final Type intermediateType;
    protected final AccumulatorStateSerializer<T> stateSerializer;
    private final AccumulatorStateFactory<T> stateFactory;

    protected AbstractAccumulator(
            Type finalType,
            Type intermediateType,
            AccumulatorStateSerializer<T> stateSerializer,
            AccumulatorStateFactory<T> stateFactory,
            int valueChannel,
            Optional<Integer> maskChannel,
            Optional<Integer> sampleWeightChannel,
            double confidence)
    {
        this.finalType = checkNotNull(finalType, "finalType is null");
        this.intermediateType = checkNotNull(intermediateType, "intermediateType is null");
        this.stateSerializer = checkNotNull(stateSerializer, "stateSerializer is null");
        this.stateFactory = checkNotNull(stateFactory, "stateFactory is null");
        this.valueChannel = valueChannel;
        this.maskChannel = maskChannel;
        this.sampleWeightChannel = sampleWeightChannel;
        this.state = stateFactory.createSingleState();
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

    protected abstract void processInput(T state, List<Block> blocks, int position, long sampleWeight);

    @Override
    public void addInput(Page page)
    {
        List<Block> values = ImmutableList.of(page.getBlock(valueChannel));
        Block masks = maskChannel.transform(page.blockGetter()).orNull();
        Block sampleWeights = sampleWeightChannel.transform(page.blockGetter()).orNull();

        for (int position = 0; position < page.getPositionCount(); position++) {
            long sampleWeight = ApproximateUtils.computeSampleWeight(masks, sampleWeights, position);
            if (sampleWeight == 0) {
                continue;
            }
            if (anyIsNull(values, position)) {
                continue;
            }
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
    public void addIntermediate(Block block)
    {
        T scratchState = stateFactory.createSingleState();

        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                processIntermediate(state, scratchState, block, position);
            }
        }
    }

    @Override
    public Block evaluateIntermediate()
    {
        BlockBuilder out = intermediateType.createBlockBuilder(new BlockBuilderStatus());
        stateSerializer.serialize(state, out);
        return out.build();
    }

    protected abstract void evaluateFinal(T state, double confidence, BlockBuilder out);

    @Override
    public Block evaluateFinal()
    {
        BlockBuilder out = finalType.createBlockBuilder(new BlockBuilderStatus());
        evaluateFinal(state, confidence, out);
        return out.build();
    }

    @Override
    public long getEstimatedSize()
    {
        return state.getEstimatedSize();
    }
}
