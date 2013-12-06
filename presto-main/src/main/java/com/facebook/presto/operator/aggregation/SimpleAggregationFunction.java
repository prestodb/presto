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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class SimpleAggregationFunction
        implements AggregationFunction
{
    private final TupleInfo finalTupleInfo;
    private final TupleInfo intermediateTupleInfo;
    private final ImmutableList<Type> parameterTypes;

    public SimpleAggregationFunction(TupleInfo finalTupleInfo, TupleInfo intermediateTupleInfo, Type parameterType)
    {
        this.finalTupleInfo = finalTupleInfo;
        this.intermediateTupleInfo = intermediateTupleInfo;
        this.parameterTypes = ImmutableList.of(parameterType);
    }

    @Override
    public final List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    @Override
    public final TupleInfo getFinalTupleInfo()
    {
        return finalTupleInfo;
    }

    @Override
    public final TupleInfo getIntermediateTupleInfo()
    {
        return intermediateTupleInfo;
    }

    @Override
    public final GroupedAccumulator createGroupedAggregation(int... argumentChannels)
    {
        checkArgument(argumentChannels.length == 1, "Expected one argument channel, but got %s", argumentChannels.length);

        return createGroupedAccumulator(argumentChannels[0]);
    }

    @Override
    public final GroupedAccumulator createGroupedIntermediateAggregation()
    {
        return createGroupedAccumulator(-1);
    }

    protected abstract GroupedAccumulator createGroupedAccumulator(int valueChannel);

    public abstract static class SimpleGroupedAccumulator
            implements GroupedAccumulator
    {
        private final int valueChannel;
        private final TupleInfo finalTupleInfo;
        private final TupleInfo intermediateTupleInfo;

        public SimpleGroupedAccumulator(int valueChannel, TupleInfo finalTupleInfo, TupleInfo intermediateTupleInfo)
        {
            this.valueChannel = valueChannel;
            this.finalTupleInfo = finalTupleInfo;
            this.intermediateTupleInfo = intermediateTupleInfo;
        }

        @Override
        public final TupleInfo getFinalTupleInfo()
        {
            return finalTupleInfo;
        }

        @Override
        public final TupleInfo getIntermediateTupleInfo()
        {
            return intermediateTupleInfo;
        }

        @Override
        public final void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            checkArgument(valueChannel != -1, "Raw input is not allowed for a final aggregation");

            processInput(groupIdsBlock, page.getBlock(valueChannel));
        }

        protected abstract void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock);

        @Override
        public final void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            checkArgument(valueChannel == -1, "Intermediate input is only allowed for a final aggregation");

            processIntermediate(groupIdsBlock, block);
        }

        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            processInput(groupIdsBlock, valuesBlock);
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            evaluateFinal(groupId, output);
        }

        @Override
        public abstract void evaluateFinal(int groupId, BlockBuilder output);
    }

    @Override
    public final Accumulator createAggregation(int... argumentChannels)
    {
        checkArgument(argumentChannels.length == 1, "Expected one argument channel, but got %s", argumentChannels.length);

        return createAccumulator(argumentChannels[0]);
    }

    @Override
    public final Accumulator createIntermediateAggregation()
    {
        return createAccumulator(-1);
    }

    protected abstract Accumulator createAccumulator(int valueChannel);

    public abstract static class SimpleAccumulator
            implements Accumulator
    {
        private final int valueChannel;
        private final TupleInfo finalTupleInfo;
        private final TupleInfo intermediateTupleInfo;

        public SimpleAccumulator(int valueChannel, TupleInfo finalTupleInfo, TupleInfo intermediateTupleInfo)
        {
            this.valueChannel = valueChannel;
            this.finalTupleInfo = finalTupleInfo;
            this.intermediateTupleInfo = intermediateTupleInfo;
        }

        @Override
        public final TupleInfo getFinalTupleInfo()
        {
            return finalTupleInfo;
        }

        @Override
        public final TupleInfo getIntermediateTupleInfo()
        {
            return intermediateTupleInfo;
        }


        public final void addInput(Page page)
        {
            checkArgument(valueChannel != -1, "Raw input is not allowed for a final aggregation");

            processInput(page.getBlock(valueChannel));
        }

        protected abstract void processInput(Block block);

        @Override
        public final void addIntermediate(Block block)
        {
            checkArgument(valueChannel == -1, "Intermediate input is only allowed for a final aggregation");

            processIntermediate(block);
        }

        protected void processIntermediate(Block block)
        {
            processInput(block);
        }

        @Override
        public final Block evaluateIntermediate()
        {
            BlockBuilder out = new BlockBuilder(intermediateTupleInfo);
            evaluateIntermediate(out);
            return out.build();
        }

        @Override
        public final Block evaluateFinal()
        {
            BlockBuilder out = new BlockBuilder(finalTupleInfo);
            evaluateFinal(out);
            return out.build();
        }

        protected void evaluateIntermediate(BlockBuilder out)
        {
            evaluateFinal(out);
        }

        protected abstract void evaluateFinal(BlockBuilder out);
    }
}
