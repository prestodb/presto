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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class CountColumnAggregation
        extends SimpleAggregationFunction
{
    public CountColumnAggregation(Type parameterType)
    {
        super(BIGINT, BIGINT, parameterType);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "count does not support approximate queries");
        return new CountColumnGroupedAccumulator(valueChannel, maskChannel, sampleWeightChannel);
    }

    public static class CountColumnGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final LongBigArray counts;

        public CountColumnGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, BIGINT, BIGINT, maskChannel, sampleWeightChannel);
            this.counts = new LongBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            BlockCursor sampleWeights = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                checkState(sampleWeights == null || sampleWeights.advanceNextPosition());
                long sampleWeight = computeSampleWeight(masks, sampleWeights);
                if (!values.isNull() && sampleWeight > 0) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    counts.add(groupId, sampleWeight);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull()) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    counts.add(groupId, values.getLong());
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long value = counts.get((long) groupId);
            output.append(value);
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "count does not support approximate queries");
        return new CountColumnAccumulator(valueChannel, maskChannel, sampleWeightChannel);
    }

    public static class CountColumnAccumulator
            extends SimpleAccumulator
    {
        private long count;

        public CountColumnAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, BIGINT, BIGINT, maskChannel, sampleWeightChannel);
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor masks = null;
            BlockCursor sampleWeights = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get().cursor();
            }

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                checkState(sampleWeights == null || sampleWeights.advanceNextPosition());
                long sampleWeight = computeSampleWeight(masks, sampleWeights);
                if (!values.isNull() && sampleWeight > 0) {
                    count += sampleWeight;
                }
            }
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                count += intermediates.getLong();
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            out.append(count);
        }
    }
}
