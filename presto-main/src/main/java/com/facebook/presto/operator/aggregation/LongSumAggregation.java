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
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.util.array.BooleanBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;

import static com.facebook.presto.type.Types.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class LongSumAggregation
        extends SimpleAggregationFunction
{
    public static final LongSumAggregation LONG_SUM = new LongSumAggregation();

    public LongSumAggregation()
    {
        super(BIGINT, BIGINT, BIGINT);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "sum does not support approximate queries");
        return new LongSumGroupedAccumulator(valueChannel, maskChannel, sampleWeightChannel);
    }

    public static class LongSumGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final BooleanBigArray notNull;
        private final LongBigArray sums;

        public LongSumGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, BIGINT, BIGINT, maskChannel, sampleWeightChannel);

            this.notNull = new BooleanBigArray();

            this.sums = new LongBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return notNull.sizeOf() + sums.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            notNull.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());

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

                long groupId = groupIdsBlock.getGroupId(position);

                long sampleWeight = computeSampleWeight(masks, sampleWeights);
                if (!values.isNull() && sampleWeight > 0) {
                    notNull.set(groupId, true);

                    long value = values.getLong();
                    sums.add(groupId, sampleWeight * value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            if (notNull.get((long) groupId)) {
                long value = sums.get((long) groupId);
                output.append(value);
            }
            else {
                output.appendNull();
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "sum does not support approximate queries");
        return new LongSumAccumulator(valueChannel, maskChannel, sampleWeightChannel);
    }

    public static class LongSumAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private long sum;

        public LongSumAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, BIGINT, BIGINT, maskChannel, sampleWeightChannel);
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
                    notNull = true;
                    sum += sampleWeight * values.getLong();
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (notNull) {
                out.append(sum);
            }
            else {
                out.appendNull();
            }
        }
    }
}
