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
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.google.common.base.Preconditions.checkState;

public class CountIfAggregation
        extends SimpleAggregationFunction
{
    public static final CountIfAggregation COUNT_IF = new CountIfAggregation();

    public CountIfAggregation()
    {
        super(SINGLE_LONG, SINGLE_LONG, BOOLEAN);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, int valueChannel)
    {
        return new CountIfGroupedAccumulator(valueChannel, maskChannel);
    }

    public static class CountIfGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final LongBigArray counts;

        public CountIfGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_LONG, maskChannel);
            this.counts = new LongBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());

                if (!values.isNull() && values.getBoolean() && (masks == null || masks.getBoolean())) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    counts.increment(groupId);
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
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, int valueChannel)
    {
        return new CountIfAccumulator(valueChannel, maskChannel);
    }

    public static class CountIfAccumulator
            extends SimpleAccumulator
    {
        private long count;

        public CountIfAccumulator(int valueChannel, Optional<Integer> maskChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_LONG, maskChannel);
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                if (!values.isNull() && values.getBoolean() && (masks == null || masks.getBoolean())) {
                    count++;
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
        public void evaluateIntermediate(BlockBuilder out)
        {
            evaluateFinal(out);
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            out.append(count);
        }
    }
}
