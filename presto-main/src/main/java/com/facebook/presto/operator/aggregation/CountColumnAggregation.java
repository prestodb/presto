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
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.array.LongBigArray;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkState;

public class CountColumnAggregation
        extends SimpleAggregationFunction
{
    public static final CountColumnAggregation COUNT_BOOLEAN_COLUMN = new CountColumnAggregation(BOOLEAN);
    public static final CountColumnAggregation COUNT_LONG_COLUMN = new CountColumnAggregation(FIXED_INT_64);
    public static final CountColumnAggregation COUNT_DOUBLE_COLUMN = new CountColumnAggregation(DOUBLE);
    public static final CountColumnAggregation COUNT_STRING_COLUMN = new CountColumnAggregation(VARIABLE_BINARY);

    private CountColumnAggregation(Type parameterType)
    {
        super(SINGLE_LONG, SINGLE_LONG, parameterType);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        return new CountColumnGroupedAccumulator(valueChannel);
    }

    public static class CountColumnGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final LongBigArray counts;

        public CountColumnGroupedAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_LONG);
            this.counts = new LongBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull(0)) {
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

                if (!values.isNull(0)) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    counts.add(groupId, values.getLong(0));
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
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new CountColumnAccumulator(valueChannel);
    }

    public static class CountColumnAccumulator
            extends SimpleAccumulator
    {
        private long count;

        public CountColumnAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_LONG, SINGLE_LONG);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull(0)) {
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
                count += intermediates.getLong(0);
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            out.append(count);
        }
    }
}
