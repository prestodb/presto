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
import com.facebook.presto.util.array.DoubleBigArray;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.google.common.base.Preconditions.checkState;

public class DoubleMaxAggregation
        extends SimpleAggregationFunction
{
    public static final DoubleMaxAggregation DOUBLE_MAX = new DoubleMaxAggregation();

    public DoubleMaxAggregation()
    {
        super(SINGLE_DOUBLE, SINGLE_DOUBLE, DOUBLE);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        return new DoubleMaxGroupedAccumulator(valueChannel);
    }

    public static class DoubleMaxGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final BooleanBigArray notNull;
        private final DoubleBigArray maxValues;

        public DoubleMaxGroupedAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_DOUBLE);

            this.notNull = new BooleanBigArray();

            this.maxValues = new DoubleBigArray(Double.NEGATIVE_INFINITY);
        }

        @Override
        public long getEstimatedSize()
        {
            return notNull.sizeOf() + maxValues.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            notNull.ensureCapacity(groupIdsBlock.getGroupCount());
            maxValues.ensureCapacity(groupIdsBlock.getGroupCount(), Double.NEGATIVE_INFINITY);

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                long groupId = groupIdsBlock.getGroupId(position);

                if (!values.isNull(0)) {
                    notNull.set(groupId, true);

                    double value = values.getDouble(0);
                    value = Math.max(value, maxValues.get(groupId));
                    maxValues.set(groupId, value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            if (notNull.get((long) groupId)) {
                double value = maxValues.get((long) groupId);
                output.append(value);
            }
            else {
                output.appendNull();
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new DoubleMaxAccumulator(valueChannel);
    }

    public static class DoubleMaxAccumulator
            extends SimpleAccumulator
    {
        private boolean notNull;
        private double max = Double.NEGATIVE_INFINITY;

        public DoubleMaxAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_DOUBLE);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull(0)) {
                    notNull = true;
                    max = Math.max(max, values.getDouble(0));
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (notNull) {
                out.append(max);
            }
            else {
                out.appendNull();
            }
        }
    }
}
