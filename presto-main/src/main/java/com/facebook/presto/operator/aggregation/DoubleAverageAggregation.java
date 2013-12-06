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
import com.facebook.presto.util.array.DoubleBigArray;
import com.facebook.presto.util.array.LongBigArray;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class DoubleAverageAggregation
        extends SimpleAggregationFunction
{
    public static final DoubleAverageAggregation DOUBLE_AVERAGE = new DoubleAverageAggregation();

    public DoubleAverageAggregation()
    {
        super(SINGLE_DOUBLE, SINGLE_VARBINARY, DOUBLE);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(int valueChannel)
    {
        return new DoubleSumGroupedAccumulator(valueChannel);
    }

    public static class DoubleSumGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final LongBigArray counts;
        private final DoubleBigArray sums;

        public DoubleSumGroupedAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_VARBINARY);
            this.counts = new LongBigArray();
            this.sums = new DoubleBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf() + sums.sizeOf();
        }

        @Override
        public void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                long groupId = groupIdsBlock.getGroupId(position);

                if (!values.isNull(0)) {
                    counts.increment(groupId);

                    double value = values.getDouble(0);
                    sums.add(groupId, value);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void processIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor intermediateValues = block.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediateValues.advanceNextPosition());

                long groupId = groupIdsBlock.getGroupId(position);

                Slice value = intermediateValues.getSlice(0);
                long count = value.getLong(0);
                counts.add(groupId, count);

                double sum = value.getDouble(SIZE_OF_LONG);
                sums.add(groupId, sum);
            }
            checkState(!intermediateValues.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            long count = counts.get((long) groupId);
            double sum = sums.get((long) groupId);

            // todo replace this when general fixed with values are supported
            Slice value = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE);
            value.setLong(0, count);
            value.setDouble(SIZE_OF_LONG, sum);
            output.append(value);
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long count = counts.get((long) groupId);
            if (count != 0) {
                double value = sums.get((long) groupId);
                output.append(value / count);
            }
            else {
                output.appendNull();
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(int valueChannel)
    {
        return new DoubleAverageAccumulator(valueChannel);
    }

    public static class DoubleAverageAccumulator
            extends SimpleAccumulator
    {
        private long count;
        private double sum;

        public DoubleAverageAccumulator(int valueChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_VARBINARY);
        }

        @Override
        protected void processInput(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                if (!values.isNull(0)) {
                    count++;
                    sum += values.getDouble(0);
                }
            }
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition());
                Slice value = intermediates.getSlice(0);
                count += value.getLong(0);
                sum += value.getDouble(SIZE_OF_LONG);
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder out)
        {
            // todo replace this when general fixed with values are supported
            Slice value = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE);
            value.setLong(0, count);
            value.setDouble(SIZE_OF_LONG, sum);
            out.append(value);
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (count != 0) {
                out.append(sum / count);
            }
            else {
                out.appendNull();
            }
        }
    }
}
