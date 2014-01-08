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
import com.facebook.presto.util.array.DoubleBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

/**
 * Generate the variance for a given set of values. This implements the
 * <a href="http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm">online algorithm</a>.
 */
public class VarianceAggregation
        extends SimpleAggregationFunction
{
    protected final boolean population;
    protected final boolean inputIsLong;
    protected final boolean standardDeviation;

    public VarianceAggregation(Type parameterType,
            boolean population,
            boolean standardDeviation)
    {
        // Intermediate type should be a fixed width structure
        super(SINGLE_DOUBLE, SINGLE_VARBINARY, parameterType);
        this.population = population;
        if (parameterType == Type.FIXED_INT_64) {
            this.inputIsLong = true;
        }
        else if (parameterType == Type.DOUBLE) {
            this.inputIsLong = false;
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be FIXED_INT_64 or DOUBLE, but was " + parameterType);
        }
        this.standardDeviation = standardDeviation;
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, int valueChannel)
    {
        return new VarianceGroupedAccumulator(valueChannel, inputIsLong, population, standardDeviation, maskChannel);
    }

    public static class VarianceGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final boolean inputIsLong;
        private final boolean population;
        private final boolean standardDeviation;

        private final LongBigArray counts;
        private final DoubleBigArray means;
        private final DoubleBigArray m2s;

        private VarianceGroupedAccumulator(int valueChannel, boolean inputIsLong, boolean population, boolean standardDeviation, Optional<Integer> maskChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_VARBINARY, maskChannel);

            this.inputIsLong = inputIsLong;
            this.population = population;
            this.standardDeviation = standardDeviation;

            this.counts = new LongBigArray();
            this.means = new DoubleBigArray();
            this.m2s = new DoubleBigArray();
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf() + means.sizeOf() + m2s.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            means.ensureCapacity(groupIdsBlock.getGroupCount());
            m2s.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());

                if (!values.isNull() && (masks == null || masks.getBoolean())) {

                    long groupId = groupIdsBlock.getGroupId(position);
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong();
                    }
                    else {
                        inputValue = values.getDouble();
                    }

                    long currentCount = counts.get(groupId);
                    double currentMean = means.get(groupId);

                    // Use numerically stable variant
                    currentCount++;
                    double delta = inputValue - currentMean;
                    currentMean += (delta / currentCount);
                    // update m2 inline
                    m2s.add(groupId, (delta * (inputValue - currentMean)));

                    // write values back out
                    counts.set(groupId, currentCount);
                    means.set(groupId, currentMean);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(GroupByIdBlock groupIdsBlock, Block valuesBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            means.ensureCapacity(groupIdsBlock.getGroupCount());
            m2s.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull()) {
                    long groupId = groupIdsBlock.getGroupId(position);

                    Slice slice = values.getSlice();
                    long inputCount = getCount(slice);
                    double inputMean = getMean(slice);
                    double inputM2 = getM2(slice);

                    long currentCount = counts.get(groupId);
                    double currentMean = means.get(groupId);
                    double currentM2 = m2s.get(groupId);

                    // Use numerically stable variant
                    long newCount = currentCount + inputCount;
                    double newMean = ((currentCount * currentMean) + (inputCount * inputMean)) / newCount;
                    double delta = inputMean - currentMean;
                    double newM2 = currentM2 + inputM2 + ((delta * delta) * (currentCount * inputCount)) / newCount;

                    counts.set(groupId, newCount);
                    means.set(groupId, newMean);
                    m2s.set(groupId, newM2);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            long count = counts.get((long) groupId);
            double mean = means.get((long) groupId);
            double m2 = m2s.get((long) groupId);

            output.append(createIntermediate(count, mean, m2));
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long count = counts.get((long) groupId);
            if (population) {
                if (count == 0) {
                    output.appendNull();
                }
                else {
                    double m2 = m2s.get((long) groupId);
                    double result = m2 / count;
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
            else {
                if (count < 2) {
                    output.appendNull();
                }
                else {
                    double m2 = m2s.get((long) groupId);
                    double result = m2 / (count - 1);
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, int valueChannel)
    {
        return new VarianceAccumulator(valueChannel, inputIsLong, population, standardDeviation, maskChannel);
    }

    public static class VarianceAccumulator
            extends SimpleAccumulator
    {
        private final boolean inputIsLong;
        private final boolean population;
        private final boolean standardDeviation;

        private long currentCount;
        private double currentMean;
        private double currentM2;

        private VarianceAccumulator(int valueChannel, boolean inputIsLong, boolean population, boolean standardDeviation, Optional<Integer> maskChannel)
        {
            super(valueChannel, SINGLE_DOUBLE, SINGLE_VARBINARY, maskChannel);

            this.inputIsLong = inputIsLong;
            this.population = population;
            this.standardDeviation = standardDeviation;
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

                if (!values.isNull() && (masks == null || masks.getBoolean())) {
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong();
                    }
                    else {
                        inputValue = values.getDouble();
                    }

                    // Use numerically stable variant
                    currentCount++;
                    double delta = inputValue - currentMean;
                    currentMean += (delta / currentCount);
                    // update m2 inline
                    currentM2 += (delta * (inputValue - currentMean));
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor values = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull()) {
                    Slice slice = values.getSlice();
                    long inputCount = getCount(slice);
                    double inputMean = getMean(slice);
                    double inputM2 = getM2(slice);

                    // Use numerically stable variant
                    long newCount = currentCount + inputCount;
                    double newMean = ((currentCount * currentMean) + (inputCount * inputMean)) / newCount;
                    double delta = inputMean - currentMean;
                    double newM2 = currentM2 + inputM2 + ((delta * delta) * (currentCount * inputCount)) / newCount;

                    currentCount = newCount;
                    currentMean = newMean;
                    currentM2 = newM2;
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(BlockBuilder output)
        {
            output.append(createIntermediate(currentCount, currentMean, currentM2));
        }

        @Override
        public void evaluateFinal(BlockBuilder output)
        {
            if (population) {
                if (currentCount == 0) {
                    output.appendNull();
                }
                else {
                    double result = currentM2 / currentCount;
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
            else {
                if (currentCount < 2) {
                    output.appendNull();
                }
                else {
                    double result = currentM2 / (currentCount - 1);
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
        }
    }

    public static long getCount(Slice slice)
    {
        return slice.getLong(0);
    }

    public static double getMean(Slice slice)
    {
        return slice.getDouble(SIZE_OF_LONG);
    }

    public static double getM2(Slice slice)
    {
        return slice.getDouble(SIZE_OF_LONG + SIZE_OF_DOUBLE);
    }

    public static Slice createIntermediate(long count, double mean, double m2)
    {
        Slice slice = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE + SIZE_OF_DOUBLE);
        slice.setLong(0, count);
        slice.setDouble(SIZE_OF_LONG, mean);
        slice.setDouble(SIZE_OF_LONG + SIZE_OF_DOUBLE, m2);
        return slice;
    }
}
