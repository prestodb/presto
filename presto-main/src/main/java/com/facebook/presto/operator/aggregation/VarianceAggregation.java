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
import com.facebook.presto.util.array.DoubleBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.type.BigintType.BIGINT;
import static com.facebook.presto.type.DoubleType.DOUBLE;
import static com.facebook.presto.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

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
        super(DOUBLE, VARCHAR, parameterType);
        this.population = population;
        if (parameterType == BIGINT) {
            this.inputIsLong = true;
        }
        else if (parameterType == DOUBLE) {
            this.inputIsLong = false;
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be FIXED_INT_64 or DOUBLE, but was " + parameterType);
        }
        this.standardDeviation = standardDeviation;
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "variance does not support approximate queries");
        return new VarianceGroupedAccumulator(valueChannel, inputIsLong, population, standardDeviation, maskChannel, sampleWeightChannel);
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

        private VarianceGroupedAccumulator(int valueChannel, boolean inputIsLong, boolean population, boolean standardDeviation, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, DOUBLE, VARCHAR, maskChannel, sampleWeightChannel);

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
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            means.ensureCapacity(groupIdsBlock.getGroupCount());
            m2s.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }
            BlockCursor sampleWeights = null;
            if (sampleWeightBlock.isPresent()) {
                sampleWeights = sampleWeightBlock.get().cursor();
            }

            OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();
            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());
                checkState(sampleWeights == null || sampleWeights.advanceNextPosition());

                long sampleWeight = computeSampleWeight(masks, sampleWeights);
                if (!values.isNull() && sampleWeight > 0) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong();
                    }
                    else {
                        inputValue = values.getDouble();
                    }

                    calculator.reinitialize(counts.get(groupId), means.get(groupId), m2s.get(groupId));

                    for (int i = 0; i < sampleWeight; i++) {
                        calculator.add(inputValue);
                    }

                    // write values back out
                    counts.set(groupId, calculator.getCount());
                    means.set(groupId, calculator.getMean());
                    m2s.set(groupId, calculator.getM2());
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

            OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();
            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull()) {
                    long groupId = groupIdsBlock.getGroupId(position);
                    Slice slice = values.getSlice();
                    calculator.deserializeFrom(slice, 0);
                    calculator.merge(counts.get(groupId), means.get(groupId), m2s.get(groupId));

                    counts.set(groupId, calculator.getCount());
                    means.set(groupId, calculator.getMean());
                    m2s.set(groupId, calculator.getM2());
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();
            calculator.merge(counts.get(groupId), means.get(groupId), m2s.get(groupId));

            output.append(createIntermediate(calculator));
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
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "variance does not support approximate queries");
        return new VarianceAccumulator(valueChannel, inputIsLong, population, standardDeviation, maskChannel, sampleWeightChannel);
    }

    public static class VarianceAccumulator
            extends SimpleAccumulator
    {
        private final boolean inputIsLong;
        private final boolean population;
        private final boolean standardDeviation;

        private final OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();

        private VarianceAccumulator(int valueChannel, boolean inputIsLong, boolean population, boolean standardDeviation, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, DOUBLE, VARCHAR, maskChannel, sampleWeightChannel);

            this.inputIsLong = inputIsLong;
            this.population = population;
            this.standardDeviation = standardDeviation;
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
                    double inputValue;
                    if (inputIsLong) {
                        inputValue = values.getLong();
                    }
                    else {
                        inputValue = values.getDouble();
                    }

                    // TODO: remove support for sample weights, since this is an exact aggregation
                    for (int i = 0; i < sampleWeight; i++) {
                        calculator.add(inputValue);
                    }
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        protected void processIntermediate(Block block)
        {
            BlockCursor values = block.cursor();

            OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();
            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());

                if (!values.isNull()) {
                    Slice slice = values.getSlice();
                    calculator.deserializeFrom(slice, 0);
                    this.calculator.merge(calculator);
                }
            }
            checkState(!values.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(BlockBuilder output)
        {
            output.append(createIntermediate(calculator));
        }

        @Override
        public void evaluateFinal(BlockBuilder output)
        {
            if (population) {
                if (calculator.getCount() == 0) {
                    output.appendNull();
                }
                else {
                    double result = calculator.getPopulationVariance();
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
            else {
                if (calculator.getCount() < 2) {
                    output.appendNull();
                }
                else {
                    double result = calculator.getSampleVariance();
                    if (standardDeviation) {
                        result = Math.sqrt(result);
                    }
                    output.append(result);
                }
            }
        }
    }

    private static Slice createIntermediate(OnlineVarianceCalculator calculator)
    {
        Slice slice = Slices.allocate(calculator.sizeOf());
        calculator.serializeTo(slice, 0);
        return slice;
    }
}
