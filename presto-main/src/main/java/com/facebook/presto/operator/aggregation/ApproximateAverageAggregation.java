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
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.exception.OutOfRangeException;

import static com.facebook.presto.operator.aggregation.VarianceAggregation.createIntermediate;
import static com.facebook.presto.operator.aggregation.VarianceAggregation.getCount;
import static com.facebook.presto.operator.aggregation.VarianceAggregation.getM2;
import static com.facebook.presto.operator.aggregation.VarianceAggregation.getMean;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.google.common.base.Preconditions.checkState;

public class ApproximateAverageAggregation
        extends SimpleAggregationFunction
{
    private static final NormalDistribution NORMAL_DISTRIBUTION = new NormalDistribution();
    private final boolean inputIsLong;

    public ApproximateAverageAggregation(Type parameterType)
    {
        // Intermediate type should be a fixed width structure
        super(SINGLE_VARBINARY, SINGLE_VARBINARY, parameterType);

        if (parameterType == FIXED_INT_64) {
            this.inputIsLong = true;
        }
        else if (parameterType == DOUBLE) {
            this.inputIsLong = false;
        }
        else {
            throw new IllegalArgumentException("Expected parameter type to be FIXED_INT_64 or DOUBLE, but was " + parameterType);
        }
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        return new ApproximateAverageGroupedAccumulator(valueChannel, inputIsLong, maskChannel, sampleWeightChannel, confidence);
    }

    public static class ApproximateAverageGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final boolean inputIsLong;
        private final double confidence;

        private final LongBigArray counts;
        private final DoubleBigArray means;
        private final DoubleBigArray m2s;

        private ApproximateAverageGroupedAccumulator(int valueChannel, boolean inputIsLong, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY, maskChannel, sampleWeightChannel);

            this.inputIsLong = inputIsLong;
            this.confidence = confidence;

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

                    long currentCount = counts.get(groupId);
                    double currentMean = means.get(groupId);

                    // Use numerically stable variant
                    for (int i = 0; i < sampleWeight; i++) {
                        currentCount++;
                        double delta = inputValue - currentMean;
                        currentMean += (delta / currentCount);
                        // update m2 inline
                        m2s.add(groupId, (delta * (inputValue - currentMean)));
                    }

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
            if (count == 0) {
                output.appendNull();
            }
            else {
                double mean = means.get((long) groupId);
                double m2 = m2s.get((long) groupId);
                double variance = m2 / count;

                String result = formatApproximateAverage(count, mean, variance, confidence);
                output.append(result);
            }
        }
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        return new ApproximateAverageAccumulator(valueChannel, inputIsLong, maskChannel, sampleWeightChannel, confidence);
    }

    public static class ApproximateAverageAccumulator
            extends SimpleAccumulator
    {
        private final boolean inputIsLong;
        private final double confidence;

        private long currentCount;
        private double currentMean;
        private double currentM2;

        private ApproximateAverageAccumulator(int valueChannel, boolean inputIsLong, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY, maskChannel, sampleWeightChannel);

            this.inputIsLong = inputIsLong;
            this.confidence = confidence;
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

                    for (int i = 0; i < sampleWeight; i++) {
                        // Use numerically stable variant
                        currentCount++;
                        double delta = inputValue - currentMean;
                        currentMean += (delta / currentCount);
                        // update m2 inline
                        currentM2 += (delta * (inputValue - currentMean));
                    }
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
            if (currentCount == 0) {
                output.appendNull();
            }
            else {
                String result = formatApproximateAverage(currentCount, currentMean, currentM2 / currentCount, confidence);
                output.append(result);
            }
        }
    }

    private static String formatApproximateAverage(long count, double mean, double variance, double confidence)
    {
        double zScore = 0;
        try {
            zScore = NORMAL_DISTRIBUTION.inverseCumulativeProbability((1 + confidence) / 2);
        }
        catch (OutOfRangeException e) {
            throw Throwables.propagate(e);
        }
        // Error bars at 99% confidence interval
        StringBuilder sb = new StringBuilder();
        sb.append(mean);
        sb.append(" +/- ");
        sb.append(zScore * Math.sqrt(variance / count));
        return sb.toString();
    }
}
