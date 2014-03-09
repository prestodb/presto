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

import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.util.array.DoubleBigArray;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.operator.aggregation.ApproximateUtils.sumError;
import static com.facebook.presto.type.DoubleType.DOUBLE;
import static com.facebook.presto.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class ApproximateDoubleSumAggregation
        extends SimpleAggregationFunction
{
    public static final ApproximateDoubleSumAggregation DOUBLE_APPROXIMATE_SUM_AGGREGATION = new ApproximateDoubleSumAggregation();

    private static final int COUNT_OFFSET = 0;
    private static final int SUM_OFFSET = SIZE_OF_LONG;
    private static final int VARIANCE_OFFSET = SIZE_OF_LONG + SIZE_OF_DOUBLE;

    public ApproximateDoubleSumAggregation()
    {
        // TODO: Change intermediate to fixed width, once we have a better type system
        super(VARCHAR, VARCHAR, DOUBLE);
    }

    @Override
    public ApproximateSumGroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // valueChannel is -1 for intermediates
        checkArgument(sampleWeightChannel.isPresent() ^ valueChannel == -1, "sampleWeightChannel is missing");
        return new ApproximateSumGroupedAccumulator(valueChannel, maskChannel, sampleWeightChannel, confidence);
    }

    public static class ApproximateSumGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        // Weighted count of rows
        private final LongBigArray counts;
        // Unweighted count of rows
        private final LongBigArray samples;
        private final DoubleBigArray sums;
        private final DoubleBigArray m2s;
        private final DoubleBigArray means;
        private final double confidence;

        public ApproximateSumGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, VARCHAR, VARCHAR, maskChannel, sampleWeightChannel);
            this.counts = new LongBigArray();
            this.samples = new LongBigArray();
            this.sums = new DoubleBigArray();
            this.m2s = new DoubleBigArray();
            this.means = new DoubleBigArray();
            this.confidence = confidence;
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf() + samples.sizeOf() + sums.sizeOf() + m2s.sizeOf() + means.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            samples.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());
            m2s.ensureCapacity(groupIdsBlock.getGroupCount());
            means.ensureCapacity(groupIdsBlock.getGroupCount());
            BlockCursor values = valuesBlock.cursor();
            BlockCursor sampleWeights = sampleWeightBlock.get().cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();
            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                long groupId = groupIdsBlock.getGroupId(position);
                checkState(masks == null || masks.advanceNextPosition(), "failed to advance mask cursor");
                checkState(sampleWeights.advanceNextPosition(), "failed to advance weight cursor");
                checkState(values.advanceNextPosition(), "failed to advance values cursor");
                long weight = values.isNull() ? 0 : SimpleAggregationFunction.computeSampleWeight(masks, sampleWeights);

                if (!values.isNull() && weight > 0) {
                    double value = values.getDouble();
                    counts.add(groupId, weight);
                    sums.add(groupId, value * weight);

                    calculator.reinitialize(samples.get(groupId), means.get(groupId), m2s.get(groupId));
                    calculator.add(value);
                    samples.set(groupId, calculator.getCount());
                    means.set(groupId, calculator.getMean());
                    m2s.set(groupId, calculator.getM2());
                }
            }
        }

        @Override
        public void processIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            samples.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());
            m2s.ensureCapacity(groupIdsBlock.getGroupCount());
            means.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor intermediates = block.cursor();

            OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();
            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition(), "failed to advance intermediates cursor");

                long groupId = groupIdsBlock.getGroupId(position);
                Slice slice = intermediates.getSlice();
                sums.add(groupId, slice.getDouble(SUM_OFFSET));
                counts.add(groupId, slice.getLong(COUNT_OFFSET));

                calculator.deserializeFrom(slice, VARIANCE_OFFSET);
                calculator.merge(samples.get(groupId), means.get(groupId), m2s.get(groupId));
                samples.set(groupId, calculator.getCount());
                means.set(groupId, calculator.getMean());
                m2s.set(groupId, calculator.getM2());
            }
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();
            calculator.merge(samples.get(groupId), means.get(groupId), m2s.get(groupId));
            output.append(createIntermediate(counts.get(groupId), sums.get(groupId), calculator));
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            long count = counts.get(groupId);
            if (count == 0) {
                output.appendNull();
                return;
            }

            output.append(formatApproximateResult(
                    sums.get(groupId),
                    sumError(samples.get(groupId), count, m2s.get(groupId), means.get(groupId)),
                    confidence,
                    false));
        }
    }

    @Override
    public ApproximateSumAccumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        // valueChannel is -1 for intermediates
        checkArgument(sampleWeightChannel.isPresent() ^ valueChannel == -1, "sampleWeightChannel is missing");
        return new ApproximateSumAccumulator(valueChannel, maskChannel, sampleWeightChannel, confidence);
    }

    public static class ApproximateSumAccumulator
            extends SimpleAccumulator
    {
        private double sum;
        private long count;
        private final OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();
        private final double confidence;

        public ApproximateSumAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, VARCHAR, VARCHAR, maskChannel, sampleWeightChannel);
            this.confidence = confidence;
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            BlockCursor values = block.cursor();
            BlockCursor sampleWeights = sampleWeightBlock.get().cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int i = 0; i < block.getPositionCount(); i++) {
                checkState(masks == null || masks.advanceNextPosition(), "failed to advance mask cursor");
                checkState(sampleWeights.advanceNextPosition(), "failed to advance weight cursor");
                checkState(values.advanceNextPosition(), "failed to advance values cursor");
                long weight = values.isNull() ? 0 : SimpleAggregationFunction.computeSampleWeight(masks, sampleWeights);
                if (!values.isNull() && weight > 0) {
                    double value = values.getDouble();

                    count += weight;
                    sum += value * weight;
                    calculator.add(value);
                }
            }
        }

        @Override
        public void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            OnlineVarianceCalculator calculator = new OnlineVarianceCalculator();
            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition(), "failed to advance intermediates cursor");
                Slice slice = intermediates.getSlice();
                sum += slice.getDouble(SUM_OFFSET);
                count += slice.getLong(COUNT_OFFSET);
                calculator.deserializeFrom(slice, VARIANCE_OFFSET);
                this.calculator.merge(calculator);
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder out)
        {
            out.append(createIntermediate(count, sum, calculator));
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (count == 0) {
                out.appendNull();
                return;
            }

            out.append(formatApproximateResult(
                    sum,
                    sumError(calculator.getCount(), count, calculator.getM2(), calculator.getMean()),
                    confidence,
                    false));
        }
    }

    public static Slice createIntermediate(long count, double sum, OnlineVarianceCalculator calculator)
    {
        Slice slice = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE + calculator.sizeOf());
        slice.setLong(COUNT_OFFSET, count);
        slice.setDouble(SUM_OFFSET, sum);
        calculator.serializeTo(slice, VARIANCE_OFFSET);
        return slice;
    }
}
