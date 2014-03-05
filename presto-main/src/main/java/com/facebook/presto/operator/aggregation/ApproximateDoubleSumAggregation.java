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
import com.google.common.base.Optional;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.ApproximateUtils.formatApproximateResult;
import static com.facebook.presto.operator.aggregation.ApproximateUtils.sumError;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class ApproximateDoubleSumAggregation
        extends SimpleAggregationFunction
{
    public static final ApproximateDoubleSumAggregation DOUBLE_APPROXIMATE_SUM_AGGREGATION = new ApproximateDoubleSumAggregation();

    private static final int COUNT_OFFSET = 0;
    private static final int SAMPLES_OFFSET = SIZE_OF_LONG;
    private static final int SUM_OFFSET = 2 * SIZE_OF_LONG;
    private static final int VARIANCE_OFFSET = 2 * SIZE_OF_LONG + SIZE_OF_DOUBLE;

    public ApproximateDoubleSumAggregation()
    {
        // TODO: Change intermediate to fixed width, once we have a better type system
        super(SINGLE_VARBINARY, SINGLE_VARBINARY, DOUBLE);
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
        private final DoubleBigArray variances;
        private final double confidence;

        public ApproximateSumGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY, maskChannel, sampleWeightChannel);
            this.counts = new LongBigArray();
            this.samples = new LongBigArray();
            this.sums = new DoubleBigArray();
            this.variances = new DoubleBigArray();
            this.confidence = confidence;
        }

        @Override
        public long getEstimatedSize()
        {
            return counts.sizeOf() + samples.sizeOf() + sums.sizeOf() + variances.sizeOf();
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            samples.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());
            variances.ensureCapacity(groupIdsBlock.getGroupCount());
            BlockCursor values = valuesBlock.cursor();
            BlockCursor sampleWeights = sampleWeightBlock.get().cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                long groupId = groupIdsBlock.getGroupId(position);
                checkState(masks == null || masks.advanceNextPosition(), "failed to advance mask cursor");
                checkState(sampleWeights.advanceNextPosition(), "failed to advance weight cursor");
                checkState(values.advanceNextPosition(), "failed to advance values cursor");
                long weight = values.isNull() ? 0 : SimpleAggregationFunction.computeSampleWeight(masks, sampleWeights);
                if (weight > 0) {
                    samples.increment(groupId);
                }

                if (!values.isNull() && weight > 0) {
                    double value = values.getDouble();
                    long count = counts.get(groupId);
                    double sum = sums.get(groupId);
                    double variance = variances.get(groupId);
                    for (int j = 0; j < weight; j++) {
                        count++;
                        sum += value;
                        if (count > 1) {
                            double t = count * value - sum;
                            variance += (t * t) / ((double) count * (count - 1));
                        }
                    }
                    counts.set(groupId, count);
                    sums.set(groupId, sum);
                    variances.set(groupId, variance);
                }
            }
        }

        @Override
        public void processIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            counts.ensureCapacity(groupIdsBlock.getGroupCount());
            samples.ensureCapacity(groupIdsBlock.getGroupCount());
            sums.ensureCapacity(groupIdsBlock.getGroupCount());
            variances.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition(), "failed to advance intermediates cursor");

                long groupId = groupIdsBlock.getGroupId(position);
                Slice slice = intermediates.getSlice();

                long inputCount = slice.getLong(COUNT_OFFSET);
                long count = counts.get(groupId);
                samples.add(groupId, slice.getLong(SAMPLES_OFFSET));
                double inputSum = slice.getDouble(SUM_OFFSET);
                double inputVariance = slice.getDouble(VARIANCE_OFFSET);

                if (count > 0 && inputCount > 0) {
                    double t = (inputCount / (double) count) * sums.get(groupId) - inputSum;
                    variances.set(groupId, inputVariance + t * t * count / (double) (inputCount * (count + inputCount)));
                }

                sums.add(groupId, inputSum);
                counts.add(groupId, inputCount);
            }
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            output.append(createIntermediate(counts.get(groupId), samples.get(groupId), sums.get(groupId), variances.get(groupId)));
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
                    sumError(samples.get(groupId), count, sums.get(groupId), variances.get(groupId)),
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
        private double variance;
        private long count;
        private long samples;
        private final double confidence;

        public ApproximateSumAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
        {
            super(valueChannel, SINGLE_VARBINARY, SINGLE_VARBINARY, maskChannel, sampleWeightChannel);
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
                if (weight > 0) {
                    samples++;
                }
                if (!values.isNull() && weight > 0) {
                    double value = values.getDouble();

                    for (int j = 0; j < weight; j++) {
                        count++;
                        sum += value;
                        if (count > 1) {
                            double t = count * value - sum;
                            variance += (t * t) / ((double) count * (count - 1));
                        }
                    }
                }
            }
        }

        @Override
        public void processIntermediate(Block block)
        {
            BlockCursor intermediates = block.cursor();

            for (int position = 0; position < block.getPositionCount(); position++) {
                checkState(intermediates.advanceNextPosition(), "failed to advance intermediates cursor");
                Slice slice = intermediates.getSlice();
                long inputCount = slice.getLong(COUNT_OFFSET);
                samples += slice.getLong(SAMPLES_OFFSET);
                double inputSum = slice.getDouble(SUM_OFFSET);
                double inputVariance = slice.getDouble(VARIANCE_OFFSET);

                if (count > 0 && inputCount > 0) {
                    double t = (inputCount / (double) count) * sum - inputSum;
                    variance = inputVariance + t * t * count / (double) (inputCount * (count + inputCount));
                }

                sum += inputSum;
                count += inputCount;
            }
        }

        @Override
        public void evaluateIntermediate(BlockBuilder out)
        {
            out.append(createIntermediate(count, samples, sum, variance));
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
                    sumError(samples, count, sum, variance),
                    confidence,
                    false));
        }
    }

    public static Slice createIntermediate(long count, long samples, double sum, double variance)
    {
        Slice slice = Slices.allocate(2 * SIZE_OF_LONG + 2 * SIZE_OF_DOUBLE);
        slice.setLong(COUNT_OFFSET, count);
        slice.setLong(SAMPLES_OFFSET, samples);
        slice.setDouble(SUM_OFFSET, sum);
        slice.setDouble(VARIANCE_OFFSET, variance);
        return slice;
    }
}
