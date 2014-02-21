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
package com.facebook.presto.ml;

import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class EvaluateClassifierPredictionsAggregation
        implements AggregationFunction
{
    @Override
    public List<Type> getParameterTypes()
    {
        return ImmutableList.<Type>of(BIGINT, BIGINT);
    }

    @Override
    public Type getFinalType()
    {
        return VARCHAR;
    }

    @Override
    public Type getIntermediateType()
    {
        return VARCHAR;
    }

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public Accumulator createAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeight, double confidence, int... argumentChannels)
    {
        checkArgument(!maskChannel.isPresent(), "masking is not supported");
        checkArgument(confidence == 1, "approximation is not supported");
        checkArgument(!sampleWeight.isPresent(), "sample weight is not supported");
        return new EvaluatePredictionsAccumulator(argumentChannels[0], argumentChannels[1]);
    }

    @Override
    public Accumulator createIntermediateAggregation(double confidence)
    {
        checkArgument(confidence == 1, "approximation is not supported");
        return new EvaluatePredictionsAccumulator(-1, -1);
    }

    public static class EvaluatePredictionsAccumulator
            implements Accumulator
    {
        private final int labelChannel;
        private final int predictionChannel;

        private long truePositives;
        private long falsePositives;
        private long trueNegatives;
        private long falseNegatives;

        public EvaluatePredictionsAccumulator(int labelChannel, int predictionChannel)
        {
            this.labelChannel = labelChannel;
            this.predictionChannel = predictionChannel;
        }

        @Override
        public long getEstimatedSize()
        {
            return 0;
        }

        @Override
        public Type getFinalType()
        {
            return VARCHAR;
        }

        @Override
        public Type getIntermediateType()
        {
            return VARCHAR;
        }

        @Override
        public void addInput(Page page)
        {
            BlockCursor labelCursor = page.getBlock(labelChannel).cursor();
            BlockCursor predictionCursor = page.getBlock(predictionChannel).cursor();

            while (labelCursor.advanceNextPosition()) {
                checkState(predictionCursor.advanceNextPosition());
                long predicted = predictionCursor.getLong();
                long label = labelCursor.getLong();
                checkArgument(predicted == 1 || predicted == 0, "evaluate_predictions only supports binary classifiers");
                checkArgument(label == 1 || label == 0, "evaluate_predictions only supports binary classifiers");

                if (label == 1) {
                    if (predicted == 1) {
                        truePositives++;
                    }
                    else {
                        falseNegatives++;
                    }
                }
                else {
                    if (predicted == 0) {
                        trueNegatives++;
                    }
                    else {
                        falsePositives++;
                    }
                }
            }
        }

        @Override
        public void addIntermediate(Block block)
        {
            BlockCursor cursor = block.cursor();
            checkState(cursor.advanceNextPosition());
            Slice slice = cursor.getSlice();
            checkState(!cursor.advanceNextPosition());
            truePositives += slice.getLong(0);
            falsePositives += slice.getLong(SIZE_OF_LONG);
            trueNegatives += slice.getLong(2 * SIZE_OF_LONG);
            falseNegatives += slice.getLong(3 * SIZE_OF_LONG);
        }

        @Override
        public Block evaluateIntermediate()
        {
            BlockBuilder builder = getIntermediateType().createBlockBuilder(new BlockBuilderStatus());
            return builder.appendSlice(createIntermediate(truePositives, falsePositives, trueNegatives, falseNegatives)).build();
        }

        @Override
        public Block evaluateFinal()
        {
            StringBuilder sb = new StringBuilder();
            long correct = trueNegatives + truePositives;
            long total = truePositives + trueNegatives + falsePositives + falseNegatives;
            sb.append(String.format("Accuracy: %d/%d (%.2f%%)\n", correct, total, 100.0 * correct / (double) total));
            sb.append(String.format("Precision: %d/%d (%.2f%%)\n", truePositives, truePositives + falsePositives, 100.0 * truePositives / (double) (truePositives + falsePositives)));
            sb.append(String.format("Recall: %d/%d (%.2f%%)", truePositives, truePositives + falseNegatives, 100.0 * truePositives / (double) (truePositives + falseNegatives)));

            BlockBuilder builder = getFinalType().createBlockBuilder(new BlockBuilderStatus());
            builder.appendSlice(Slices.utf8Slice(sb.toString()));
            return builder.build();
        }
    }

    @Override
    public GroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeight, double confidence, int... argumentChannels)
    {
        checkArgument(!maskChannel.isPresent(), "masking is not supported");
        checkArgument(confidence == 1, "approximation is not supported");
        checkArgument(!sampleWeight.isPresent(), "sample weight is not supported");
        return new EvaluatePredictionsGroupedAccumulator(argumentChannels[0], argumentChannels[1]);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
    {
        checkArgument(confidence == 1, "approximation is not supported");
        return new EvaluatePredictionsGroupedAccumulator(-1, -1);
    }

    public static class EvaluatePredictionsGroupedAccumulator
            implements GroupedAccumulator
    {
        private final int labelChannel;
        private final int predictionChannel;

        private final LongBigArray truePositives = new LongBigArray();
        private final LongBigArray falsePositives = new LongBigArray();
        private final LongBigArray trueNegatives = new LongBigArray();
        private final LongBigArray falseNegatives = new LongBigArray();

        public EvaluatePredictionsGroupedAccumulator(int labelChannel, int predictionChannel)
        {
            this.labelChannel = labelChannel;
            this.predictionChannel = predictionChannel;
        }

        @Override
        public Type getFinalType()
        {
            return VARCHAR;
        }

        @Override
        public Type getIntermediateType()
        {
            return VARCHAR;
        }

        @Override
        public long getEstimatedSize()
        {
            return truePositives.sizeOf() + falsePositives.sizeOf() + trueNegatives.sizeOf() + falseNegatives.sizeOf();
        }

        @Override
        public void addInput(GroupByIdBlock groupIdsBlock, Page page)
        {
            truePositives.ensureCapacity(groupIdsBlock.getGroupCount());
            falsePositives.ensureCapacity(groupIdsBlock.getGroupCount());
            trueNegatives.ensureCapacity(groupIdsBlock.getGroupCount());
            falseNegatives.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor labelCursor = page.getBlock(labelChannel).cursor();
            BlockCursor predictionCursor = page.getBlock(predictionChannel).cursor();

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                long groupId = groupIdsBlock.getGroupId(position);
                checkState(labelCursor.advanceNextPosition());
                checkState(predictionCursor.advanceNextPosition());
                long predicted = predictionCursor.getLong();
                long label = labelCursor.getLong();
                checkArgument(predicted == 1 || predicted == 0, "evaluate_predictions only supports binary classifiers");
                checkArgument(label == 1 || label == 0, "evaluate_predictions only supports binary classifiers");

                if (label == 1) {
                    if (predicted == 1) {
                        truePositives.increment(groupId);
                    }
                    else {
                        falseNegatives.increment(groupId);
                    }
                }
                else {
                    if (predicted == 0) {
                        trueNegatives.increment(groupId);
                    }
                    else {
                        falsePositives.increment(groupId);
                    }
                }
            }
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            truePositives.ensureCapacity(groupIdsBlock.getGroupCount());
            falsePositives.ensureCapacity(groupIdsBlock.getGroupCount());
            trueNegatives.ensureCapacity(groupIdsBlock.getGroupCount());
            falseNegatives.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor cursor = block.cursor();
            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(cursor.advanceNextPosition());
                long groupId = groupIdsBlock.getGroupId(position);
                Slice slice = cursor.getSlice();
                truePositives.add(groupId, slice.getLong(0));
                falsePositives.add(groupId, slice.getLong(SIZE_OF_LONG));
                trueNegatives.add(groupId, slice.getLong(2 * SIZE_OF_LONG));
                falseNegatives.add(groupId, slice.getLong(3 * SIZE_OF_LONG));
            }
            checkState(!cursor.advanceNextPosition());
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            output.appendSlice(createIntermediate(truePositives.get(groupId), falsePositives.get(groupId), trueNegatives.get(groupId), falseNegatives.get(groupId))).build();
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            output.appendSlice(Slices.utf8Slice(formatResults(truePositives.get(groupId), falsePositives.get(groupId), trueNegatives.get(groupId), falseNegatives.get(groupId))));
        }
    }

    public static String formatResults(long truePositives, long falsePositives, long trueNegatives, long falseNegatives)
    {
        StringBuilder sb = new StringBuilder();
        long correct = trueNegatives + truePositives;
        long total = truePositives + trueNegatives + falsePositives + falseNegatives;
        sb.append(String.format("Accuracy: %d/%d (%.2f%%), ", correct, total, 100.0 * correct / (double) total));
        sb.append(String.format("Precision: %d/%d (%.2f%%), ", truePositives, truePositives + falsePositives, 100.0 * truePositives / (double) (truePositives + falsePositives)));
        sb.append(String.format("Recall: %d/%d (%.2f%%)", truePositives, truePositives + falseNegatives, 100.0 * truePositives / (double) (truePositives + falseNegatives)));
        return sb.toString();
    }

    public static Slice createIntermediate(long truePositives, long falsePositives, long trueNegatives, long falseNegatives)
    {
        Slice slice = Slices.allocate(4 * SIZE_OF_LONG);
        slice.setLong(0, truePositives);
        slice.setLong(SIZE_OF_LONG, falsePositives);
        slice.setLong(2 * SIZE_OF_LONG, trueNegatives);
        slice.setLong(3 * SIZE_OF_LONG, falseNegatives);
        return slice;
    }
}
