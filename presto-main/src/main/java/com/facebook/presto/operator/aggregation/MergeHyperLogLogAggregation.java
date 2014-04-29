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
import com.facebook.presto.spi.type.HyperLogLogType;
import com.facebook.presto.util.array.ObjectBigArray;
import com.google.common.base.Optional;
import io.airlift.stats.cardinality.HyperLogLog;

import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class MergeHyperLogLogAggregation
        extends SimpleAggregationFunction
{
    public MergeHyperLogLogAggregation()
    {
        super(HYPER_LOG_LOG, HYPER_LOG_LOG, HYPER_LOG_LOG);
    }

    @Override
    protected GroupedAccumulator createGroupedAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "confidence level must be 1.0");
        return new MergeHyperLogLogGroupedAccumulator(valueChannel, maskChannel, sampleWeightChannel);
    }

    @Override
    protected Accumulator createAccumulator(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int valueChannel)
    {
        checkArgument(confidence == 1.0, "confidence level must be 1.0");
        return new MergeHyperLogLogAccumulator(valueChannel, maskChannel);
    }

    static class MergeHyperLogLogAccumulator
            extends SimpleAccumulator
    {
        private HyperLogLog estimator;

        public MergeHyperLogLogAccumulator(int valueChannel, Optional<Integer> maskChannel)
        {
            super(valueChannel, HyperLogLogType.HYPER_LOG_LOG, HyperLogLogType.HYPER_LOG_LOG, maskChannel, Optional.<Integer>absent());
        }

        @Override
        protected void processInput(Block block, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
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
                    add(values);
                }
            }
        }

        @Override
        public void evaluateFinal(BlockBuilder out)
        {
            if (estimator == null) {
                out.appendNull();
            }
            else {
                out.appendSlice(estimator.serialize());
            }
        }

        private void add(BlockCursor cursor)
        {
            HyperLogLog instance = HyperLogLog.newInstance(cursor.getSlice());

            if (estimator == null) {
                estimator = instance;
            }
            else {
                estimator.mergeWith(instance);
            }
        }
    }

    static class MergeHyperLogLogGroupedAccumulator
            extends SimpleGroupedAccumulator
    {
        private final ObjectBigArray<HyperLogLog> estimators = new ObjectBigArray<>();
        private long sizeOfValues;

        public MergeHyperLogLogGroupedAccumulator(int valueChannel, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel)
        {
            super(valueChannel, HyperLogLogType.HYPER_LOG_LOG, HyperLogLogType.HYPER_LOG_LOG, maskChannel, sampleWeightChannel);
        }

        @Override
        public long getEstimatedSize()
        {
            return sizeOfValues;
        }

        @Override
        protected void processInput(GroupByIdBlock groupIdsBlock, Block valuesBlock, Optional<Block> maskBlock, Optional<Block> sampleWeightBlock)
        {
            estimators.ensureCapacity(groupIdsBlock.getGroupCount());

            BlockCursor values = valuesBlock.cursor();
            BlockCursor masks = null;
            if (maskBlock.isPresent()) {
                masks = maskBlock.get().cursor();
            }

            for (int position = 0; position < groupIdsBlock.getPositionCount(); position++) {
                checkState(values.advanceNextPosition());
                checkState(masks == null || masks.advanceNextPosition());

                // skip null values
                if (!values.isNull() && (masks == null || masks.getBoolean())) {
                    add(groupIdsBlock.getGroupId(position), values);
                }
            }
            checkState(!values.advanceNextPosition(), "group id and value blocks have different number of entries");
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            HyperLogLog estimator = estimators.get(groupId);
            if (estimator == null) {
                output.appendNull();
            }
            else {
                output.appendSlice(estimator.serialize());
            }
        }

        private void add(long groupId, BlockCursor cursor)
        {
            HyperLogLog instance = HyperLogLog.newInstance(cursor.getSlice());

            HyperLogLog previous = estimators.get(groupId);
            if (previous == null) {
                estimators.set(groupId, instance);
                sizeOfValues += instance.estimatedInMemorySize();
            }
            else {
                sizeOfValues -= previous.estimatedInMemorySize();
                previous.mergeWith(instance);
                sizeOfValues += previous.estimatedInMemorySize();
            }
        }
    }
}
